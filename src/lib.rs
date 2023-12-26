use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::sync::{Arc, Mutex};

use tokio::select;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinSet;

mod thread_testing;

enum WorkerSignal {
    Terminate,
}

#[derive(Debug, Default)]
pub struct Pipeline {
    synchronizer: Arc<Mutex<Synchronizer>>,
    workers: JoinSet<()>,
    signal_txs: HashMap<String, Sender<WorkerSignal>>,
}

impl Pipeline {
    /// Create a new pipeline.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a pipe to transfer data between stages of the pipeline.
    ///
    /// ### Arguments:
    ///
    /// name - name for the pipe, useful for debugging and monitoring
    ///
    /// ### Returns:
    ///
    /// Both "ends" of the pipe, one for sending data to (PipeOutput, the output for a stage)
    /// and one for receiving data (PipeInput, the input for the next stage).
    pub fn create_pipe<T>(&self, name: impl AsRef<str>) -> (PipeOutput<T>, PipeInput<T>) {
        let (tx, rx) = channel(10);
        let id = format!("{}-{}", name.as_ref(), uuid::Uuid::new_v4());
        self.synchronizer.lock().unwrap().add_pipe(&id);
        (
            PipeOutput::new(&id, self.synchronizer.clone(), tx),
            PipeInput::new(&id, self.synchronizer.clone(), rx),
        )
    }

    /// Wait for all workers in the pipeline to finish.
    ///
    /// Once all workers are done running (checked via the Synchronizer),
    /// send the `TERMINATE` signal to all the workers.
    pub async fn wait(&mut self) {
        loop {
            if self.synchronizer.lock().unwrap().is_done() {
                for tx in self.signal_txs.values() {
                    tx.send(WorkerSignal::Terminate)
                        .await
                        .expect("failed to send done signal")
                }
                break;
            }
        }

        while let Some(res) = self.workers.join_next().await {
            res.unwrap();
        }
    }

    /// Register a worker to handle data between pipes.
    ///
    /// This is considered defining a "stage".
    ///
    /// ### Arguments:
    ///
    /// name - A name given to the worker; must be unique
    /// inputs - Created by [Pipeline::create_pipe], where the worker receives input
    /// outputs - Created by [Pipeline::create_pipe], where the worker sends output
    /// worker_def - A function that receives a single input and produces output
    pub fn register_worker<I, O, F, Fut>(
        &mut self,
        name: impl Into<String>,
        inputs: PipeInput<I>,
        outputs: Vec<PipeOutput<O>>,
        worker_def: F,
    ) where
        I: Send + 'static,
        O: Send + 'static,
        F: FnOnce(I) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Vec<Option<O>>> + Send + 'static,
    {
        let name = name.into();

        let (signal_tx, signal_rx) = channel(1);
        self.signal_txs.insert(name.clone(), signal_tx);

        self.workers.spawn(Self::new_worker(
            name,
            signal_rx,
            inputs,
            outputs,
            worker_def.clone(),
        ));
    }

    async fn new_worker<I, O, F, Fut>(
        name: String,
        mut signal_rx: Receiver<WorkerSignal>,
        mut inputs: PipeInput<I>,
        outputs: Vec<PipeOutput<O>>,
        worker_def: F,
    ) where
        I: Send + 'static,
        O: Send + 'static,
        F: FnOnce(I) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Vec<Option<O>>> + Send + 'static,
    {
        let mut tasks = JoinSet::new();

        let tasks_ref = &mut tasks;
        select! {
            _ = async move {
                while let Some(input) = inputs.rx.recv().await {
                    let consumed = inputs.consumed_callback();
                    let outputs = outputs.clone();
                    let work = worker_def.clone();

                    tasks_ref.spawn(async move {
                        let results = work(input).await;
                        consumed();

                        for (i, value) in results.into_iter().enumerate() {
                            if let Some(result) = value {
                                outputs
                                    .get(i)
                                    .expect("produced output count does not equal output pipe count")
                                    .submit(result)
                                    .await;
                            }
                        }
                    });
                }
            } => {}

            Some(signal) = signal_rx.recv() => {
                match signal {
                    WorkerSignal::Terminate => (),
                }
            },
        }

        while tasks.join_next().await.is_some() {}
    }
}

/// Defines an end to a pipe that allows data to be received from.
///
/// Provide this to [Pipeline::register_worker] as the input for that stage.
#[derive(Debug)]
pub struct PipeInput<T> {
    pipe_id: String,
    synchronizer: Arc<Mutex<Synchronizer>>,
    rx: Receiver<T>,
}

impl<T> PipeInput<T> {
    fn new(
        pipe_id: impl Into<String>,
        synchronizer: Arc<Mutex<Synchronizer>>,
        input_rx: Receiver<T>,
    ) -> Self {
        Self {
            pipe_id: pipe_id.into(),
            synchronizer,
            rx: input_rx,
        }
    }

    fn consumed_callback(&self) -> impl FnOnce() {
        let id = self.pipe_id.clone();
        let sync = self.synchronizer.clone();
        move || {
            sync.lock().unwrap().decrement(&id);
        }
    }
}

/// Defines an end to a pipe that allows data to be sent through.
///
/// Provide this to [Pipeline::register_worker] as an output for a stage.
#[derive(Debug)]
pub struct PipeOutput<T> {
    pipe_id: String,
    synchronizer: Arc<Mutex<Synchronizer>>,
    tx: Sender<T>,
}

impl<T> Clone for PipeOutput<T> {
    fn clone(&self) -> Self {
        Self {
            pipe_id: self.pipe_id.clone(),
            synchronizer: self.synchronizer.clone(),
            tx: self.tx.clone(),
        }
    }
}

impl<T> PipeOutput<T> {
    fn new(
        pipe_id: impl Into<String>,
        synchronizer: Arc<Mutex<Synchronizer>>,
        output_tx: Sender<T>,
    ) -> Self {
        Self {
            pipe_id: pipe_id.into(),
            synchronizer,
            tx: output_tx,
        }
    }

    pub async fn submit(&self, value: T) {
        self.synchronizer.lock().unwrap().increment(&self.pipe_id);
        self.send(value).await;
    }

    pub async fn submit_all(&self, values: Vec<T>) {
        self.synchronizer
            .lock()
            .unwrap()
            .increment_by(&self.pipe_id, values.len());
        for value in values {
            self.send(value).await;
        }
    }

    async fn send(&self, value: T) {
        self.tx
            .send(value)
            .await
            .expect("failed to send input over channel");
    }
}

#[derive(Default, Debug)]
struct Synchronizer {
    net_activity_count: Mutex<usize>,
    pipe_activity_counts: HashMap<String, Mutex<usize>>,
}

impl Synchronizer {
    fn add_pipe(&mut self, pipe_id: impl Into<String>) {
        self.pipe_activity_counts
            .insert(pipe_id.into(), Mutex::new(0));
    }

    fn increment(&self, pipe_id: impl AsRef<str>) {
        self.increment_by(pipe_id, 1);
    }

    fn increment_by(&self, pipe_id: impl AsRef<str>, n: usize) {
        if let Some(count) = self.pipe_activity_counts.get(pipe_id.as_ref()) {
            let mut count = count
                .lock()
                .expect("failed to acquire pipe activity counts lock");
            let mut net_count = self
                .net_activity_count
                .lock()
                .expect("failed to acquire net activity count lock");

            *count += n;
            *net_count += n;
        }
    }

    fn decrement(&self, pipe_id: impl AsRef<str>) {
        self.decrement_by(pipe_id, 1);
    }

    fn decrement_by(&self, pipe_id: impl AsRef<str>, n: usize) {
        if let Some(count) = self.pipe_activity_counts.get(pipe_id.as_ref()) {
            let mut count = count
                .lock()
                .expect("failed to acquire pipe activity counts lock");
            let mut net_count = self
                .net_activity_count
                .lock()
                .expect("failed to acquire net activity count lock");

            *count -= n;
            *net_count -= n;
        }
    }

    fn is_done(&self) -> bool {
        let net_count = self
            .net_activity_count
            .lock()
            .expect("failed to acquire pipe activity counts lock");

        *net_count == 0
    }
}
