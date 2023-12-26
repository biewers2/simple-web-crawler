mod thread_testing;

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinSet;

#[derive(Debug, Default)]
pub struct Pipeline {
    workers: JoinSet<()>,
}

impl Pipeline {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_pipe<T>(&self) -> (PipeOutput<T>, PipeInput<T>) {
        let (tx, rx) = channel(100);
        let id = uuid::Uuid::new_v4().to_string();
        (PipeOutput::new(&id, tx), PipeInput::new(&id, rx))
    }

    pub async fn run(&mut self) {
        while let Some(res) = self.workers.join_next().await {
            res.unwrap();
        }
        println!("Running complete")
    }

    pub fn register_worker<I, O, F, Fut>(
        &mut self,
        input: PipeInput<I>,
        output: Option<PipeOutput<O>>,
        worker_def: F,
    ) where
        I: Send + 'static,
        O: Send + 'static,
        F: FnOnce(I) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Option<O>> + Send + 'static,
    {
        let worker = Self::new_worker(input, output, worker_def.clone());
        self.workers.spawn(worker);
    }

    async fn new_worker<I, O, F, Fut>(
        mut input: PipeInput<I>,
        output: Option<PipeOutput<O>>,
        worker_def: F,
    ) where
        I: Send + 'static,
        O: Send + 'static,
        F: FnOnce(I) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Option<O>> + Send + 'static,
    {
        let mut tasks = JoinSet::new();

        while let Some(value) = input.rx.recv().await {
            let output = output.clone();
            let work = worker_def.clone();
            tasks.spawn(async move {
                if let Some(result) = work(value).await {
                    if let Some(output) = &output {
                        output.submit(result).await;
                    }
                }
            });
        }

        while tasks.join_next().await.is_some() {}
        println!("Worker done")
    }
}

pub struct PipeInput<T> {
    pipe_id: String,
    rx: Receiver<T>,
}

impl<T> PipeInput<T> {
    fn new(pipe_id: impl Into<String>, input_rx: Receiver<T>) -> Self {
        Self {
            pipe_id: pipe_id.into(),
            rx: input_rx,
        }
    }
}

pub struct PipeOutput<T> {
    pipe_id: String,
    tx: Sender<T>,
}

impl<T> Clone for PipeOutput<T> {
    fn clone(&self) -> Self {
        Self {
            pipe_id: self.pipe_id.clone(),
            tx: self.tx.clone(),
        }
    }
}

impl<T> PipeOutput<T> {
    fn new(pipe_id: impl Into<String>, output_tx: Sender<T>) -> Self {
        Self {
            pipe_id: pipe_id.into(),
            tx: output_tx,
        }
    }

    pub async fn submit(&self, value: T) {
        self.send(value).await;
    }

    async fn send(&self, value: T) {
        self.tx
            .send(value)
            .await
            .expect("failed to send input over channel");
    }
}

#[derive(Debug, Default)]
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
