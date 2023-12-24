use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinSet;

#[derive(Debug, Default)]
pub struct Pipeline {
    synchronizer: Arc<Mutex<Synchronizer>>,
    workers: JoinSet<()>,
}

impl Pipeline {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_pipe<T>(&self) -> (PipeOutput<T>, PipeInput<T>) {
        let (tx, rx) = channel(100);
        let id = uuid::Uuid::new_v4().to_string();
        self.synchronizer.lock().unwrap().add_pipe(&id);
        (
            PipeOutput::new(&id, self.synchronizer.clone(), tx),
            PipeInput::new(&id, self.synchronizer.clone(), rx),
        )
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
        F: FnMut(I) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Option<O>> + Send + 'static,
    {
        self.workers.spawn(Self::new_worker(
            self.synchronizer.clone(),
            input,
            output,
            worker_def,
        ));
    }

    async fn new_worker<I, O, F, Fut>(
        synchronizer: Arc<Mutex<Synchronizer>>,
        mut input: PipeInput<I>,
        output: Option<PipeOutput<O>>,
        worker_def: F,
    ) where
        I: Send + 'static,
        O: Send + 'static,
        F: FnMut(I) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Option<O>> + Send + 'static,
    {
        let mut tasks = JoinSet::new();

        while let Some(value) = input.rx.recv().await {
            let consumed = input.consumed_callback();
            let output = output.clone();
            let mut work = worker_def.clone();

            tasks.spawn(async move {
                if let Some(result) = work(value).await {
                    if let Some(output) = &output {
                        output.submit(result).await;
                    }
                }
                consumed();
            });
        }

        while tasks.join_next().await.is_some() {}
        println!("Worker done")
    }
}

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
