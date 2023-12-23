use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::iter::Map;
use std::ops::Deref;
use std::sync::{Arc, Mutex, MutexGuard};

use reqwest::Response;
use tokio::join;
use tokio::sync::mpsc::{channel, Receiver, Sender};

pub async fn crawl_the_web(query: impl Into<String>, urls: Vec<String>) {
    let (urls_tx, urls_rx) = channel(100);
    let (resp_tx, resp_rx) = channel(100);
    let (content_tx, content_rx) = channel(100);

    let synchronizer = Arc::new(PipelineSynchronizer::new());

    urls_tx.send(urls).await.unwrap();
    let fetching = fetch(synchronizer.clone(), urls_rx, resp_tx);
    let crawling = crawl(query.into(), synchronizer, resp_rx, content_tx, urls_tx);
    let receiving = receive(content_rx);
    join!(fetching, crawling, receiving);
}

pub struct PipeInput<T> {
    pipe_id: String,
    synchronizer: Arc<PipelineSynchronizer>,
    output_tx: Sender<T>,
}

impl<T> PipeInput<T> {
    pub fn new(synchronizer: Arc<PipelineSynchronizer>, output_tx: Sender<T>) -> Self {
        Self {
            pipe_id: "".to_string(),
            synchronizer,
            output_tx,
        }
    }

    pub async fn submit(&self, value: T) {
        self.synchronizer.increment(&self.pipe_id);
        self.output_tx
            .send(value)
            .await
            .expect("failed to send input over channel");
    }
}

#[derive(Debug)]
struct PipelineSynchronizer {
    pipe_activity_counts: HashMap<String, Mutex<usize>>,
}

impl PipelineSynchronizer {
    fn new() -> Self {
        Self {
            pipe_activity_counts: HashMap::new(),
        }
    }

    fn is_done(&self) -> bool {
        // Maintain ownership over all locks while checking value
        let locks: Vec<MutexGuard<usize>> = self
            .pipe_activity_counts
            .values()
            .map(|count| count.lock().unwrap())
            .collect();

        // All `usize` should be > 0, so the sum should be 0 if the pipeline is done
        locks.iter().fold(0usize, |acc, count| acc + count.deref()) == 0
    }

    fn increment(&self, pipe_id: impl AsRef<str>) {
        if let Some(count) = self.pipe_activity_counts.get(pipe_id.as_ref()) {
            let count = count
                .lock()
                .expect("failed to acquire pipe activity counts lock");

            *count += 1;
        }
    }

    fn decrement(&self, pipe_id: impl AsRef<str>) {
        if let Some(count) = self.pipe_activity_counts.get(pipe_id.as_ref()) {
            let count = count
                .lock()
                .expect("failed to acquire pipe activity counts lock");

            *count -= 1;
        }
    }
}

async fn worker_factory<I, O>(
    synchronizer: Arc<PipelineSynchronizer>,
    mut worker_inputs: Receiver<I>,
    next_worker_inputs: Sender<O>,
    mut worker_def: impl FnMut(I, &PipeInput<O>),
) {
    let outputs = PipeInput::new(synchronizer.clone(), next_worker_inputs);

    while let Some(value) = worker_inputs.recv().await {
        worker_def(value, &outputs);
        synchronizer.decrement(outputs);

        if synchronizer.is_done() {
            break;
        }
    }

    println!("Finished fetching");
}

async fn crawl(
    query: String,
    synchronizer: Arc<PipelineSynchronizer>,
    mut resp_rx: Receiver<Response>,
    content_tx: Sender<String>,
    urls_tx: Sender<Vec<String>>,
) {
    while let Some(resp) = resp_rx.recv().await {
        let content = resp.text().await.unwrap();
        synchronizer.decr_crawling(1);
        content_tx.send(content).await.unwrap();

        if synchronizer.is_done() {
            break;
        }
    }

    println!("Finished crawling");
}

async fn receive(mut content_rx: Receiver<String>) {
    while let Some(content) = content_rx.recv().await {
        // println!("{}", content)
    }

    println!("Finished receiving");
}
