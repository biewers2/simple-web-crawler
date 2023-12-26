use reqwest::dns::Resolve;
use reqwest::Response;
use std::collections::HashSet;
use std::future::Future;
use std::sync::{Arc, Mutex};
use swc::{PipeInput, PipeOutput, Pipeline};
use tokio::join;

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let query = "Google";
    let init_urls: Vec<String> = vec![
        "https://google.com",
        "https://example.com",
        "https://hello.com",
        "https://google.com",
        "https://maps.google.com",
        "https://google.com",
        "https://example.com",
        "https://drive.google.com",
    ]
    .into_iter()
    .map(|s| s.to_string())
    .collect();

    let mut pipeline = Pipeline::new();

    let (url_out, url_in) = pipeline.create_pipe();
    let (resp_out, resp_in) = pipeline.create_pipe();
    let (content_out, content_in) = pipeline.create_pipe();

    let producer = async move {
        for url in init_urls {
            url_out.submit(url).await;
        }
    };
    register_fetch(&mut pipeline, url_in, resp_out);
    register_crawl(&mut pipeline, resp_in, content_out);
    register_format(&mut pipeline, content_in);

    join!(producer, pipeline.run());
    Ok(())
}

fn register_fetch(pipeline: &mut Pipeline, input: PipeInput<String>, output: PipeOutput<Response>) {
    let visited = Arc::new(Mutex::new(HashSet::new()));

    pipeline.register_worker(input, Some(output), |url: String| async move {
        if visited.lock().unwrap().contains(&url) {
            return None;
        }

        visited.lock().unwrap().insert(url.clone());
        reqwest::get(&url).await.map(Some).unwrap_or(None)
    });
}

fn register_crawl(pipeline: &mut Pipeline, input: PipeInput<Response>, output: PipeOutput<String>) {
    pipeline.register_worker(input, Some(output), |response: Response| async move {
        let content = response.text().await.unwrap();
        println!("Response size: {}", content.len());
        Some(content)
    });
}

fn register_format(pipeline: &mut Pipeline, input: PipeInput<String>) {
    pipeline.register_worker(input, None, |_content: String| async move {
        println!("Received content");
        Option::<String>::None
    });
}
