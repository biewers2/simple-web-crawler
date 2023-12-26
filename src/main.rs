use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use reqwest::Response;
use tokio::join;

use swc::{PipeInput, PipeOutput, Pipeline};

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
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

    let (url_out, url_in) = pipeline.create_pipe("urls");
    let (resp_out, resp_in) = pipeline.create_pipe("responses");
    let (content_out, content_in) = pipeline.create_pipe("content");

    let init_url_out = url_out.clone();
    let producer = async move {
        for url in init_urls {
            init_url_out.submit(url).await;
        }
    };
    register_fetch(&mut pipeline, url_in, resp_out);
    register_crawl(&mut pipeline, resp_in, url_out, content_out);
    register_format(&mut pipeline, content_in);

    join!(producer, pipeline.wait());
    Ok(())
}

fn register_fetch(pipeline: &mut Pipeline, input: PipeInput<String>, output: PipeOutput<Response>) {
    let visited = Arc::new(Mutex::new(HashSet::new()));

    pipeline.register_worker("fetch", input, vec![output], |url: String| async move {
        if visited.lock().unwrap().contains(&url) {
            return vec![None];
        }

        visited.lock().unwrap().insert(url.clone());
        vec![reqwest::get(&url).await.map(Some).unwrap_or(None)]
    });
}

fn register_crawl(
    pipeline: &mut Pipeline,
    input: PipeInput<Response>,
    url_output: PipeOutput<String>,
    content_output: PipeOutput<String>,
) {
    let outputs = vec![url_output, content_output];
    pipeline.register_worker("crawl", input, outputs, |response: Response| async move {
        let content = response.text().await.unwrap();
        println!("Response size: {}", content.len());
        vec![None, Some(content)]
    });
}

fn register_format(pipeline: &mut Pipeline, input: PipeInput<String>) {
    pipeline.register_worker("format", input, vec![], |_content: String| async move {
        println!("Received content");
        vec![Option::<String>::None]
    });
}
