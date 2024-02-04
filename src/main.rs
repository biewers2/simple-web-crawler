use std::collections::HashSet;
use std::io::Cursor;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Duration;

use async_pipes::{branch, BoxedAnySend, NoOutput, Pipeline, PipelineBuilder, WorkerOptions};
use html5ever::parse_document;
use html5ever::tendril::TendrilSink;
use markup5ever_arcdom::{ArcDom, Handle, NodeData};
use reqwest::{Client, Response};
use tokio::sync::Mutex;

const MAX_DEPTH: usize = 10;

enum Pipe {
    Urls,
    FoundUrls,
    Responses,
    Content,
}

impl AsRef<str> for Pipe {
    fn as_ref(&self) -> &str {
        match self {
            Self::Urls => "urls",
            Self::FoundUrls => "found-urls",
            Self::Responses => "responses",
            Self::Content => "content",
        }
    }
}

#[tokio::main(worker_threads = 100)]
async fn main() {
    let init_urls: Vec<(String, usize)> = vec![
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
    .map(|s| (s.to_string(), 0))
    .collect();

    let crawled = Arc::new(AtomicUsize::new(0));

    Pipeline::builder()
        .with_inputs(Pipe::Urls, init_urls)
        .with_flattener::<Vec<(String, usize)>>(Pipe::FoundUrls, Pipe::Urls)
        .also(register_fetcher(MAX_DEPTH))
        .also(register_crawler(crawled.clone()))
        .with_consumer(
            Pipe::Content,
            WorkerOptions::default_single_task(),
            |_content: String| async move {
                println!("Received content");
            },
        )
        .build()
        .unwrap()
        .wait()
        .await;

    println!("Crawled over {} websites", crawled.load(Relaxed));
}

fn register_fetcher(max_depth: usize) -> impl FnOnce(PipelineBuilder) -> PipelineBuilder {
    let client: Client = reqwest::ClientBuilder::new()
        .timeout(Duration::from_secs(10))
        .user_agent("Rust")
        .build()
        .unwrap();
    let visited = Arc::new(Mutex::new(HashSet::new()));

    move |builder| {
        builder.with_stage(
            Pipe::Urls,
            Pipe::Responses,
            WorkerOptions {
                pipe_buffer_size: 100000,
                max_task_count: 1000,
            },
            move |(url, depth): (String, usize)| {
                fetch(url, depth, max_depth, client.clone(), visited.clone())
            },
        )
    }
}

fn register_crawler(crawled: Arc<AtomicUsize>) -> impl FnOnce(PipelineBuilder) -> PipelineBuilder {
    move |builder| {
        builder.with_branching_stage(
            Pipe::Responses,
            vec![Pipe::FoundUrls, Pipe::Responses],
            WorkerOptions {
                pipe_buffer_size: 10000,
                max_task_count: 1000,
            },
            move |(response, depth): (Response, usize)| crawl(response, depth, crawled.clone()),
        )
    }
}

async fn fetch(
    url: String,
    depth: usize,
    max_depth: usize,
    client: Client,
    visited: Arc<Mutex<HashSet<String>>>,
) -> Option<(Response, usize)> {
    if depth >= max_depth {
        return None;
    }

    // Drop the lock's guard after using
    {
        let mut visited = visited.lock().await;
        if visited.contains(&url) {
            return None;
        }
        visited.insert(url.clone());
    }

    client.get(&url).send().await.ok().map(|resp| (resp, depth))
}

async fn crawl(
    response: Response,
    depth: usize,
    crawled: Arc<AtomicUsize>,
) -> Option<Vec<Option<BoxedAnySend>>> {
    let response = match response.error_for_status() {
        Ok(res) => {
            // println!("Received response from {}", res.url());
            res
        }
        Err(e) => {
            eprintln!("Response error: {}", e);
            return None;
        }
    };

    let mut body = if let Ok(body) = response.text().await {
        Cursor::new(body.into_bytes())
    } else {
        return None;
    };

    let dom = parse_document(ArcDom::default(), Default::default())
        .from_utf8()
        .read_from(&mut body)
        .unwrap();

    let mut urls = Vec::new();
    visit_node(&dom.document, &mut urls);
    let urls: Vec<(String, usize)> = urls.into_iter().map(|url| (url, depth + 1)).collect();

    crawled.fetch_add(1, Relaxed);
    Some(branch![urls, NoOutput])
}

fn visit_node(node: &Handle, urls: &mut Vec<String>) {
    let node_data = &node.data;
    if let NodeData::Element {
        ref name,
        ref attrs,
        ..
    } = *node_data
    {
        if name.local.eq_str_ignore_ascii_case("a") {
            if let Some(href) = attrs
                .borrow()
                .iter()
                .find(|attr| attr.name.local.eq_str_ignore_ascii_case("href"))
            {
                urls.push(href.value.to_string());
            }
        }
    }

    for child in node.children.borrow().iter() {
        visit_node(child, urls);
    }
}
