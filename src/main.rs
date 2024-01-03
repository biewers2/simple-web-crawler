use std::collections::HashSet;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use async_pipes::{BoxedAnySend, Pipeline, PipelineBuilder};
use html5ever::parse_document;
use html5ever::tendril::TendrilSink;
use markup5ever_arcdom::{ArcDom, Handle, NodeData};
use reqwest::{Client, Response};
use tokio::sync::Mutex;

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

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let query = "Google";
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

    let b = Pipeline::builder()
        .with_inputs(Pipe::Urls, init_urls)
        .with_flattener::<Vec<(String, usize)>>(Pipe::FoundUrls, Pipe::Urls);

    let b = register_fetcher(b, 4);
    let b = register_crawler(b);

    let b = b.with_consumer(Pipe::Content, |_content: String| async move {
        println!("Received content");
    });

    b.build().unwrap().wait().await;
    Ok(())
}

fn register_fetcher(builder: PipelineBuilder, max_depth: usize) -> PipelineBuilder {
    let client: Client = reqwest::ClientBuilder::new()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let visited = Arc::new(Mutex::new(HashSet::new()));

    builder.with_stage(
        Pipe::Urls,
        Pipe::Responses,
        move |(url, depth): (String, usize)| {
            let client = client.clone();
            let visited = visited.clone();

            async move {
                if depth >= max_depth {
                    return None;
                }

                {
                    let mut visited = visited.lock().await;
                    if visited.contains(&url) {
                        println!("{}:\tAlready fetched", &url);
                        return None;
                    }
                    visited.insert(url.clone());
                }

                println!("Fetching ({}): {}", depth, &url);
                client.get(&url).send().await.ok().map(|resp| (resp, depth))
            }
        },
    )
}

fn register_crawler(builder: PipelineBuilder) -> PipelineBuilder {
    builder.with_branching_stage(
        Pipe::Responses,
        vec![Pipe::FoundUrls, Pipe::Responses],
        move |(response, depth): (Response, usize)| async move {
            let response = match response.error_for_status() {
                Ok(r) => r,
                Err(e) => {
                    println!("Response error: {}", e);
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

            Some(vec![Some(Box::new(urls) as BoxedAnySend), None])
        },
    )
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
