use reqwest::Response;
use std::collections::HashSet;
use swc::Pipeline;
use tokio::join;

///
/// Web Crawler
///
/// Given a URL and a query, find all content on that page that matches the query (like grep, but over the internet)
///
/// For example,
///   $ swc "Google" https://google.com https://maps.google.com
///
/// Implementation Notes
///
/// - "urlBatches" = Channel to transfer URLs to be crawled (each item in channel is a list of URLs)
/// - "visited" = Set tracking already visited URLs
/// - "Fetcher" = Service/class/method/??? to fetch data from a URL
///
/// Pseudo:
///   query, initUrls
///   urlBatches (chan)
///   visited (set)
///   fetcher = Fetcher
///
///   contentCallback = ~method to handle content and look for matches~
///
///   urlBatches <- initUrls
///
///   for urls in urlBatches {
///     urls = dedupe urls
///     for url in urls {
///       if url not in visited {
///         visited.add(url)
///         fetcher.fetch(url, urlBatches, contentCallback)
///       }
///     }
///   }
///

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let query = "Google";
    let init_urls = vec![
        "https://google.com/".to_string(),
        "https://maps.google.com".to_string(),
    ];

    let mut visited = HashSet::new();
    let fetch = |url: String| async {
        if visited.contains(&url) {
            return None;
        }

        visited.insert(url.clone());
        Some(reqwest::get(&url).await.unwrap())
    };

    let mut pipeline = Pipeline::new();

    let (url_out, url_in) = pipeline.create_pipe();
    let (resp_out, resp_in) = pipeline.create_pipe();
    let (content_out, content_in) = pipeline.create_pipe();

    let producer = async move {
        url_out.submit_all(init_urls).await;
    };
    pipeline.register_worker(url_in, Some(resp_out), fetch);
    pipeline.register_worker(resp_in, Some(content_out), crawl);
    pipeline.register_worker(content_in, None, format);

    join!(producer, pipeline.run());
    Ok(())
}

struct Fetcher {
    visited: Vec<String>,
}

async fn crawl(response: Response) -> Option<String> {
    let content = response.text().await.unwrap();
    println!("Response size: {}", content.len());
    Some(content)
}

async fn format(_content: String) -> Option<()> {
    println!("Received content");
    None
}
