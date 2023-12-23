use std::collections::HashSet;
use reqwest::Response;
use tokio::sync::mpsc::Sender;
use swc::PipeInput;

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
    let query = "Google";
    let init_urls = vec![
        "https://google.com/".to_string(),
        "https://maps.google.com".to_string(),
    ];
    
    let mut visited = HashSet::new();
    
    let fetch = async move |url: String, crawl_input: &PipeInput<Response>| {
        if visited.contains(&url) {
            return
        }
        
        visited.insert(url.clone());
        let resp = reqwest::get(&url).await.unwrap();
        crawl_input.submit(resp).await;
    };

    swc::crawl_the_web(query, init_urls).await;
    Ok(())
}
