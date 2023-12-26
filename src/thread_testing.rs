use std::future::Future;
use tokio::task::JoinSet;

async fn run<T, R, F, Fut>(input: Vec<T>, callback: F) -> Vec<R>
where
    R: Send + 'static,
    F: FnOnce(T) -> Fut + Clone + 'static,
    Fut: Future<Output = R> + Send + 'static,
{
    let mut tasks = JoinSet::new();
    for value in input {
        tasks.spawn(callback.clone()(value));
    }

    let mut results = vec![];
    while let Some(f) = tasks.join_next().await {
        results.push(f.unwrap());
    }
    results
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};

    use crate::thread_testing::run;
    use tokio::task::JoinSet;

    #[tokio::test]
    async fn test_concurrent_state() {
        let urls: Vec<String> = vec![
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
        let expected: Vec<String> = vec![
            "https://google.com",
            "https://example.com",
            "https://hello.com",
            "https://maps.google.com",
            "https://drive.google.com",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect();

        let visited = Arc::new(Mutex::new(HashSet::new()));
        let fetch = |url: String| async move {
            let mut vs = visited.lock().unwrap();
            if vs.contains(&url) {
                return None;
            }

            vs.insert(url.clone());
            Some(url)
        };

        let results: Vec<String> = run(urls, fetch).await.into_iter().flatten().collect();
        assert_eq!(expected, results);
    }
}
