use crate::tracker::AnnounceRequest;
use reqwest;
use std::{sync::Arc, time::Duration};
use tokio::task::JoinHandle;

pub struct HeartBeater {
    handles: Vec<JoinHandle<()>>,
}

impl HeartBeater {
    pub fn new(self_url: String, trackers: Vec<String>, interval: Duration) -> Box<Self> {
        let client = Arc::new(
            reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .connect_timeout(Duration::from_secs(5))
                .pool_max_idle_per_host(1)
                .pool_idle_timeout(Duration::from_secs(20))
                .http1_only()
                .build()
                .expect("Failed to build heartbeat HTTP client"),
        );

        Box::new(HeartBeater {
            handles: trackers
                .iter()
                .map(|url| (url.clone(), client.clone(), self_url.clone()))
                .map(move |(mut url, client, self_url)| {
                    tokio::spawn(async move {
                        url.push_str("/announce");
                        loop {
                            let req = AnnounceRequest {
                                addr: self_url.clone(),
                            };

                            // Retry up to 3 times with backoff for transient errors
                            let mut last_err = None;
                            for attempt in 0..3 {
                                match client.post(url.as_str()).json(&req).send().await {
                                    Ok(resp) => {
                                        if let Err(err) = resp.error_for_status() {
                                            eprintln!(
                                                "Heartbeat got error status: {:?}",
                                                err
                                            );
                                        }
                                        last_err = None;
                                        break;
                                    }
                                    Err(err) => {
                                        last_err = Some(err);
                                        if attempt < 2 {
                                            tokio::time::sleep(Duration::from_millis(
                                                500 * (attempt as u64 + 1),
                                            ))
                                            .await;
                                        }
                                    }
                                }
                            }

                            if let Some(err) = last_err {
                                eprintln!(
                                    "Failed to send heartbeat after 3 attempts: {:?}",
                                    err
                                );
                            }

                            tokio::time::sleep(interval).await;
                        }
                    })
                })
                .collect::<Vec<_>>(),
        })
    }

    pub fn stop(&self) {
        for handle in self.handles.iter() {
            handle.abort();
        }
    }
}
