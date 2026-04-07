use crate::server::LookupDirOrFile;
use crate::tracker::PeersResponse;
use futures::TryFutureExt;
use reqwest;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::{collections::HashSet, error::Error};

#[derive(Debug, Clone)]
pub enum Action {
    Download {
        peers: Arc<RwLock<Vec<String>>>,
        peer_id: usize,
        path: PathBuf,
        md5: String,
        size: usize,
    },
    MakeDir {
        path: PathBuf,
    },
}

pub struct Planer {
    tracker_urls: Vec<String>,
}

impl Planer {
    pub fn new(tracker_urls: Vec<String>) -> Self {
        Planer { tracker_urls }
    }

    pub async fn plan(&self, md5: &str) -> Result<Vec<Action>, Box<dyn Error + Sync + Send>> {
        let peers = {
            let mut peers_set = HashSet::new();

            let mut errs = Vec::new();
            for result in self
                .tracker_urls
                .iter()
                .map(|url| reqwest::get(format!("{}/peers", url)))
            {
                match result
                    .and_then(async |r| r.error_for_status()?.json::<PeersResponse>().await)
                    .await
                {
                    Ok(peers_response) => {
                        for addr in peers_response.peers.iter().map(|p| p.addr.clone()) {
                            peers_set.insert(addr);
                        }
                    }
                    Err(err) => {
                        errs.push(err);
                    }
                }
            }
            if peers_set.is_empty() {
                if errs.is_empty() {
                    return Err("tracker_urls is empty".into());
                } else {
                    return Err(errs
                        .iter()
                        .map(|e| format!("internal errors: {}", e))
                        .collect::<Vec<_>>()
                        .join("; ")
                        .into());
                }
            }

            peers_set.iter().map(|str| str.clone()).collect::<Vec<_>>()
        };

        // get md5 tree from peers

        let (tree, new_peers) = {
            let mut errs = Vec::new();
            let mut tree_and_peer = Vec::new();
            for (peer, result) in peers.iter().map(|peer| {
                let encoded_md5 = urlencoding::encode(md5);
                (
                    peer.as_str(),
                    reqwest::get(format!("{}/query?md5={}", peer.as_str(), encoded_md5)),
                )
            }) {
                match result
                    .and_then(async |r| r.error_for_status()?.json::<LookupDirOrFile>().await)
                    .await
                {
                    Ok(tree) => {
                        tree_and_peer.push((peer, tree));
                    }
                    Err(err) => {
                        errs.push(err);
                    }
                }
            }

            if tree_and_peer.len() == 0 {
                return if errs.len() == 0 {
                    Err("no peers found".into())
                } else {
                    Err(errs
                        .iter()
                        .map(|e| format!("{}", e))
                        .collect::<Vec<_>>()
                        .join("; ")
                        .into())
                };
            }

            let (peer, tree) = tree_and_peer.pop().unwrap();

            let mut new_peers = Vec::new();

            new_peers.push(String::from(peer));

            while let Some((peer, _)) = tree_and_peer.pop() {
                new_peers.push(String::from(peer))
            }

            (tree, new_peers)
        };

        let n_peers = new_peers.len();
        let mut next_id: usize = 0;

        let peers_ptr = Arc::new(RwLock::new(new_peers));

        {
            let mut frontier = VecDeque::new();
            frontier.push_back((PathBuf::from("."), &tree));

            let mut result = Vec::new();

            while !frontier.is_empty() {
                let (prefix, tree) = frontier.pop_front().unwrap();

                match tree {
                    LookupDirOrFile::Dir { name, children } => {
                        let cur_path = prefix.join(name);

                        result.push(Action::MakeDir {
                            path: cur_path.clone(),
                        });

                        for child in children.iter() {
                            frontier.push_back((cur_path.clone(), &child));
                        }
                    }
                    LookupDirOrFile::File { name, md5, size } => {
                        let cur_path = prefix.join(name);

                        result.push(Action::Download {
                            peers: peers_ptr.clone(),
                            peer_id: next_id,
                            path: cur_path,
                            md5: md5.clone(),
                            size: size.clone(),
                        });

                        next_id += 1;
                        next_id %= n_peers;
                    }
                }
            }
            Ok(result)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::LookupDirOrFile;
    use crate::tracker::{PeerInfo, PeersResponse};
    use axum::{Router, extract::Query, response::Json, routing::get};
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;

    // Mock tracker server that returns predefined peers
    async fn mock_tracker_handler() -> Json<PeersResponse> {
        let peers = vec![
            PeerInfo {
                addr: "http://127.0.0.1:18081".to_string(),
                last_seen: 1234567890,
            },
            PeerInfo {
                addr: "http://127.0.0.1:18082".to_string(),
                last_seen: 1234567890,
            },
        ];
        Json(PeersResponse { peers })
    }

    // Mock tracker that returns no peers
    async fn mock_tracker_no_peers_handler() -> Json<PeersResponse> {
        Json(PeersResponse { peers: vec![] })
    }

    // Mock peer server that returns file tree structure
    async fn mock_peer_handler(
        Query(params): Query<HashMap<String, String>>,
    ) -> Json<LookupDirOrFile> {
        let md5 = params.get("md5").map_or("default", |v| v.as_str());

        match md5 {
            "test_file_md5" => Json(LookupDirOrFile::File {
                name: "test.txt".to_string(),
                md5: "test_file_md5".to_string(),
                size: 100,
            }),
            "test_dir_md5" => Json(LookupDirOrFile::Dir {
                name: "test_dir".to_string(),
                children: vec![
                    LookupDirOrFile::File {
                        name: "file1.txt".to_string(),
                        md5: "file1_md5".to_string(),
                        size: 100,
                    },
                    LookupDirOrFile::File {
                        name: "file2.txt".to_string(),
                        md5: "file2_md5".to_string(),
                        size: 100,
                    },
                ],
            }),
            "nested_dir_md5" => Json(LookupDirOrFile::Dir {
                name: "parent".to_string(),
                children: vec![
                    LookupDirOrFile::Dir {
                        name: "subdir".to_string(),
                        children: vec![LookupDirOrFile::File {
                            name: "nested.txt".to_string(),
                            md5: "nested_file_md5".to_string(),
                            size: 100,
                        }],
                    },
                    LookupDirOrFile::File {
                        name: "root_file.txt".to_string(),
                        md5: "root_file_md5".to_string(),
                        size: 100,
                    },
                ],
            }),
            _ => Json(LookupDirOrFile::File {
                name: "default.txt".to_string(),
                md5: "default_md5".to_string(),
                size: 100,
            }),
        }
    }

    // Mock peer server that returns different tree (for mismatch test)
    async fn mock_peer_mismatch_handler(
        _: Query<HashMap<String, String>>,
    ) -> Json<LookupDirOrFile> {
        Json(LookupDirOrFile::File {
            name: "different.txt".to_string(),
            md5: "different_md5".to_string(),
            size: 100,
        })
    }

    async fn start_mock_tracker_server(port: u16) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let app = Router::new().route("/peers", get(mock_tracker_handler));
            let addr = SocketAddr::from(([127, 0, 0, 1], port));
            let listener = TcpListener::bind(addr).await.unwrap();
            axum::serve(listener, app).await.unwrap();
        })
    }

    async fn start_mock_peer_server(port: u16, mismatch: bool) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let app = if mismatch {
                Router::new().route("/query", get(mock_peer_mismatch_handler))
            } else {
                Router::new().route("/query", get(mock_peer_handler))
            };
            let addr = SocketAddr::from(([127, 0, 0, 1], port));
            let listener = TcpListener::bind(addr).await.unwrap();
            axum::serve(listener, app).await.unwrap();
        })
    }

    #[tokio::test]
    async fn test_plan_single_file() {
        // Start mock servers
        let _tracker_handle = start_mock_tracker_server(18080).await;
        let _peer1_handle = start_mock_peer_server(18081, false).await;
        let _peer2_handle = start_mock_peer_server(18082, false).await;

        // Wait for servers to start
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let planer = Planer::new(vec!["http://127.0.0.1:18080".to_string()]);
        let actions = planer.plan("test_file_md5").await.unwrap();

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::Download { path, md5, .. } => {
                assert_eq!(path.to_string_lossy(), "./test.txt");
                assert_eq!(md5, "test_file_md5");
            }
            _ => panic!("Expected Download action"),
        }
    }

    #[tokio::test]
    async fn test_plan_directory_with_files() {
        // Start mock servers
        let _tracker_handle = start_mock_tracker_server(18083).await;
        let _peer1_handle = start_mock_peer_server(18084, false).await;
        let _peer2_handle = start_mock_peer_server(18085, false).await;

        // Wait for servers to start
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let planer = Planer::new(vec!["http://127.0.0.1:18083".to_string()]);
        let actions = planer.plan("test_dir_md5").await.unwrap();

        assert_eq!(actions.len(), 3); // 1 MakeDir + 2 Download

        // First action should be MakeDir
        match &actions[0] {
            Action::MakeDir { path } => {
                assert_eq!(path.to_string_lossy(), "./test_dir");
            }
            _ => panic!("Expected MakeDir action"),
        }

        // Next actions should be Downloads
        for action in &actions[1..] {
            match action {
                Action::Download { path, md5, .. } => {
                    assert!(path.starts_with("./test_dir"));
                    assert!(md5 == "file1_md5" || md5 == "file2_md5");
                }
                _ => panic!("Expected Download action"),
            }
        }
    }

    #[tokio::test]
    async fn test_plan_nested_directory() {
        // Start mock servers
        let _tracker_handle = start_mock_tracker_server(18086).await;
        let _peer1_handle = start_mock_peer_server(18087, false).await;
        let _peer2_handle = start_mock_peer_server(18088, false).await;

        // Wait for servers to start
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let planer = Planer::new(vec!["http://127.0.0.1:18086".to_string()]);
        let actions = planer.plan("nested_dir_md5").await.unwrap();

        assert_eq!(actions.len(), 4); // 2 MakeDir + 2 Download

        let mut make_dir_count = 0;
        let mut download_count = 0;

        for action in &actions {
            match action {
                Action::MakeDir { .. } => make_dir_count += 1,
                Action::Download { .. } => download_count += 1,
            }
        }

        assert_eq!(make_dir_count, 2);
        assert_eq!(download_count, 2);
    }

    #[tokio::test]
    async fn test_plan_empty_tracker_urls() {
        let planer = Planer::new(vec![]);
        let result = planer.plan("test_md5").await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("tracker_urls is empty")
        );
    }

    #[tokio::test]
    async fn test_plan_invalid_tracker_url() {
        let planer = Planer::new(vec!["http://invalid-url:9999".to_string()]);
        let result = planer.plan("test_md5").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_plan_peer_tree_mismatch() {
        // Create a custom tracker that returns mismatched peers
        async fn mock_tracker_mismatch_handler() -> Json<PeersResponse> {
            let peers = vec![
                PeerInfo {
                    addr: "http://127.0.0.1:19081".to_string(),
                    last_seen: 1234567890,
                },
                PeerInfo {
                    addr: "http://127.0.0.1:19082".to_string(),
                    last_seen: 1234567890,
                },
            ];
            Json(PeersResponse { peers })
        }

        let _tracker_handle = tokio::spawn(async move {
            let app = Router::new().route("/peers", get(mock_tracker_mismatch_handler));
            let addr = SocketAddr::from(([127, 0, 0, 1], 19080));
            let listener = TcpListener::bind(addr).await.unwrap();
            axum::serve(listener, app).await.unwrap();
        });
        let _peer1_handle = start_mock_peer_server(19081, false).await;
        let _peer2_handle = start_mock_peer_server(19082, true).await; // mismatch

        // Wait for servers to start
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

        let planer = Planer::new(vec!["http://127.0.0.1:19080".to_string()]);
        let result = planer.plan("test_file_md5").await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("tree mismatch") || error_msg.contains("connection"));
    }

    #[tokio::test]
    async fn test_plan_multiple_trackers() {
        // Start multiple tracker servers (though they return same data)
        let _tracker1_handle = start_mock_tracker_server(18092).await;
        let _tracker2_handle = start_mock_tracker_server(18093).await;
        let _peer1_handle = start_mock_peer_server(18094, false).await;
        let _peer2_handle = start_mock_peer_server(18095, false).await;

        // Wait for servers to start
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let planer = Planer::new(vec![
            "http://127.0.0.1:18092".to_string(),
            "http://127.0.0.1:18093".to_string(),
        ]);
        let actions = planer.plan("test_file_md5").await.unwrap();

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::Download { path, md5, .. } => {
                assert_eq!(path.to_string_lossy(), "./test.txt");
                assert_eq!(md5, "test_file_md5");
            }
            _ => panic!("Expected Download action"),
        }
    }

    #[tokio::test]
    async fn test_action_peer_round_robin() {
        // Start mock servers
        let _tracker_handle = start_mock_tracker_server(18096).await;
        let _peer1_handle = start_mock_peer_server(18097, false).await;
        let _peer2_handle = start_mock_peer_server(18098, false).await;

        // Wait for servers to start
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let planer = Planer::new(vec!["http://127.0.0.1:18096".to_string()]);
        let actions = planer.plan("test_dir_md5").await.unwrap();

        // Check that peer_id values rotate (round robin)
        let download_actions: Vec<_> = actions
            .iter()
            .filter_map(|action| match action {
                Action::Download { peer_id, .. } => Some(*peer_id),
                _ => None,
            })
            .collect();

        assert_eq!(download_actions.len(), 2);
        assert_eq!(download_actions[0], 0);
        assert_eq!(download_actions[1], 1);
    }

    #[tokio::test]
    async fn test_plan_no_peers_available() {
        // Start a tracker server that returns empty peers list
        let _tracker_handle = tokio::spawn(async move {
            let app = Router::new().route("/peers", get(mock_tracker_no_peers_handler));
            let addr = SocketAddr::from(([127, 0, 0, 1], 18099));
            let listener = TcpListener::bind(addr).await.unwrap();
            axum::serve(listener, app).await.unwrap();
        });

        // Wait for tracker to start
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let planer = Planer::new(vec!["http://127.0.0.1:18099".to_string()]);
        let result = planer.plan("test_md5").await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(
            error_msg.contains("tracker_urls is empty") || error_msg.contains("no peers found")
        );
    }
}
