use tokio::{fs, io::AsyncWriteExt};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::downloader::planer::Action;
use reqwest::Client;
use std::sync::{Arc, Mutex};
use std::{collections::VecDeque, io, path::Path, time::Duration};

/// Maximum number of retry attempts per URL before moving to next peer
const MAX_RETRIES_PER_URL: usize = 3;
/// Base delay between retries (exponential backoff)
const RETRY_BASE_DELAY_MS: u64 = 2000;
/// Buffer size for MD5 computation (8MB, same as serve side)
const MD5_BUFFER_SIZE: usize = 8 * 1024 * 1024;

struct DownloadURLs<'a> {
    md5: &'a str,
    peers: &'a Vec<String>,
    peer_id: usize,
    cur: usize,
}

impl<'a> Iterator for DownloadURLs<'a> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur >= self.peers.len() {
            None
        } else {
            let offset = (self.peer_id + self.cur) % self.peers.len();
            self.cur += 1;
            // URL-encode the md5/key value to handle paths with special characters
            let encoded_key = urlencoding::encode(self.md5);
            Some(format!("{}/download?md5={}", self.peers[offset], encoded_key))
        }
    }
}

/// Compute MD5 hash of a file asynchronously using streaming reads.
/// Uses 8MB buffer to avoid loading entire file into memory.
async fn compute_file_md5(file_path: &Path) -> Result<String, Box<dyn std::error::Error + Sync + Send>> {
    let mut file = tokio::fs::File::open(file_path).await?;
    let mut buffer = vec![0u8; MD5_BUFFER_SIZE];
    let mut ctx = md5::Context::new();

    loop {
        let n = file.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        ctx.consume(&buffer[..n]);
    }

    Ok(format!("{:x}", ctx.compute()))
}

async fn download_and_check(
    client: &Client,
    url: &str,
    md5: &str,
    file_path: &Path,
    pbar: Arc<Mutex<tqdm::Tqdm<()>>>,
    no_checksum: bool,
    expected_size: usize,
    resume: bool,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    // Check if file already exists
    let existing_size = if file_path.exists() {
        let metadata = tokio::fs::metadata(file_path).await?;
        metadata.len()
    } else {
        0
    };

    // In resume mode: if file already has the expected size, check if we can skip.
    // In overwrite mode: don't skip — will be handled by the overwrite logic below.
    if resume && expected_size > 0 && existing_size == expected_size as u64 {
        if !no_checksum {
            // Verify md5 of existing file
            let actual_md5 = compute_file_md5(file_path).await?;
            if actual_md5 != md5 {
                // MD5 mismatch — delete and re-download from scratch
                eprintln!(
                    "[MD5 MISMATCH] {} (expected: {}, got: {}), re-downloading...",
                    file_path.display(), md5, actual_md5
                );
                tokio::fs::remove_file(file_path).await?;
                // Fall through to fresh download below (existing_size is now stale but we'll create new file)
            } else {
                // File is correct, skip
                pbar.lock().unwrap().update(expected_size)?;
                return Ok(());
            }
        } else {
            // no_checksum mode: trust size, skip
            pbar.lock().unwrap().update(expected_size)?;
            return Ok(());
        }
    }

    // Re-check existing size after potential deletion above
    let existing_size = if file_path.exists() {
        let metadata = tokio::fs::metadata(file_path).await?;
        metadata.len()
    } else {
        0
    };

    // Decide whether to resume or overwrite
    let mut request = client.get(url);
    let resume_offset = if resume && existing_size > 0 {
        // Resume mode: send Range header to continue from where we left off
        if expected_size > 0 && existing_size > expected_size as u64 {
            // File is larger than expected — corrupted, delete and start fresh
            eprintln!(
                "[CORRUPT] {} is larger than expected ({} > {}), deleting and re-downloading...",
                file_path.display(), existing_size, expected_size
            );
            tokio::fs::remove_file(file_path).await?;
            0
        } else {
            request = request.header("Range", format!("bytes={}-", existing_size));
            existing_size
        }
    } else if existing_size > 0 {
        // Overwrite mode (default): delete existing file and start fresh
        tokio::fs::remove_file(file_path).await?;
        0
    } else {
        0
    };

    let resp = request.send().await?;
    let status = resp.status();

    // Handle response based on status code
    let mut output_file = if status == reqwest::StatusCode::PARTIAL_CONTENT && resume_offset > 0 {
        // Server supports Range — append to existing file
        // Update progress bar for already-downloaded bytes
        pbar.lock().unwrap().update(resume_offset as usize)?;
        let f = tokio::fs::OpenOptions::new()
            .write(true)
            .open(file_path)
            .await?;
        // Seek to end to append
        let mut f = f;
        f.seek(std::io::SeekFrom::End(0)).await?;
        f
    } else if status == reqwest::StatusCode::OK {
        // Server returned full file (no Range support or fresh start)
        fs::File::create(file_path).await?
    } else if status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE {
        // 416: check if file size matches expected
        if expected_size > 0 && resume_offset != expected_size as u64 {
            return Err(format!(
                "Range not satisfiable and file size mismatch: existing={} expected={} for {}",
                resume_offset, expected_size, file_path.display()
            ).into());
        }
        // File is already complete on server side
        pbar.lock().unwrap().update(expected_size)?;
        return Ok(());
    } else {
        return Err(format!("HTTP error: {} for URL: {}", status, url).into());
    };

    let mut resp = resp;

    // Use larger buffer for better performance
    const BUFFER_SIZE: usize = 4 * 1024 * 1024; // 4MB buffer
    let mut buffer = Vec::with_capacity(BUFFER_SIZE);

    while let Some(chunk) = resp.chunk().await? {
        buffer.extend_from_slice(&chunk);

        pbar.lock().unwrap().update(chunk.len())?;
        // Write buffer to file when it's large enough
        if buffer.len() >= BUFFER_SIZE {
            output_file.write_all(&buffer).await?;
            buffer.clear();
        }
    }

    // Write remaining buffer content
    if !buffer.is_empty() {
        output_file.write_all(&buffer).await?;
    }

    // Ensure all data is written to disk
    output_file.flush().await?;

    // Verify MD5 after download if checksum is enabled
    if !no_checksum {
        let actual_md5 = compute_file_md5(file_path).await?;
        if actual_md5 != md5 {
            return Err(format!(
                "MD5 verification failed for {}: expected={}, actual={}",
                file_path.display(), md5, actual_md5
            ).into());
        }
    }

    Ok(())
}

async fn execute_action(
    action: Action,
    pbar: Arc<Mutex<tqdm::Tqdm<()>>>,
    client: Arc<Client>,
    no_checksum: bool,
    resume: bool,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    Ok(match action {
        Action::Download {
            peers,
            peer_id,
            path: file_path,
            md5,
            size,
        } => {
            if let Some(parent) = file_path.parent() {
                fs::create_dir_all(parent).await?;
            }

            // In resume mode, check if file already exists with correct size to skip re-download.
            // In overwrite mode (default), always re-download — no skipping.
            if resume && file_path.exists() {
                let metadata = tokio::fs::metadata(&file_path).await?;
                if metadata.len() == size as u64 {
                    if !no_checksum {
                        // Verify md5 before skipping
                        let actual_md5 = compute_file_md5(&file_path).await?;
                        if actual_md5 == md5 {
                            println!("[SKIP] {} (already complete and verified, {} bytes)", file_path.display(), size);
                            pbar.lock().unwrap().update(size)?;
                            return Ok(());
                        } else {
                            eprintln!(
                                "[MD5 MISMATCH] {} (expected: {}, got: {}), re-downloading...",
                                file_path.display(), md5, actual_md5
                            );
                            // Don't skip — fall through to download
                        }
                    } else {
                        println!("[SKIP] {} (already complete, {} bytes)", file_path.display(), size);
                        pbar.lock().unwrap().update(size)?;
                        return Ok(());
                    }
                }
            }

            // Extract peers data before async operations
            let peers_vec = {
                let peers_read_guard = peers
                    .read()
                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
                peers_read_guard.clone()
            };

            let urls = DownloadURLs {
                md5: &md5,
                peers: &peers_vec,
                peer_id,
                cur: 0,
            };

            let mut errs = Vec::new();
            for url in urls {
                // Retry each peer up to MAX_RETRIES_PER_URL times with exponential backoff
                let mut succeeded = false;
                for attempt in 0..MAX_RETRIES_PER_URL {
                    match download_and_check(
                        &client,
                        url.as_str(),
                        &md5,
                        file_path.as_path(),
                        pbar.clone(),
                        no_checksum,
                        size,
                        resume,
                    )
                    .await
                    {
                        Ok(()) => {
                            succeeded = true;
                            break;
                        }
                        Err(err) => {
                            let delay = Duration::from_millis(RETRY_BASE_DELAY_MS * (1 << attempt));
                            eprintln!(
                                "[RETRY] Attempt {}/{} failed for {}: {}. Retrying in {:?}...",
                                attempt + 1, MAX_RETRIES_PER_URL, url, err, delay
                            );
                            errs.push(err);
                            if attempt + 1 < MAX_RETRIES_PER_URL {
                                tokio::time::sleep(delay).await;
                            }
                        }
                    }
                }
                if succeeded {
                    return Ok(());
                }
            }

            let error_msgs: Vec<String> = errs.iter().map(|e| e.to_string()).collect();
            return Err(format!("All download attempts failed after retries: {}", error_msgs.join("; ")).into());
        }
        Action::MakeDir { path } => fs::create_dir_all(path).await?,
    })
}

fn human_readable_size(bytes: usize) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];
    let mut size = bytes as f64;
    for unit in UNITS {
        if size < 1024.0 {
            return format!("{:.2} {}", size, unit);
        }
        size /= 1024.0;
    }
    format!("{:.2} EB", size)
}

fn total_size(actions: &Vec<Action>) -> usize {
    let mut total_size = 0;
    for action in actions {
        match action {
            Action::Download { size, .. } => total_size += size,
            Action::MakeDir { .. } => {}
        }
    }
    total_size
}

pub async fn execute_actions(
    actions: &Vec<Action>,
    concurrency: usize,
    no_checksum: bool,
    resume: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Create optimized reqwest client with larger buffers and better performance settings
    // NOTE: Do NOT set a global .timeout() — it caps the ENTIRE request including body
    // transfer. For large files (tens of GB), this causes silent truncation.
    // Use connect_timeout + read_timeout instead.
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(30))
        .read_timeout(Duration::from_secs(600))
        .pool_max_idle_per_host(20)
        .pool_idle_timeout(Duration::from_secs(60))
        .tcp_keepalive(Duration::from_secs(60))
        .tcp_nodelay(true)
        .http2_keep_alive_interval(Duration::from_secs(30))
        .http2_keep_alive_timeout(Duration::from_secs(10))
        .http2_adaptive_window(true)
        .http2_initial_stream_window_size(Some(1024 * 1024)) // 1MB initial window
        .http2_initial_connection_window_size(Some(2 * 1024 * 1024)) // 2MB connection window
        .build()?;
    let client = Arc::new(client);
    let mut handles = VecDeque::new();
    let mut errs = Vec::new();

    // Print file list with sizes
    let mut file_count = 0;
    let total = total_size(actions);
    println!("\n========== Download File List ==========");
    for action in actions {
        if let Action::Download { path, size, .. } = action {
            file_count += 1;
            println!("  [{}] {} ({})", file_count, path.display(), human_readable_size(*size));
        }
    }
    println!("========================================");
    println!("Total: {} files, {}\n", file_count, human_readable_size(total));

    let tqdm = Arc::new(Mutex::new(
        tqdm::pbar(Some(total)).desc(Some("download")),
    ));
    for action in actions {
        let no_checksum = no_checksum;
        let resume = resume;
        handles.push_back(tokio::spawn(execute_action(
            action.clone(),
            tqdm.clone(),
            client.clone(),
            no_checksum,
            resume,
        )));

        if handles.len() > concurrency {
            let handle = handles.pop_front().unwrap();
            match handle.await {
                Err(join_err) => errs.push(format!("Task panicked: {}", join_err)),
                Ok(Err(task_err)) => errs.push(format!("Download failed: {}", task_err)),
                Ok(Ok(())) => {}
            }
        }

        if !errs.is_empty() {
            break;
        }
    }

    while let Some(handle) = handles.pop_front() {
        match handle.await {
            Err(join_err) => errs.push(format!("Task panicked: {}", join_err)),
            Ok(Err(task_err)) => errs.push(format!("Download failed: {}", task_err)),
            Ok(Ok(())) => {}
        }
    }

    if !errs.is_empty() {
        return Err(format!("Some tasks failed: {}", errs.join("; ")).into());
    }

    Ok(())
}
