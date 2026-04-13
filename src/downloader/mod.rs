mod executor;
mod planer;

pub async fn download(
    md5: String,
    concurrency: usize,
    tracker_urls: Vec<String>,
    no_checksum: bool,
    resume: bool,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let planer = planer::Planer::new(tracker_urls);
    let actions = planer.plan(md5.as_str()).await?;
    executor::execute_actions(&actions, concurrency, no_checksum, resume).await
}
