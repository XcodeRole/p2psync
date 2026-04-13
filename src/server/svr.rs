use axum::{
    Json, Router,
    body::Body,
    extract::{Query, State},
    http::{StatusCode, HeaderMap, HeaderValue, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_binary::binary_stream::Endian;
use std::{
    collections::HashMap,
    fs::File,
    io::{ErrorKind, Write, stderr},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use crate::server::fs;
use crate::server::heart_beater::HeartBeater;
use axum::routing::get;
use tokio::{net::TcpListener, sync::RwLock};
use tokio::io::AsyncSeekExt;
use tokio_util::io::ReaderStream;

struct AppState {
    vfs: RwLock<Box<fs::VirtualFileSystem>>,
    pathes: Vec<PathBuf>,
}

#[derive(Debug, Serialize)]
struct AppStateDumpItem<'a> {
    vfs: &'a fs::VirtualFileSystem,
    pathes: &'a Vec<PathBuf>,
}

#[derive(Debug, Deserialize, Default)]
struct AppStateLoadItem {
    vfs: Box<fs::VirtualFileSystem>,
    pathes: Vec<PathBuf>,
}

impl AppState {
    pub fn new(pathes: Vec<String>, no_checksum: bool, manifest_out: Option<String>) -> std::io::Result<Self> {
        let mut vfs = Box::new(fs::VirtualFileSystem::new());
        let path_buffers = pathes.into_iter().map(PathBuf::from).collect::<Vec<_>>();
        for p in path_buffers.iter() {
            vfs.add(p.clone())?;
        }
        vfs.seal_with_options(no_checksum)?;
        vfs.dump_md5(stderr())?;

        // Write manifest file directly if --manifest-out is specified
        if let Some(ref manifest_path) = manifest_out {
            let manifest_file = File::create(manifest_path)?;
            let mut buf_writer = std::io::BufWriter::new(manifest_file);
            vfs.write_manifest(&mut buf_writer)?;
            buf_writer.flush()?;
            eprintln!("Manifest written to: {}", manifest_path);
        }

        Ok(Self {
            vfs: RwLock::new(vfs),
            pathes: path_buffers,
        })
    }

    pub fn load_from_binary(file: String) -> std::io::Result<Self> {
        let data = std::fs::read(file)?;
        let load_item: AppStateLoadItem = serde_binary::from_slice(&data, Endian::Little)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        Ok(Self {
            vfs: RwLock::new(load_item.vfs),
            pathes: load_item.pathes,
        })
    }

    pub async fn dump<W: Write>(&self, mut w: W) -> std::io::Result<()> {
        let read_guard = self.vfs.read().await;
        let dump_item = AppStateDumpItem {
            vfs: read_guard.as_ref(),
            pathes: &self.pathes,
        };

        let binary_data = serde_binary::to_vec(&dump_item, Endian::Little)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        w.write_all(&binary_data)?;
        Ok(())
    }
}

async fn query(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let md5 = match params.get("md5") {
        Some(_md5) => _md5,
        None => return StatusCode::BAD_REQUEST.into_response(),
    };

    let resp = match state.vfs.read().await.lookup(md5) {
        Some(_resp) => _resp,
        None => return StatusCode::NOT_FOUND.into_response(),
    };

    Json(resp).into_response()
}

async fn download(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let md5 = match params.get("md5") {
        Some(_md5) => _md5,
        None => return StatusCode::BAD_REQUEST.into_response(),
    };

    let path = match state.vfs.read().await.file_path(md5) {
        Ok(_path) => _path,
        Err(err) => {
            if err.kind() == ErrorKind::NotFound {
                return (StatusCode::NOT_FOUND, format!("File not found {}", md5)).into_response();
            } else {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Internal server error: {}", err),
                )
                    .into_response();
            }
        }
    };

    // Get file metadata for size info
    let file_metadata = match tokio::fs::metadata(&path).await {
        Ok(m) => m,
        Err(err) => {
            return (StatusCode::NOT_FOUND, format!("File not found: {}", err)).into_response();
        }
    };
    let file_size = file_metadata.len();

    let mut file = match tokio::fs::File::open(&path).await {
        Ok(file) => file,
        Err(err) => {
            return (StatusCode::NOT_FOUND, format!("File not found: {}", err)).into_response();
        }
    };

    // Parse Range header for resume support
    let range_start = parse_range_header(&headers, file_size);

    if let Some(start) = range_start {
        if start >= file_size {
            // Range not satisfiable
            let mut resp_headers = HeaderMap::new();
            resp_headers.insert(
                header::CONTENT_RANGE,
                HeaderValue::from_str(&format!("bytes */{}", file_size)).unwrap(),
            );
            return (StatusCode::RANGE_NOT_SATISFIABLE, resp_headers, "").into_response();
        }

        // Seek to the requested offset
        if let Err(err) = file.seek(std::io::SeekFrom::Start(start)).await {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Seek failed: {}", err),
            )
                .into_response();
        }

        let remaining = file_size - start;
        let stream = ReaderStream::with_capacity(file, 4 * 1024 * 1024);
        let body = Body::from_stream(stream);

        let mut resp_headers = HeaderMap::new();
        resp_headers.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes {}-{}/{}", start, file_size - 1, file_size))
                .unwrap(),
        );
        resp_headers.insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&remaining.to_string()).unwrap(),
        );
        resp_headers.insert(
            header::ACCEPT_RANGES,
            HeaderValue::from_static("bytes"),
        );

        (StatusCode::PARTIAL_CONTENT, resp_headers, body).into_response()
    } else {
        // No Range header — return full file
        let stream = ReaderStream::with_capacity(file, 4 * 1024 * 1024);
        let body = Body::from_stream(stream);

        let mut resp_headers = HeaderMap::new();
        resp_headers.insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&file_size.to_string()).unwrap(),
        );
        resp_headers.insert(
            header::ACCEPT_RANGES,
            HeaderValue::from_static("bytes"),
        );

        (StatusCode::OK, resp_headers, body).into_response()
    }
}

/// Parse "Range: bytes=START-" header, return Some(start) or None
fn parse_range_header(headers: &HeaderMap, _file_size: u64) -> Option<u64> {
    let range_val = headers.get(header::RANGE)?.to_str().ok()?;
    // Expected format: "bytes=START-" or "bytes=START-END"
    let range_str = range_val.strip_prefix("bytes=")?;
    let parts: Vec<&str> = range_str.splitn(2, '-').collect();
    if parts.is_empty() {
        return None;
    }
    let start: u64 = parts[0].parse().ok()?;
    Some(start)
}

fn build_app(app_state: Arc<AppState>) -> Router {
    let router = Router::new()
        .route("/query", get(query))
        .route("/download", get(download))
        .with_state(app_state);

    router
}

pub enum CreateArgs {
    Pathes { pathes: Vec<String>, no_checksum: bool, manifest_out: Option<String> },
    LoadPath(String),
}

pub async fn startup(
    args: CreateArgs,
    address: String,
    port: u16,
    dump_path: Option<String>,
    tracker: Vec<String>,
) -> std::io::Result<()> {
    let app_state = Arc::new(match args {
        CreateArgs::Pathes { pathes, no_checksum, manifest_out } => AppState::new(pathes, no_checksum, manifest_out)?,
        CreateArgs::LoadPath(path) => AppState::load_from_binary(path)?,
    });
    if let Some(dump_path) = dump_path {
        app_state.dump(File::create(dump_path)?).await?;
    }

    let app = build_app(app_state);

    let addr = format!("{}:{}", address, port);
    let listener = TcpListener::bind(&addr).await?;

    let heart_beater =
        HeartBeater::new(format!("http://{}", addr), tracker, Duration::from_secs(30));

    println!("Listening on http://{}", addr);

    axum::serve(listener, app).await?;

    heart_beater.stop();

    Ok(())
}
