use axum::{
    Json, Router,
    body::Body,
    extract::{Query, State},
    http::StatusCode,
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
    pub fn new(pathes: Vec<String>, no_checksum: bool) -> std::io::Result<Self> {
        let mut vfs = Box::new(fs::VirtualFileSystem::new());
        let path_buffers = pathes.into_iter().map(PathBuf::from).collect::<Vec<_>>();
        for p in path_buffers.iter() {
            vfs.add(p.clone())?;
        }
        vfs.seal_with_options(no_checksum)?;
        vfs.dump_md5(stderr())?;
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
    let file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(err) => {
            return (StatusCode::NOT_FOUND, format!("File not found: {}", err)).into_response();
        }
    };

    let stream = ReaderStream::with_capacity(file, 4 * 1024 * 1024);
    let body = Body::from_stream(stream);
    body.into_response()
}

fn build_app(app_state: Arc<AppState>) -> Router {
    let router = Router::new()
        .route("/query", get(query))
        .route("/download", get(download))
        .with_state(app_state);

    router
}

pub enum CreateArgs {
    Pathes { pathes: Vec<String>, no_checksum: bool },
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
        CreateArgs::Pathes { pathes, no_checksum } => AppState::new(pathes, no_checksum)?,
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
