use clap::{Parser, Subcommand};

mod tracker;
use tracker::TrackerServer;

mod server;
use server::{CreateArgs, startup};

mod downloader;
use downloader::download;

#[derive(Parser)]
#[command(name = "p2psync")]
#[command(about = "A peer-to-peer file synchronization tool")]
#[command(version = "1.0")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Tracker {
        #[arg(short, long, default_value_t = 9090)]
        port: u16,
    },
    Serve {
        #[arg(long, help = "Directory to monitor")]
        path: Vec<String>,
        #[arg(long, help = "Address to bind to")]
        address: String,
        #[arg(long, default_value_t = 8080)]
        port: u16,
        #[arg(short, long, help = "dump binary")]
        dump_path: Option<String>,
        #[arg(short, long, help = "load binary")]
        load_path: Option<String>,
        #[arg(short, long, help = "tracker address")]
        tracker: Vec<String>,
        #[arg(long, help = "Skip MD5 checksum, use file path as key instead", default_value_t = false)]
        no_checksum: bool,
    },
    Download {
        #[arg(short, long, help = "md5 or path-based identifier")]
        md5: String,
        #[arg(short, long, help = "concurrency", default_value_t = 10)]
        concurrency: usize,
        #[arg(short, long, help = "tracker address")]
        tracker: Vec<String>,
        #[arg(long, help = "Skip MD5 verification after download", default_value_t = false)]
        no_checksum: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Tracker { port }) => {
            println!("Starting tracker on port {}", port);
            let server = TrackerServer::new();
            server.start(port).await?;
        }
        Some(Commands::Serve {
            path,
            address,
            port,
            dump_path,
            load_path,
            tracker,
            no_checksum,
        }) => {
            startup(
                if path.is_empty() {
                    match load_path {
                        Some(path) => CreateArgs::LoadPath(path),
                        None => {
                            panic!("--load-path or --path must set")
                        }
                    }
                } else {
                    CreateArgs::Pathes { pathes: path, no_checksum }
                },
                address,
                port,
                dump_path,
                tracker,
            )
            .await?;
        }

        Some(Commands::Download {
            md5,
            concurrency,
            tracker,
            no_checksum,
        }) => {
            if let Err(err) = download(md5, concurrency, tracker, no_checksum).await {
                eprintln!("download: {}", err);
                panic!("download failed");
            }
        }

        None => {
            println!("Use --help for available commands");
        }
    }

    Ok(())
}
