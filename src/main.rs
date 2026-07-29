use std::{process::ExitCode, time::Duration};

use clap::{Parser, Subcommand};
use smithers_nanocodex::{Capabilities, capabilities::BRIDGE_PROTOCOL_VERSION, serve};

const SERVE_WORKER_THREADS: usize = 2;

#[derive(Debug, Parser)]
#[command(name = "smithers-nanocodex", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Print side-effect-free bridge capability metadata.
    Capabilities {
        /// Emit machine-readable JSON (the default output is also JSON).
        #[arg(long)]
        json: bool,
    },
    /// Serve one headless Nanocodex turn over stdin/stdout JSONL.
    Serve {
        /// Exact bridge protocol version requested by the parent.
        #[arg(long)]
        protocol_version: u16,
    },
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Capabilities { json } => {
            let capabilities = Capabilities::current();
            let encoded = if json {
                serde_json::to_string(&capabilities)
            } else {
                serde_json::to_string_pretty(&capabilities)
            };
            match encoded {
                Ok(encoded) => {
                    println!("{encoded}");
                    ExitCode::SUCCESS
                }
                Err(_) => ExitCode::from(5),
            }
        }
        Command::Serve { protocol_version } => {
            if protocol_version != BRIDGE_PROTOCOL_VERSION {
                eprintln!("unsupported bridge protocol version");
                return ExitCode::from(2);
            }
            let runtime = match serve_runtime() {
                Ok(runtime) => runtime,
                Err(_) => return ExitCode::from(5),
            };
            let result = runtime.block_on(serve());
            // Tokio's portable stdin adapter may have an OS blocking read in
            // progress. A bounded runtime shutdown lets this one-shot process
            // exit after SIGINT/SIGTERM/EPIPE without waiting for the parent
            // to close stdin.
            runtime.shutdown_timeout(Duration::from_millis(100));
            match result {
                Ok(exit) => ExitCode::from(exit.code()),
                Err(_) => {
                    eprintln!("bridge input/output failed");
                    ExitCode::from(5)
                }
            }
        }
    }
}

fn serve_runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        // A bridge process serves exactly one turn. Two async workers keep
        // signal and command handling responsive without scaling the runtime
        // footprint with the host CPU count. Tokio's separate blocking pool
        // remains available for portable stdin and native tool operations.
        .worker_threads(SERVE_WORKER_THREADS)
        .enable_all()
        .build()
}

#[cfg(test)]
mod tests {
    use super::{SERVE_WORKER_THREADS, serve_runtime};

    #[test]
    fn serve_runtime_has_a_bounded_worker_pool() {
        let runtime = serve_runtime().expect("serve runtime must build");
        assert_eq!(runtime.metrics().num_workers(), SERVE_WORKER_THREADS);
    }
}
