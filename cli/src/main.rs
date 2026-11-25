//! The `capdag` command: find capabilities, run them on files, and plan and
//! run machines of them — capdag-tuikit's command line, on the engine.

#[tokio::main]
async fn main() {
    let host = std::sync::Arc::new(capdag_cli::engine::EngineHost::new());
    std::process::exit(capdag_tuikit::cli::run(host).await);
}
