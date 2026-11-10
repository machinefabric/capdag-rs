//! The `capdag` command: find capabilities, run them on files, and plan and
//! run machines of them. Everything is in [`capdag::cli`].

#[tokio::main]
async fn main() {
    std::process::exit(capdag::cli::main().await);
}
