use crate::config::AppConfig;
use crate::generator::start_generators;
use crate::kafka::KafkaSink;
use crate::nonsense::Nonsense;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::{fmt, prelude::*};

mod config;
mod generator;
mod kafka;
mod nonsense;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    init_logger();
    configure_pool()?;

    let config = Arc::new(AppConfig::new()?);
    let throttle = if config.concurrency.throttling_enabled {
        Some(Duration::from_millis(config.concurrency.throttling_ms))
    } else {
        None
    };

    let (tx, rx) = crossbeam_channel::bounded::<Nonsense>(10_000);
    let (shutdown_tx, shutdown_rx) = crossbeam_channel::bounded::<()>(1);

    // Handle SIGINT
    {
        let shutdown_tx = shutdown_tx.clone();
        ctrlc::set_handler(move || {
            let _ = shutdown_tx.send(());
        })?;
    }

    start_generators(available_cores(), tx, throttle);
    tracing::info!("Started {} generator threads", available_cores());

    let sink = KafkaSink::new(config.clone())?;
    sink.dispatch_loop(rx, shutdown_rx).await?;

    Ok(())
}

fn init_logger() {
    tracing_subscriber::registry()
        .with(fmt::layer().json())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();
}

fn configure_pool() -> anyhow::Result<()> {
    rayon::ThreadPoolBuilder::new()
        .num_threads(available_cores())
        .build_global()
        .map_err(Into::into)
}

fn available_cores() -> usize {
    num_cpus::get_physical().saturating_sub(1).max(1)
}
