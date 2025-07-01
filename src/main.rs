use crate::generator::start_generators;
use crate::nonsense::Nonsense;
use tracing::{debug, debug_span};
use tracing_subscriber::{fmt, prelude::*};

mod generator;
mod nonsense;

fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    init_logger();
    configure_pool()?;

    let (tx, rx) = crossbeam_channel::bounded::<Nonsense>(10000);
    start_generators(available_cores(), tx, None);

    for msg in rx.iter() {
        debug_span!("received_message").in_scope(|| {
            debug!("RECV: {:?}", msg);
        });
    }

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
    num_cpus::get_physical() - 1
}
