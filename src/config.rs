#[derive(Debug, Clone, serde::Deserialize)]
pub struct AppConfig {
    pub producer: ProducerConfig,
    pub dispatch: DispatchConfig,
    pub concurrency: ConcurrencyConfig,
}

impl AppConfig {
    pub fn new() -> anyhow::Result<Self> {
        let config = config::Config::builder()
            .add_source(config::File::with_name("Config").required(false))
            .build()?;

        config.try_deserialize::<AppConfig>().map_err(|e| e.into())
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ProducerConfig {
    pub brokers: String,
    pub topic: String,
    pub compression: String,
    pub acks: String,
    pub timeout_ms: u64,
    pub buffering_max_ms: u64,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct DispatchConfig {
    pub batch_size: usize,
    pub flush_interval_ms: u64,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ConcurrencyConfig {
    pub throttling_enabled: bool,
    pub throttling_ms: u64,
}
