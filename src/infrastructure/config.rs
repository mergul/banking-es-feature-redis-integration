use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataCaptureMethod {
    OutboxPoller,
    CdcDebezium,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DataCaptureConfig {
    pub method: DataCaptureMethod,
}

impl Default for DataCaptureConfig {
    fn default() -> Self {
        Self {
            method: DataCaptureMethod::CdcDebezium,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub database_pool_size: u32,
    pub max_concurrent_operations: usize,
    pub max_requests_per_second: usize,
    pub batch_flush_interval_ms: u64,
    pub cache_size: usize,
    pub port: u16,
    pub data_capture: DataCaptureConfig,
}

impl AppConfig {
    pub fn from_env() -> Self {
        let data_capture_method =
            std::env::var("DATA_CAPTURE_METHOD").unwrap_or_else(|_| "cdc_debezium".to_string());

        let method = match data_capture_method.as_str() {
            "outbox_poller" => DataCaptureMethod::OutboxPoller,
            _ => DataCaptureMethod::CdcDebezium,
        };

        Self {
            data_capture: DataCaptureConfig { method },
            ..Default::default()
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            database_pool_size: 10,
            max_concurrent_operations: 100,
            max_requests_per_second: 1000,
            batch_flush_interval_ms: 100,
            cache_size: 1000,
            port: 3000,
            data_capture: DataCaptureConfig::default(),
        }
    }
}
