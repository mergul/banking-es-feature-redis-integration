use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, PartialEq)]
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
            database_pool_size: std::env::var("DB_MAX_CONNECTIONS")
                .unwrap_or_else(|_| "80".to_string())
                .parse()
                .unwrap_or(80),
            max_concurrent_operations: std::env::var("MAX_CONCURRENT_OPERATIONS")
                .unwrap_or_else(|_| "200".to_string())
                .parse()
                .unwrap_or(200),
            max_requests_per_second: std::env::var("MAX_REQUESTS_PER_SECOND")
                .unwrap_or_else(|_| "1000".to_string())
                .parse()
                .unwrap_or(1000),
            batch_flush_interval_ms: std::env::var("BATCH_FLUSH_INTERVAL_MS")
                .unwrap_or_else(|_| "100".to_string())
                .parse()
                .unwrap_or(100),
            cache_size: std::env::var("CACHE_MAX_SIZE")
                .unwrap_or_else(|_| "5000".to_string())
                .parse()
                .unwrap_or(5000),
            port: std::env::var("PORT")
                .unwrap_or_else(|_| "3000".to_string())
                .parse()
                .unwrap_or(3000),
            data_capture: DataCaptureConfig { method },
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            database_pool_size: std::env::var("DB_MAX_CONNECTIONS")
                .unwrap_or_else(|_| "80".to_string())
                .parse()
                .unwrap_or(80),
            max_concurrent_operations: std::env::var("MAX_CONCURRENT_OPERATIONS")
                .unwrap_or_else(|_| "200".to_string())
                .parse()
                .unwrap_or(200),
            max_requests_per_second: std::env::var("MAX_REQUESTS_PER_SECOND")
                .unwrap_or_else(|_| "1000".to_string())
                .parse()
                .unwrap_or(1000),
            batch_flush_interval_ms: std::env::var("BATCH_FLUSH_INTERVAL_MS")
                .unwrap_or_else(|_| "100".to_string())
                .parse()
                .unwrap_or(100),
            cache_size: std::env::var("CACHE_MAX_SIZE")
                .unwrap_or_else(|_| "5000".to_string())
                .parse()
                .unwrap_or(5000),
            port: std::env::var("PORT")
                .unwrap_or_else(|_| "3000".to_string())
                .parse()
                .unwrap_or(3000),
            data_capture: DataCaptureConfig::default(),
        }
    }
}
