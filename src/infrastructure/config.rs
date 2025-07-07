#[derive(Debug, Clone)]
pub struct AppConfig {
    pub database_pool_size: u32,
    pub max_concurrent_operations: usize,
    pub max_requests_per_second: usize,
    pub batch_flush_interval_ms: u64,
    pub cache_size: usize,
    pub port: u16,
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
        }
    }
}
