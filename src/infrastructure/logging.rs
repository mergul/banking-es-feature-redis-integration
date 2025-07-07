use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{error, info, Level, Subscriber};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{
    fmt::{self},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter, Layer,
};

/// Configuration for file logging
#[derive(Debug, Clone)]
pub struct LoggingConfig {
    pub log_dir: String,
    pub max_files: usize,
    pub max_file_size: usize,
    pub enable_console: bool,
    pub enable_file: bool,
    pub log_level: Level,
    pub enable_json: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            log_dir: "logs".to_string(),
            max_files: 30,                    // Keep 30 days of logs
            max_file_size: 100 * 1024 * 1024, // 100MB
            enable_console: true,
            enable_file: true,
            log_level: Level::INFO,
            enable_json: false,
        }
    }
}

/// Initialize comprehensive logging with file rotation and structured output
pub fn init_logging(config: Option<LoggingConfig>) -> Result<(), Box<dyn std::error::Error>> {
    let config = config.unwrap_or_default();

    // Create log directory if it doesn't exist
    fs::create_dir_all(&config.log_dir)?;

    // Create log files for different levels
    let error_appender = RollingFileAppender::new(Rotation::DAILY, &config.log_dir, "error.log");
    let warn_appender = RollingFileAppender::new(Rotation::DAILY, &config.log_dir, "warn.log");
    let info_appender = RollingFileAppender::new(Rotation::DAILY, &config.log_dir, "info.log");
    let debug_appender = RollingFileAppender::new(Rotation::DAILY, &config.log_dir, "debug.log");

    // Create non-blocking writers
    let (error_writer, _error_guard) = tracing_appender::non_blocking(error_appender);
    let (warn_writer, _warn_guard) = tracing_appender::non_blocking(warn_appender);
    let (info_writer, _info_guard) = tracing_appender::non_blocking(info_appender);
    let (debug_writer, _debug_guard) = tracing_appender::non_blocking(debug_appender);

    // Create combined appender for all logs
    let all_appender =
        RollingFileAppender::new(Rotation::DAILY, &config.log_dir, "banking-service.log");
    let (all_writer, _all_guard) = tracing_appender::non_blocking(all_appender);

    // Configure environment filter
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        let filter_str = env!("CARGO_PKG_NAME").to_string()
            + "="
            + &config.log_level.to_string()
            + ",banking_es="
            + &config.log_level.to_string()
            + ",sqlx=warn,rdkafka=warn";
        EnvFilter::new(filter_str)
    });

    // Create layers
    let mut layers: Vec<Box<dyn Layer<_> + Send + Sync>> = Vec::new();

    // Console layer
    if config.enable_console {
        let console_layer = fmt::layer()
            .with_target(false)
            .with_thread_ids(true)
            .with_file(true)
            .with_line_number(true)
            .with_level(true)
            .with_ansi(true);
        layers.push(Box::new(console_layer));
    }

    // File layers for different levels
    if config.enable_file {
        // Error level file
        let error_layer = fmt::layer()
            .with_writer(error_writer)
            .with_target(false)
            .with_thread_ids(true)
            .with_file(true)
            .with_line_number(true)
            .with_level(true)
            .with_ansi(false)
            .with_filter(tracing_subscriber::filter::LevelFilter::ERROR);
        layers.push(Box::new(error_layer));

        // Warn level file
        let warn_layer = fmt::layer()
            .with_writer(warn_writer)
            .with_target(false)
            .with_thread_ids(true)
            .with_file(true)
            .with_line_number(true)
            .with_level(true)
            .with_ansi(false)
            .with_filter(tracing_subscriber::filter::LevelFilter::WARN);
        layers.push(Box::new(warn_layer));

        // Info level file
        let info_layer = fmt::layer()
            .with_writer(info_writer)
            .with_target(false)
            .with_thread_ids(true)
            .with_file(true)
            .with_line_number(true)
            .with_level(true)
            .with_ansi(false)
            .with_filter(tracing_subscriber::filter::LevelFilter::INFO);
        layers.push(Box::new(info_layer));

        // Debug level file
        let debug_layer = fmt::layer()
            .with_writer(debug_writer)
            .with_target(false)
            .with_thread_ids(true)
            .with_file(true)
            .with_line_number(true)
            .with_level(true)
            .with_ansi(false)
            .with_filter(tracing_subscriber::filter::LevelFilter::DEBUG);
        layers.push(Box::new(debug_layer));

        // Combined log file
        let all_layer = fmt::layer()
            .with_writer(all_writer)
            .with_target(false)
            .with_thread_ids(true)
            .with_file(true)
            .with_line_number(true)
            .with_level(true)
            .with_ansi(false);
        layers.push(Box::new(all_layer));
    }

    // Initialize subscriber with all layers
    tracing_subscriber::registry()
        .with(env_filter)
        .with(layers)
        .init();

    // eprintln!("Logging initialized successfully");
    // eprintln!("Log directory: {}", config.log_dir);
    // eprintln!("Log level: {}", config.log_level);
    // eprintln!("Console logging: {}", config.enable_console);
    // eprintln!("File logging: {}", config.enable_file);

    Ok(())
}

/// Clean up old log files based on retention policy
pub fn cleanup_old_logs(log_dir: &str, max_files: usize) -> Result<(), Box<dyn std::error::Error>> {
    let log_path = Path::new(log_dir);
    if !log_path.exists() {
        return Ok(());
    }

    let mut log_files: Vec<_> = fs::read_dir(log_path)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext == "log")
                .unwrap_or(false)
        })
        .collect();

    // Sort by modification time (oldest first)
    log_files.sort_by_key(|entry| {
        entry
            .metadata()
            .and_then(|meta| meta.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
    });

    // Remove oldest files if we have more than max_files
    if log_files.len() > max_files {
        let files_to_remove = log_files.len() - max_files;
        for entry in log_files.iter().take(files_to_remove) {
            if let Err(_e) = fs::remove_file(entry.path()) {
                // Failed to remove old log file
            } else {
                // Successfully removed old log file
            }
        }
    }

    Ok(())
}

/// Get log file statistics
pub fn get_log_stats(log_dir: &str) -> Result<LogStats, Box<dyn std::error::Error>> {
    let log_path = Path::new(log_dir);
    if !log_path.exists() {
        return Ok(LogStats::default());
    }

    let mut stats = LogStats::default();

    for entry in fs::read_dir(log_path)? {
        let entry = entry?;
        let path = entry.path();

        if let Some(extension) = path.extension() {
            if extension == "log" {
                stats.total_files += 1;

                if let Ok(metadata) = entry.metadata() {
                    stats.total_size += metadata.len();

                    if let Ok(modified) = metadata.modified() {
                        if let Ok(duration) = modified.elapsed() {
                            if duration.as_secs() < 24 * 60 * 60 {
                                stats.files_last_24h += 1;
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(stats)
}

#[derive(Debug, Default)]
pub struct LogStats {
    pub total_files: usize,
    pub total_size: u64,
    pub files_last_24h: usize,
}

impl LogStats {
    pub fn total_size_mb(&self) -> f64 {
        self.total_size as f64 / (1024.0 * 1024.0)
    }

    pub fn total_size_gb(&self) -> f64 {
        self.total_size as f64 / (1024.0 * 1024.0 * 1024.0)
    }
}

/// Log rotation task that runs periodically
pub async fn start_log_rotation_task(
    log_dir: String,
    max_files: usize,
    rotation_interval: std::time::Duration,
) {
    let mut interval = tokio::time::interval(rotation_interval);

    loop {
        interval.tick().await;

        if let Err(e) = cleanup_old_logs(&log_dir, max_files) {
            error!("Failed to cleanup old logs: {}", e);
        }

        // Log statistics
        if let Ok(stats) = get_log_stats(&log_dir) {
            info!(
                "Log statistics: {} files, {:.2} MB total, {} files in last 24h",
                stats.total_files,
                stats.total_size_mb(),
                stats.files_last_24h
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_stats() {
        // Test implementation
    }

    #[test]
    fn test_cleanup_old_logs() {
        // Test implementation
    }
}
