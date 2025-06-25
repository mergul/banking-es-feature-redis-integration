use tracing::{error, info, warn};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

fn main() {
    // Create logs directory
    std::fs::create_dir_all("logs").ok();

    // Configure file logging with rotation
    let file_appender = RollingFileAppender::new(Rotation::DAILY, "logs", "test.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    // Initialize tracing with both console and file output
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .with_writer(non_blocking)
        .init();

    println!("Testing file logging...");

    // Test different log levels
    info!("This is an INFO message - should appear in logs/test.log");
    warn!("This is a WARNING message - should appear in logs/test.log");
    error!("This is an ERROR message - should appear in logs/test.log");

    println!("Check the logs/test.log file to see if logging is working!");
    println!("Log files should be created in the logs/ directory.");
}
