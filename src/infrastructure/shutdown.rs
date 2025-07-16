use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;

#[async_trait]
pub trait Shutdown: Send + Sync {
    async fn shutdown(&self) -> Result<()>;
}

pub struct ShutdownManager {
    shutdown_tx: mpsc::Sender<()>,
}

impl ShutdownManager {
    pub fn new(shutdown_tx: mpsc::Sender<()>) -> Self {
        Self { shutdown_tx }
    }
}

#[async_trait]
impl Shutdown for ShutdownManager {
    async fn shutdown(&self) -> Result<()> {
        self.shutdown_tx.send(()).await?;
        Ok(())
    }
}
