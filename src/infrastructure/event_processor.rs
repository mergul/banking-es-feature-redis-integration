use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

#[async_trait]
pub trait EventProcessor: Send + Sync {
    async fn process_event(&self, event: Value) -> Result<()>;
}
