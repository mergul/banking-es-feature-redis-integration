use crate::infrastructure::redis_abstraction::{RedisClient, RedisClientTrait};
use anyhow::Result;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInstance {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub status: InstanceStatus,
    pub metrics: InstanceMetrics,
    pub shard_assignments: Vec<ShardId>,
    pub last_heartbeat: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum InstanceStatus {
    Active,
    Starting,
    Stopping,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceMetrics {
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub request_count: u64,
    pub error_count: u64,
    pub latency_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingConfig {
    pub min_instances: usize,
    pub max_instances: usize,
    pub scale_up_threshold: f64,
    pub scale_down_threshold: f64,
    pub cooldown_period: Duration,
    pub health_check_interval: Duration,
    pub instance_timeout: Duration,
}

impl Default for ScalingConfig {
    fn default() -> Self {
        Self {
            min_instances: 2,
            max_instances: 10,
            scale_up_threshold: 0.8,    // 80% resource usage
            scale_down_threshold: 0.3,  // 30% resource usage
            cooldown_period: Duration::from_secs(300),  // 5 minutes
            health_check_interval: Duration::from_secs(30),
            instance_timeout: Duration::from_secs(60),
        }
    }
}

pub struct ScalingManager {
    redis_client: Arc<dyn RedisClientTrait>,
    config: ScalingConfig,
    instances: Arc<DashMap<String, ServiceInstance>>,
    last_scale_time: Arc<RwLock<DateTime<Utc>>>,
    metrics: Arc<ScalingMetrics>,
}

#[derive(Debug, Default)]
pub struct ScalingMetrics {
    pub scale_up_operations: AtomicU64,
    pub scale_down_operations: AtomicU64,
    pub failed_scale_operations: AtomicU64,
    pub instance_failures: AtomicU64,
}

impl ScalingManager {
    pub fn new(redis_client: Arc<dyn RedisClientTrait>, config: ScalingConfig) -> Self {
        Self {
            redis_client,
            config,
            instances: Arc::new(DashMap::new()),
            last_scale_time: Arc::new(RwLock::new(Utc::now())),
            metrics: Arc::new(ScalingMetrics::default()),
        }
    }

    pub async fn register_instance(&self, instance: ServiceInstance) -> Result<()> {
        let key = format!("instance:{}", instance.id);
        let value = serde_json::to_string(&instance)?;
        
        self.redis_client
            .get_async_connection()
            .await?
            .set_ex_bytes(key.as_bytes(), value.as_bytes(), 60)
            .await?;

        self.instances.insert(instance.id.clone(), instance);
        info!("Registered new instance: {}", instance.id);
        Ok(())
    }

    pub async fn update_instance_metrics(&self, instance_id: &str, metrics: InstanceMetrics) -> Result<()> {
        if let Some(mut instance) = self.instances.get_mut(instance_id) {
            instance.metrics = metrics;
            instance.last_heartbeat = Utc::now();
            
            let key = format!("instance:{}", instance_id);
            let value = serde_json::to_string(&*instance)?;
            
            self.redis_client
                .get_async_connection()
                .await?
                .set_ex_bytes(key.as_bytes(), value.as_bytes(), 60)
                .await?;
        }
        Ok(())
    }

    pub async fn start_scaling_manager(&self) -> Result<()> {
        let instances = self.instances.clone();
        let config = self.config.clone();
        let last_scale_time = self.last_scale_time.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            loop {
                if let Err(e) = Self::check_and_scale(
                    &instances,
                    &config,
                    &last_scale_time,
                    &metrics,
                ).await {
                    error!("Scaling check failed: {}", e);
                }
                tokio::time::sleep(config.health_check_interval).await;
            }
        });

        Ok(())
    }

    async fn check_and_scale(
        instances: &DashMap<String, ServiceInstance>,
        config: &ScalingConfig,
        last_scale_time: &RwLock<DateTime<Utc>>,
        metrics: &ScalingMetrics,
    ) -> Result<()> {
        let now = Utc::now();
        let last_scale = *last_scale_time.read().await;
        
        if now.signed_duration_since(last_scale) < chrono::Duration::from_std(config.cooldown_period)? {
            return Ok(());
        }

        let active_instances: Vec<_> = instances
            .iter()
            .filter(|i| i.status == InstanceStatus::Active)
            .collect();

        let total_cpu: f64 = active_instances.iter().map(|i| i.metrics.cpu_usage).sum();
        let total_memory: f64 = active_instances.iter().map(|i| i.metrics.memory_usage).sum();
        let avg_cpu = total_cpu / active_instances.len() as f64;
        let avg_memory = total_memory / active_instances.len() as f64;

        if avg_cpu > config.scale_up_threshold || avg_memory > config.scale_up_threshold {
            if active_instances.len() < config.max_instances {
                Self::scale_up(instances, metrics).await?;
                *last_scale_time.write().await = now;
            }
        } else if avg_cpu < config.scale_down_threshold && avg_memory < config.scale_down_threshold {
            if active_instances.len() > config.min_instances {
                Self::scale_down(instances, metrics).await?;
                *last_scale_time.write().await = now;
            }
        }

        Ok(())
    }

    async fn scale_up(instances: &DashMap<String, ServiceInstance>, metrics: &ScalingMetrics) -> Result<()> {
        // In a real implementation, this would trigger the creation of a new instance
        // through your container orchestration system (e.g., Kubernetes)
        info!("Scaling up: Creating new instance");
        metrics.scale_up_operations.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn scale_down(instances: &DashMap<String, ServiceInstance>, metrics: &ScalingMetrics) -> Result<()> {
        // In a real implementation, this would trigger the removal of an instance
        // through your container orchestration system
        if let Some(instance) = instances.iter().find(|i| i.status == InstanceStatus::Active) {
            info!("Scaling down: Removing instance {}", instance.id);
            metrics.scale_down_operations.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(())
    }

    pub async fn cleanup_failed_instances(&self) -> Result<()> {
        let now = Utc::now();
        let timeout = self.config.instance_timeout;

        let failed_instances: Vec<_> = self.instances
            .iter()
            .filter(|i| {
                now.signed_duration_since(i.last_heartbeat) > chrono::Duration::from_std(timeout).unwrap()
            })
            .map(|i| i.id.clone())
            .collect();

        for instance_id in failed_instances {
            if let Some(instance) = self.instances.remove(&instance_id) {
                warn!("Removing failed instance: {}", instance_id);
                self.metrics.instance_failures.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                
                // In a real implementation, this would trigger cleanup in your container orchestration system
            }
        }

        Ok(())
    }

    pub fn get_metrics(&self) -> ScalingMetrics {
        ScalingMetrics {
            scale_up_operations: AtomicU64::new(
                self.metrics.scale_up_operations.load(std::sync::atomic::Ordering::Relaxed)
            ),
            scale_down_operations: AtomicU64::new(
                self.metrics.scale_down_operations.load(std::sync::atomic::Ordering::Relaxed)
            ),
            failed_scale_operations: AtomicU64::new(
                self.metrics.failed_scale_operations.load(std::sync::atomic::Ordering::Relaxed)
            ),
            instance_failures: AtomicU64::new(
                self.metrics.instance_failures.load(std::sync::atomic::Ordering::Relaxed)
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::redis_abstraction::MockRedisClient;

    #[tokio::test]
    async fn test_instance_registration() {
        let redis_client = Arc::new(MockRedisClient::new());
        let config = ScalingConfig::default();
        let manager = ScalingManager::new(redis_client, config);

        let instance = ServiceInstance {
            id: "test-instance".to_string(),
            host: "localhost".to_string(),
            port: 8080,
            status: InstanceStatus::Active,
            metrics: InstanceMetrics {
                cpu_usage: 0.5,
                memory_usage: 0.6,
                request_count: 100,
                error_count: 0,
                latency_ms: 50.0,
            },
            shard_assignments: vec![],
            last_heartbeat: Utc::now(),
        };

        assert!(manager.register_instance(instance).await.is_ok());
        assert_eq!(manager.instances.len(), 1);
    }

    #[tokio::test]
    async fn test_metrics_update() {
        let redis_client = Arc::new(MockRedisClient::new());
        let config = ScalingConfig::default();
        let manager = ScalingManager::new(redis_client, config);

        let instance = ServiceInstance {
            id: "test-instance".to_string(),
            host: "localhost".to_string(),
            port: 8080,
            status: InstanceStatus::Active,
            metrics: InstanceMetrics {
                cpu_usage: 0.5,
                memory_usage: 0.6,
                request_count: 100,
                error_count: 0,
                latency_ms: 50.0,
            },
            shard_assignments: vec![],
            last_heartbeat: Utc::now(),
        };

        manager.register_instance(instance).await.unwrap();

        let new_metrics = InstanceMetrics {
            cpu_usage: 0.7,
            memory_usage: 0.8,
            request_count: 200,
            error_count: 1,
            latency_ms: 60.0,
        };

        assert!(manager.update_instance_metrics("test-instance", new_metrics).await.is_ok());
        
        let instance = manager.instances.get("test-instance").unwrap();
        assert_eq!(instance.metrics.cpu_usage, 0.7);
        assert_eq!(instance.metrics.memory_usage, 0.8);
    }
} 