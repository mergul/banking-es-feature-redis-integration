pub mod dlq_router {
    use crate::infrastructure::kafka_abstraction::KafkaProducerTrait;
    use anyhow::Result;
    use std::sync::Arc;

    pub enum DLQErrorType {
        Deserialization,
        Validation,
        Database,
        Unknown,
    }

    pub struct DLQRouter {
        producer: Arc<dyn KafkaProducerTrait>,
    }

    impl DLQRouter {
        pub fn new(producer: Arc<dyn KafkaProducerTrait>) -> Self {
            Self { producer }
        }

        pub async fn route(
            &self,
            error_type: DLQErrorType,
            message: &[u8],
            key: &str,
        ) -> Result<()> {
            let topic = match error_type {
                DLQErrorType::Deserialization => "dlq_deserialization",
                DLQErrorType::Validation => "dlq_validation",
                DLQErrorType::Database => "dlq_database",
                DLQErrorType::Unknown => "dlq_unknown",
            };

            self.producer.publish_binary_event(topic, message, key).await
        }
    }
}
