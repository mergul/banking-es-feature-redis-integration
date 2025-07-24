use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::config::ClientConfig;

#[tokio::main]
async fn main() {
    let admin_client: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()
        .expect("Failed to create admin client");

    let topics = vec![
        NewTopic::new("banking-es-events", 1, TopicReplication::Fixed(1)),
        NewTopic::new("banking-es-cache", 1, TopicReplication::Fixed(1)),
        NewTopic::new(
            "banking-es-cache-invalidation",
            1,
            TopicReplication::Fixed(1),
        ),
        NewTopic::new("banking-es-dlq", 1, TopicReplication::Fixed(1)),
        NewTopic::new(
            "banking-es.public.kafka_outbox_cdc",
            3,
            TopicReplication::Fixed(1),
        ), // CDC topic
    ];

    let results = admin_client
        .create_topics(&topics, &AdminOptions::new())
        .await
        .expect("Failed to create topics");

    for result in results {
        match result {
            Ok(topic) => println!("Created topic: {}", topic),
            Err((topic, e)) => println!("Failed to create topic {}: {}", topic, e),
        }
    }
}
