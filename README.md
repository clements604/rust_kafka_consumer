# rust_kafka_consumer

### Reference:
- https://docs.rs/kafka/latest/kafka/consumer/index.html
- https://kafka.apache.org/quickstart
- https://www.youtube.com/watch?v=fD9ptABVQbI

### WIP Features
- Command line arguments, updating to include file path param
- with_topic_partitions currently hardcoded to 0,1

### *PLANNED* Command line options
- File output flag (topic as filename)
- Properties file name override
- Message key override
- Topics to subscribe to (comma seperated)
- Group ID to use
- Bootstrap server override
- Autocommit flag override