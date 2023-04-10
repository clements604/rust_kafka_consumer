# rust_kafka_consumer

### Reference:
- https://docs.rs/kafka/latest/kafka/consumer/index.html
- https://kafka.apache.org/quickstart

### WIP Features
- SSL support

### Command line options
    -b, --bootstrap <bootstrap_servers>
            Specify bootstrap servers, overrides configuration file
    -c, --autocommit
            auto commit flag, overrides configuration file
    -f
            Flag to output to files based on topic names rather than stdout
    -g, --group <group_id>
            Specify group ID, overrides configuration file
    -h, --help
            Print help information
    -k, --key <message_key>
            Specify a key to search with
    -p, --properties <properties_file>
            Specify properties file path, overrides default in CWD [default: configuration.json]
    -t, --topics <topics>
            Specify topics to subscribe to, overrides configuration file, comma seperated

### Configuration file
-   If no -p/--properties file path is provided and no configuration.json file exists in the current working directory the consumer
    will generate a default configuration file named configuration.json in the current working directory.
-   If -p/--properties file path is provided and no configuration.json file exists in the provided path the consumer
    will generate a default configuration file named configuration.json in the provided path.

#### Sample configuration.json
    {
    "AUTOCOMMIT_FLAG": false,
    "BOOTSTRAP_SERVERS": "localhost:9092",
    "GROUP_ID": "",
    "OFFSET_RESET_FLAG": "earliest",
    "TOPICS": "quickstart-events"
    }

### Build
Cargo build --release -target-dir DIRECTORY_PATH
