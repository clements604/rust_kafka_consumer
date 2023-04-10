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