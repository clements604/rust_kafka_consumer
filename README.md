# rust_kafka_consumer

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
      "GROUP_ID": "josh",
      "OFFSET": "earliest",
      "TOPICS": "quickstart-events",
      "MESSAGE_KEY": "",
      "FILE_OUTPUT": false,
      "SSL_ENABLED": false,
      "SSL_SECURITY_PROTOCOL": "ssl",
      "SSL_CA_LOCATION": "",
      "SSL_CERT_LOCATION": "",
      "SSL_KEY_LOCATION": "",
      "SSL_KEY_PASSWD": ""
    }

### Dependencies
-   cargo -- https://doc.rust-lang.org/cargo/getting-started/installation.html
-   libssl-dev
-   build-essential

### Build
    cargo build --release --target-dir DIRECTORY_PATH
