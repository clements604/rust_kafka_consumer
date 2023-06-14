use env_logger::Builder;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::Consumer as RdConsumer;
use rdkafka::message::Message;
use serde_json::Value;
use std::fs::OpenOptions;
use std::io::Write;
use std::time::Duration;
use rdkafka::ClientContext;
use rdkafka::topic_partition_list::TopicPartitionList;
use clap::{App, Arg};
use log::{debug, error, info, LevelFilter};
use rdkafka::error::{KafkaResult, KafkaError};
use rdkafka::consumer::{BaseConsumer, CommitMode, ConsumerContext, Rebalance};

mod config_mgr;
mod constants;
mod utils;

struct CustomContext;

#[tokio::main]
async fn main() {
    let commit_consumed: bool;

    Builder::new().filter_level(LevelFilter::Info).init();

    let matches = App::new("kafka_consumer")
        .arg(
            Arg::with_name(constants::ARG_FILE_OUTPUT)
                .short('f')
                .takes_value(false)
                .max_occurrences(1)
                .help("Flag to output to files based on topic names rather than stdout"),
        )
        .arg(
            Arg::with_name(constants::ARG_AUTOCOMMIT)
                .short('c')
                .long("autocommit")
                .takes_value(false)
                .max_occurrences(1)
                .help("auto commit flag, overrides configuration file"),
        )
        .arg(
            Arg::with_name(constants::ARG_PROPERTIES)
                .short('p')
                .long("properties")
                .takes_value(true)
                .forbid_empty_values(true)
                .max_occurrences(1)
                .default_value(config_mgr::DEFAULT_PATH_STR)
                .help("Specify properties file path, overrides default in CWD"),
        )
        .arg(
            Arg::with_name(constants::ARG_MESSAGE_KEY)
                .short('k')
                .long("key")
                .takes_value(true)
                .forbid_empty_values(true)
                .max_occurrences(1)
                .help("Specify a key to search with"),
        )
        .arg(
            Arg::with_name(constants::ARG_TOPICS)
                .short('t')
                .long(constants::ARG_TOPICS)
                .takes_value(true)
                .forbid_empty_values(true)
                .max_occurrences(1)
                .help(
                    "Specify topics to subscribe to, overrides configuration file, comma seperated",
                ),
        )
        .arg(
            Arg::with_name(constants::ARG_GROUPS)
                .short('g')
                .long("group")
                .takes_value(true)
                .forbid_empty_values(true)
                .max_occurrences(1)
                .help("Specify group ID, overrides configuration file"),
        )
        .arg(
            Arg::with_name(constants::ARG_BOOTSTRAPS)
                .short('b')
                .long("bootstrap")
                .takes_value(true)
                .max_occurrences(1)
                .forbid_empty_values(true)
                .help("Specify bootstrap servers, overrides configuration file"),
        )
        .get_matches();

    let mut cfg_map: serde_json::Value = match matches.value_of(constants::ARG_PROPERTIES) {
        Some(path) => {
            debug!("Custom path [{}] provided for configuration file", path);
            config_mgr::load_cfg(Some(String::from(path)))
        }
        None => {
            debug!("using default path of ./configuration.properties");
            config_mgr::load_cfg(None)
        }
    };

    if matches.is_present(constants::ARG_FILE_OUTPUT) {
        cfg_map[constants::CFG_FILE_OUTPUT] = serde_json::Value::Bool(true);
        debug!("file_output enabled");
    } else {
        debug!("{}", cfg_map[constants::CFG_FILE_OUTPUT]);
    }

    if matches.is_present(constants::ARG_BOOTSTRAPS) {
        if let Some(bootstrap_servers) = matches.value_of(constants::ARG_BOOTSTRAPS) {
            cfg_map[constants::CFG_BOOTSTRAPS] = Value::from(bootstrap_servers);
        }
    }

    if matches.is_present(constants::ARG_AUTOCOMMIT) {
        commit_consumed = true;
    } else {
        commit_consumed = cfg_map[constants::CFG_AUTOCOMMIT]
            .as_bool()
            .unwrap_or(false);
    }
    cfg_map[constants::CFG_AUTOCOMMIT] = serde_json::Value::Bool(commit_consumed);

    if matches.is_present(constants::ARG_MESSAGE_KEY) {
        let message_key = String::from(matches.value_of(constants::ARG_MESSAGE_KEY).unwrap_or(""));
        cfg_map[constants::CFG_MESSAGE_KEY] = serde_json::Value::String(message_key);
    }

    if matches.is_present(constants::ARG_GROUPS) {
        if let Some(group_id) = matches.value_of(constants::ARG_GROUPS) {
            cfg_map[constants::CFG_GROUP] = Value::from(group_id);
        }
    }

    if matches.is_present(constants::ARG_TOPICS) {
        cfg_map[constants::CFG_TOPICS] = serde_json::Value::String(String::from(
            matches.value_of(constants::ARG_TOPICS).unwrap_or(""),
        ));
    }

    debug!("{}", cfg_map);

    let consumer: BaseConsumer<CustomContext> = get_rd_consumer(&cfg_map);

    poll(&consumer, &cfg_map).await;
}

async fn poll(consumer: &BaseConsumer<CustomContext>, cfg_map: &serde_json::Value) {
    loop {
        match consumer.poll(Duration::from_secs(constants::POLL_DUR_SECS)) {
            Some(Ok(message)) => {
                let message_key: String =
                    String::from(std::str::from_utf8(message.key().unwrap_or(b"")).unwrap_or(""));
                if cfg_map[constants::CFG_MESSAGE_KEY].to_owned() == "" || message_key == cfg_map[constants::CFG_MESSAGE_KEY].to_string().to_owned() {
                    match message.payload() {
                        Some(content) => {
                            let content = std::str::from_utf8(content).unwrap();
                            info!("{}", content);
                            if cfg_map[constants::CFG_FILE_OUTPUT]
                                .as_bool()
                                .unwrap_or(false)
                            {
                                let timestamp = utils::get_timestamp();
                                let timestamp_msg =
                                    timestamp.as_str().to_owned() + "\t\t" + content;
                                write_message_to_file(String::from(message.topic()), timestamp_msg);
                            }
                        }
                        None => {}
                    }
                }
                if cfg_map[constants::CFG_AUTOCOMMIT]
                    .as_bool()
                    .unwrap_or(false)
                {
                    match consumer.commit_message(&message, CommitMode::Async) {
                        Ok(_) => {
                            debug!("Message commit complete")
                        }
                        Err(why) => {
                            error!("{}", why);
                            break
                        }
                    }
                }
            }
            Some(Err(why)) => {
                error!("{}", why)
            },
            None => {}
        }
    }
}

fn write_message_to_file(topic: String, mut message: String) {
    match OpenOptions::new()
        .append(true)
        .create(true)
        .open(format!("{}.log", topic))
    {
        Ok(mut topic_log) => {
            message = message + "\n";
            match topic_log.write_all(message.as_bytes()) {
                Ok(result) => result,
                Err(why) => error!("{}", why),
            };
        }
        Err(why) => {
            error!("{}", why);
        }
    };
}

fn get_rd_consumer(cfg_map: &serde_json::Value) -> BaseConsumer<CustomContext> {
    let mut consumer_config = ClientConfig::new();
    let custom_context = CustomContext;
    consumer_config.set(
        "bootstrap.servers",
        cfg_map[constants::CFG_BOOTSTRAPS]
            .as_str()
            .unwrap_or(constants::DEFAULT_BOOTSTRAPS),
    );
    consumer_config.set(
        "group.id",
        &cfg_map[constants::CFG_GROUP].as_str().unwrap_or(""),
    );
    consumer_config.set(
        "enable.auto.commit",
        &cfg_map[constants::CFG_AUTOCOMMIT]
            .as_str()
            .unwrap_or(constants::DEFAULT_AUTOCOMMIT),
    );
    consumer_config.set(
        "auto.offset.reset",
        &cfg_map[constants::CFG_OFFSET]
            .as_str()
            .unwrap_or(constants::DEFAULT_OFFSET_TYPE),
    );
    consumer_config.set("session.timeout.ms", constants::CONNECTION_TIMEOUT_MS);

    let consumer: BaseConsumer<CustomContext> = consumer_config
        .create_with_context(custom_context)
        .expect("Failed to create Kafka consumer");

    let topics: Vec<&str> = cfg_map[constants::CFG_TOPICS]
        .as_str()
        .unwrap_or("")
        .split(",")
        .collect();
    consumer
        .subscribe(&topics)
        .expect("Could not subscribe to topics");

    consumer
}

impl ClientContext for CustomContext {
    fn error(&self, error: KafkaError, reason: &str) {
        error!("{}", error);
        error!("{}", reason);
        std::process::exit(1)
    }
}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        debug!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        debug!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        debug!("Committing offsets: {:?}", result);
    }
}
