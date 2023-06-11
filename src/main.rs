use env_logger::Builder;
use std::io::prelude::*;
use std::fs::OpenOptions;
use serde_json::Value;
use rdkafka::config::ClientConfig;
use rdkafka::message::Message;
use rdkafka::consumer::Consumer as RdConsumer;
use rdkafka::consumer::CommitMode;
use std::time::Duration;
use rdkafka::consumer::BaseConsumer;
use log::{info, error, debug, LevelFilter};
use clap::{App, Arg};

mod config_mgr;
mod utils;
mod constants;

#[tokio::main]
async fn main() {

    let commit_consumed: bool;

    Builder::new()
        .filter_level(LevelFilter::Debug)
        .init();

    let matches = App::new("test")
        .arg(
            Arg::with_name(constants::CFG_FILE_OUTPUT)
            .short('f')
            .takes_value(false)
            .max_occurrences(1)
            .help("Flag to output to files based on topic names rather than stdout")
        )
        .arg(
            Arg::with_name("auto_commt")
            .short('c')
            .long("autocommit")
            .takes_value(false)
            .max_occurrences(1)
            .help("auto commit flag, overrides configuration file")
        )
        .arg(
            Arg::with_name("properties_file")
            .short('p')
            .long("properties")
            .takes_value(true)
            .forbid_empty_values(true)
            .max_occurrences(1)
            .default_value(config_mgr::DEFAULT_PATH_STR)
            .help("Specify properties file path, overrides default in CWD")
        )
        .arg(
            Arg::with_name("message_key")
            .short('k')
            .long("key")
            .takes_value(true)
            .forbid_empty_values(true)
            .max_occurrences(1)
            .help("Specify a key to search with")
        )
        .arg(
            Arg::with_name("topics")
            .short('t')
            .long("topics")
            .takes_value(true)
            .forbid_empty_values(true)
            .max_occurrences(1)
            .help("Specify topics to subscribe to, overrides configuration file, comma seperated")
        )
        .arg(
            Arg::with_name("group_id")
            .short('g')
            .long("group")
            .takes_value(true)
            .forbid_empty_values(true)
            .max_occurrences(1)
            .help("Specify group ID, overrides configuration file")
        )
        .arg(
            Arg::with_name("bootstrap_servers")
            .short('b')
            .long("bootstrap")
            .takes_value(true)
            .max_occurrences(1)
            .forbid_empty_values(true)
            .help("Specify bootstrap servers, overrides configuration file")
        )
        .get_matches();

    let mut cfg_map: serde_json::Value = match matches.value_of("properties_file"){
        Some(path) => {
            debug!("Custom path [{}] provided for configuration file", path);
            config_mgr::load_cfg(Some(String::from(path)))
        },
        None => {
            debug!("using default path of ./configuration.properties");
            config_mgr::load_cfg(None)
        }
    };

    if matches.is_present(constants::CFG_FILE_OUTPUT) {
        cfg_map[constants::CFG_FILE_OUTPUT] = serde_json::Value::Bool(true);
        debug!("file_output [true]");
    }
    else {
        debug!("{}", cfg_map[constants::CFG_FILE_OUTPUT]);
    }

    if matches.is_present("bootstrap_servers") {
        if let Some(bootstrap_servers) = matches.value_of("bootstrap_servers"){
            cfg_map["BOOTSTRAP_SERVERS"] = Value::from(bootstrap_servers);
        }     
    }

    if matches.is_present("auto_commt") {
        commit_consumed = true;
    }
    else {
        commit_consumed = cfg_map["AUTOCOMMIT_FLAG"].as_bool().unwrap_or(false);
    }
    cfg_map["AUTOCOMMIT_FLAG"] = serde_json::Value::Bool(commit_consumed);
    
    if matches.is_present("message_key") {
        let message_key = String::from(matches.value_of("message_key").unwrap_or(""));
        cfg_map["MESSAGE_KEY"] = serde_json::Value::String(message_key);
    }

    if matches.is_present("group_id") {
        if let Some(group_id) = matches.value_of("group_id"){
            cfg_map["GROUP_ID"] = Value::from(group_id);
        }
    }

    if matches.is_present("topics") {
        cfg_map["TOPICS"] = serde_json::Value::String(String::from(matches.value_of("topics").unwrap_or("")));
    }

    debug!("{}", cfg_map);

    let consumer: BaseConsumer = get_rd_consumer(&cfg_map).await;

    poll(&consumer, &cfg_map).await;

}

async fn poll(consumer: &BaseConsumer, cfg_map: &serde_json::Value) {

    loop {
        match consumer.poll(Duration::from_secs(1)) {
            Some(message) => {
                for message in message.iter() {
                    let message_key: String = String::from(std::str::from_utf8(message.key().unwrap_or(b"")).unwrap_or(""));
                    if cfg_map["MESSAGE_KEY"].to_owned() == "" || message_key == cfg_map["MESSAGE_KEY"].to_string().to_owned() {
                        match message.payload() {
                            Some(content) => {
                                let content = std::str::from_utf8(content).unwrap();
                                info!("{}", content);
                                if cfg_map[constants::CFG_FILE_OUTPUT].as_bool().unwrap_or(false) {
                                    let timestamp = utils::get_timestamp();
                                    let timestamp_msg = timestamp.as_str().to_owned() + "\t\t" + content;
                                    write_message_to_file(String::from(message.topic()), timestamp_msg);
                                }
                            },
                            None => {
                                debug!("nothing 2")
                            }
                        }
                    }
                    if cfg_map["AUTOCOMMIT_FLAG"].as_bool().unwrap_or(false) {
                        match consumer.commit_message(message, CommitMode::Async) {
                            Ok(_) => {
                                debug!("Message commit complete")
                            },
                            Err(why) => {
                                error!("{}", why)
                            }
                        }
                    }
                }
            },
            None => {}
        }
    }
}

fn write_message_to_file(topic: String, mut message: String) {
    match OpenOptions::new()
        .append(true)
        .create(true)
        .open(format!("{}.log", topic)) {
            Ok(mut topic_log) => {
                message = message + "\n";
                match topic_log.write_all(message.as_bytes()) {
                    Ok(result) => result,
                    Err(why) => error!("{}", why),
                };
            },
            Err(why) => {
                error!("{}", why);
            },
        };
}

async fn get_rd_consumer(cfg_map: &serde_json::Value) -> BaseConsumer {

    let mut consumer_config = ClientConfig::new();
    consumer_config.set("bootstrap.servers", cfg_map["BOOTSTRAP_SERVERS"].as_str().unwrap_or(""));
    consumer_config.set("group.id", &cfg_map["GROUP_ID"].to_string().to_owned());
    consumer_config.set("enable.auto.commit", &cfg_map["AUTOCOMMIT_FLAG"].to_string().to_owned());
    consumer_config.set("session.timeout.ms", "6000");

    let consumer: BaseConsumer = consumer_config
        .create()
        .expect("Failed to create Kafka consumer");

    let topics: Vec<&str> = cfg_map["TOPICS"].as_str().unwrap_or("").split(",").collect();
    consumer.subscribe(&topics).expect("Could not subscribe to topics");

    consumer
}