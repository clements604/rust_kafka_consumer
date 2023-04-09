use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use log::{info, error, debug, LevelFilter};
use env_logger::Builder;
use std::io::prelude::*;
use std::fs::OpenOptions;
use serde_json::Value;

mod lib;
mod utils;

mod args;
use clap::{App, Arg};

fn main() {

    let mut commit_consumed: bool = false;
    let mut message_key: String = String::from("");

    Builder::new()
        .filter_level(LevelFilter::Debug)
        .init();

    let matches = App::new("test")
        .arg(
            Arg::with_name("file_output")
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
            .default_value(lib::DEFAULT_PATH_STR)
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

    let file_output: bool = matches.is_present("file_output");
    debug!("file_output [{}]", file_output);

    let mut cfg_map: serde_json::Value = match matches.value_of("properties_file"){
        Some(path) => {
            debug!("Custom path [{}] provided for configuration file", path);
            lib::load_cfg(Some(String::from(path)))
        },
        None => {
            debug!("using default path of ./configuration.properties");
            lib::load_cfg(None)
        }
    };

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

    if matches.is_present("message_key") {
        message_key = String::from(matches.value_of("message_key").unwrap_or(""));
    }

    if matches.is_present("group_id") {
        if let Some(group_id) = matches.value_of("group_id"){
            cfg_map["GROUP_ID"] = Value::from(group_id);
        }
    }

    let mut consumer = get_consumer(&cfg_map);

    debug!("Consumer created");

    loop {
        for message_set in consumer.poll().unwrap().iter() {
            let topic = message_set.topic();
            for message in message_set.messages() {
                if std::str::from_utf8(&message.key).unwrap() == &message_key || &message_key == "" {
                    let mut message: String = String::from(std::str::from_utf8(&message.value).unwrap());
                    let timestamp = utils::get_timestamp();
                    message = timestamp.to_owned() + "\t\t" + &message;
                    if file_output {
                        message = message + "\n";
                        write_message_to_file(topic.to_owned(), message);
                    }
                    else {
                        info!("{}", message);
                    }
                }
            }
            match consumer.consume_messageset(message_set) {
                Ok(result) => result,
                Err(why) => error!("{}", why)
            };
        }
        if commit_consumed && cfg_map["GROUP_ID"].as_str().unwrap_or("") != "" {
            consumer.commit_consumed().unwrap();
        }
    }
    
}

fn write_message_to_file(topic: String, message: String) {
    match OpenOptions::new()
        .append(true)
        .create(true)
        .open(format!("{}.log", topic)) {
            Ok(mut topic_log) => {
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

fn get_consumer(cfg_map: &serde_json::Value) -> Consumer{

    let bootstrap_servers: Vec<String> = vec!(cfg_map["BOOTSTRAP_SERVERS"].as_str().unwrap_or("").split(",").collect());

    let mut consumer = Consumer::from_hosts(bootstrap_servers)
    .with_group(cfg_map["GROUP_ID"].to_string().to_owned())
    .with_offset_storage(GroupOffsetStorage::Kafka);

    match cfg_map["OFFSET_RESET_FLAG"].as_str().unwrap_or("") {
        "earliest" => consumer = consumer.with_fallback_offset(FetchOffset::Earliest),
        "latest" => consumer = consumer.with_fallback_offset(FetchOffset::Latest),
        _ => consumer = consumer.with_fallback_offset(FetchOffset::Earliest),
    };

    let topics: Vec<&str> = cfg_map["TOPICS"].as_str().unwrap_or("").split(",").collect();
    debug!("{:?}", topics);

    for topic in topics {
        debug!("{}", topic);
        consumer = consumer.with_topic(topic.to_owned())
    }

    match consumer.create() {
        Ok(consumer) => consumer,
        Err(error) => {
            error!("{}", error);
            panic!("{}", error)
        }
    }

}
