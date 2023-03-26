use kafka::consumer;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use log::{info, warn, error, debug, trace, LevelFilter};
use env_logger::Builder;
use std::collections::HashMap;
use std::fs::File;
use std::hash::Hash;
use std::io::prelude::*;
use std::fs::metadata;
use std::fs::OpenOptions;
use kafka::error::{Error};

mod lib;
mod utils;

mod args;
use args::AppArgs;
use clap::{Parser, App, Arg};

fn main() {

    //let args = AppArgs::parse();
    let mut commit_consumed: bool = false;

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
            .default_value(lib::PATH_STR)
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
    
    /*let cfg_map = lib::load_cfg(
        match matches.value_of("properties_file"){
            Some(val) => val,
            None => "abc123"
        }
    );*/

    let file_output: bool = matches.is_present("file_output");
    debug!("file_output [{}]", file_output);

    let mut cfg_map: HashMap<String, String> = match matches.value_of("properties_file"){
        Some(path) => {
            debug!("Custom path [{}] provided for configuration file", path);
            lib::load_cfg()//TODO make path dynamic,
            //lib::load_cfg(path)//TODO make path dynamic,
        },
        None => {
            debug!("using default path of ./configuration.properties");
            lib::load_cfg()
        }
    };

    if matches.is_present("bootstrap_servers") {
        if let Some(bootstrap_servers) = matches.value_of("bootstrap_servers"){
            cfg_map.insert(String::from("BOOTSTRAP_SERVERS"), String::from(bootstrap_servers));
        }     
    }

    if matches.is_present("auto_commt") {
        commit_consumed = true;
    }

    let mut consumer = get_consumer(&cfg_map);
    debug!("Consumer created");

    loop {
        for message_set in consumer.poll().unwrap().iter() {
            let topic = message_set.topic();
            for message in message_set.messages() {
                let mut message: String = String::from(std::str::from_utf8(&message.value).unwrap());
                //debug!("{:?}", message);
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
            match consumer.consume_messageset(message_set) {
                Ok(result) => result,
                Err(why) => error!("{}", why)
            };
        }
        if commit_consumed {
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

fn get_consumer(cfg_map: &HashMap<String, String>) -> Consumer{

    let mut consumer = Consumer::from_hosts(vec!(cfg_map["BOOTSTRAP_SERVERS"].to_owned()))
    //.with_topic_partitions(cfg_map["TOPICS"].to_owned(), &[0, 1])
    .with_fallback_offset(FetchOffset::Earliest)
    .with_group(cfg_map["GROUP_ID"].to_owned())
    .with_offset_storage(GroupOffsetStorage::Kafka);

    let topics: Vec<&str> = cfg_map["TOPICS"].split(",").collect();
    debug!("{:?}", topics);

    for topic in topics {
        debug!("{}", topic);
        consumer = consumer.with_topic(topic.to_owned())
        //.with_topic_partitions(topic.to_owned(), &[0, 1]);
    }

    match consumer.create() {
        Ok(consumer) => consumer,
        Err(error) => {
            error!("{}", error);
            panic!("{}", error)
        }
    }

}
