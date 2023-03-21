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

fn main() {

    Builder::new()
        .filter_level(LevelFilter::Debug)
        .init();

    let cfg_map = lib::load_cfg();

    debug!("{:?}", cfg_map["BOOTSTRAP_SERVERS"]);

    let mut consumer = get_consumer(&cfg_map);
    debug!("Consumer created");

    loop {
        for message_set in consumer.poll().unwrap().iter() {
            let topic = message_set.topic();
            for message in message_set.messages() {
                let mut message: String = String::from(std::str::from_utf8(&message.value).unwrap());
                info!("{:?}", message);
                let timestamp = utils::get_timestamp();
                message = timestamp.to_owned() + "\t\t" + &message + "\n";
                write_message_to_file(topic.to_owned(), message);
            }
            match consumer.consume_messageset(message_set) {
                Ok(result) => result,
                Err(why) => error!("{}", why)
            };
        }
        consumer.commit_consumed().unwrap();
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
        info!("{}", topic);
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
