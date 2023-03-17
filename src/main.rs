use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use log::{info, warn, error, debug, trace, LevelFilter};
use env_logger::Builder;
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::fs::metadata;
use std::fs::OpenOptions;

mod lib;
mod utils;

fn main() {

    Builder::new()
        .filter_level(LevelFilter::Debug)
        .init();

    let cfg_map = lib::load_cfg();

    debug!("{:?}", cfg_map["BOOTSTRAP_SERVERS"]);

    let mut consumer = match
    Consumer::from_hosts(vec!(cfg_map["BOOTSTRAP_SERVERS"].to_owned()))
    .with_topic_partitions(cfg_map["TOPICS"].to_owned(), &[0, 1])
    .with_topic(cfg_map["TOPICS"].to_owned())
        .with_fallback_offset(FetchOffset::Earliest)
        .with_group(cfg_map["GROUP_ID"].to_owned())
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create() {
            Ok(consumer) => {
                consumer
            },
            Err(why) => {
                error!("{}", why.to_string());
                panic!("{}", why);
            }
        };

    

    debug!("Consumer created");

    loop {
        for message_set in consumer.poll().unwrap().iter() {
            for message in message_set.messages() {
                let mut message: String = String::from(std::str::from_utf8(&message.value).unwrap());
                info!("{:?}", message);
                message = utils::get_epoch_time() + "\t\t" + &message + "\n";
                info!("{}", utils::get_epoch_time());
                write_message_to_file(cfg_map["TOPICS"].to_owned(), message);
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
        //.write(true)
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

fn get_epoch_time() -> String {
    return String::from("");
}
