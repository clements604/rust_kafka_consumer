use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use log::{info, warn, error, debug, trace, LevelFilter};
use env_logger::Builder;
use std::collections::HashMap;

mod lib;

fn main() {

    let mut cfg_map: HashMap<String, String> = HashMap::new();

    Builder::new()
        .filter_level(LevelFilter::Info)
        .init();

        cfg_map = lib::load_cfg();

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

    info!("Consumer created");
    loop {
        for ms in consumer.poll().unwrap().iter() {
        for m in ms.messages() {
                info!("{:?}", std::str::from_utf8(&m.value).unwrap());
            }
            consumer.consume_messageset(ms);
        }
        consumer.commit_consumed().unwrap();
    }

}

