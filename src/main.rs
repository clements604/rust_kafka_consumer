use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use log::{info, warn, error, debug, trace, LevelFilter};
use env_logger::Builder;
use my_config::{add, load_cfg};

fn main() {

    Builder::new()
        .filter_level(LevelFilter::Debug)
        .init();

        debug!("{}", my_config::add(1, 2));
        my_config::load_cfg();

    let mut consumer =
    Consumer::from_hosts(vec!("localhost:9092".to_owned()))
    .with_topic_partitions("quickstart-events".to_owned(), &[0, 1])
    .with_topic("quickstart-events".to_owned())
        .with_fallback_offset(FetchOffset::Earliest)
        //.with_group("my-group".to_owned())
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()
        .unwrap();
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
