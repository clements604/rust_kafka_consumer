use futures::TryStreamExt;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use log::{info, error, debug, LevelFilter};
use env_logger::Builder;
use std::io::prelude::*;
use std::fs::OpenOptions;
use serde_json::Value;

//use kafka::client::{KafkaClient, SecurityConfig};
use kafka::client::{KafkaClient,SecurityConfig};
use openssl;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::util::get_rdkafka_version;
use rdkafka::consumer::Consumer as RdConsumer;
use rdkafka::consumer::CommitMode;
use rdkafka::consumer::DefaultConsumerContext;
use std::time::Duration;

use futures::stream::StreamExt;

use rdkafka::client::ClientContext;
use rdkafka::config::{RDKafkaLogLevel};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Headers};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::consumer::MessageStream;

use tokio::time::sleep;
use tokio::runtime::Runtime;

use rdkafka::consumer::BaseConsumer;

mod config_mgr;
mod utils;
mod ssl_helper;

use clap::{App, Arg};

#[tokio::main]
async fn main() {

    let commit_consumed: bool;
    let mut message_key: String = String::from("");

    Builder::new()
        .filter_level(LevelFilter::Info)
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

    let file_output: bool = matches.is_present("file_output");
    debug!("file_output [{}]", file_output);

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

    //let consumer: StreamConsumer<CustomContext> = get_rd_consumer(&cfg_map).await;

    let mut consumer_config = ClientConfig::new();
    consumer_config.set("bootstrap.servers", "localhost:9092");


    let consumer: BaseConsumer = consumer_config
        .set("group.id", &cfg_map["GROUP_ID"].to_string().to_owned())
        .set("enable.auto.commit", "false")
        .set("session.timeout.ms", "6000")
        .create()
        .expect("Failed to create Kafka consumer");

    let topics: Vec<&str> = cfg_map["TOPICS"].as_str().unwrap_or("").split(",").collect();
    consumer.subscribe(&topics).unwrap();

    poll(&consumer).await;

    
}

async fn poll(consumer: &BaseConsumer) {
    //let mut consumer = get_consumer(&cfg_map);
    //let mut consumer: StreamConsumer<CustomContext> = get_rd_consumer(&cfg_map).await();
    
    
    

    //let mut stream: MessageStream<'_, CustomContext> = consumer.start();

    debug!("Consumer created");

    loop {
        debug!("loop");
        match consumer.poll(Duration::from_secs(1)) {
            Some(message) => {
                for message in message.iter() {
                    match message.payload() {
                    Some(content) => {
                        let content = std::str::from_utf8(content).unwrap();
                        info!("{:?}", content)
                    },
                    None => {
                        debug!("nothing 2")
                    }
                }
                }
            },
            None => {
                debug!("nothing 1")
            }
        }
    }

    /*loop {
        match stream.next().await {
            Some(message) => {
                println!("Received message: {:?}", &message.unwrap().payload_view::<str>().unwrap());
                //consumer.commit_message(&message, CommitMode::Async).unwrap();
            },
            None => {
                println!("No message");
            }
            /*Some(Ok(message)) => {
                println!("Received message: {:?}", message.payload_view::<str>().unwrap());
            },
            Some(Err(e)) => {
                eprintln!("Error while receiving message: {:?}", e);
            },
            None => {
                // No message received within the timeout interval.
            }*/
        }
        
    }*/

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

struct CustomContext;
impl rdkafka::client::ClientContext for CustomContext {}
impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        println!("Pre rebalance: {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        println!("Post rebalance: {:?}", rebalance);
    }
}

async fn get_rd_consumer(cfg_map: &serde_json::Value) -> StreamConsumer<CustomContext> {

    let context = CustomContext;

    let bootstrap_servers: String = String::from("");
    let topics: Vec<&str> = cfg_map["TOPICS"].as_str().unwrap_or("").split(",").collect();

    let consumer: StreamConsumer<CustomContext> = ClientConfig::new()
        .set("group.id", &cfg_map["GROUP_ID"].to_string().to_owned())
        .set("bootstrap.servers", &bootstrap_servers)
        .set("enable.auto.commit", "false")
        .set("session.timeout.ms", "6000")
        //.set("topics", "quickstart-events")
        .create_with_context(CustomContext)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&["my-topic"])
        .expect("Could not subscribe to topics");
   
    consumer
}