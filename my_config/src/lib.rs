use std::fs::{File, read_to_string};
use std::path::Path;
use std::io::prelude::*;
use std::str::FromStr;
use serde_json::{json, Result};
use serde::{Deserialize, Serialize};
use log::{info, warn, error, debug, trace, LevelFilter};
use std::collections::HashMap;
use std::process;
use serde_json::Value;

const DATA: &str = r#"{
    "SSL_ENABLED": "ssl.enabled=true",
    "IDENTIFICATION_ALGORITHIM": "ssl.endpoint.identification.algorithm=",
    "KEYMANAGER_ALGORITHIM": "ssl.keymanager.algorithm=SunX509",
    "KEYSTORE_TYPE": "ssl.keystore.type=JKS",
    "TRUSTMANAGER_ALGORITHIM": "ssl.trustmanager.algorithm=PKIX",
    "TRUSTSTORE_TYPE": "ssl.truststore.type=JKS",
    "SSL_ENABLED_PROTOCOLS": "ssl.enabled.protocols=TLSv1.2,TLSv1.1,TLSv1",
    "SECURITY_PROTOCOL": "security.protocol=SSL",
    "KEYSTORE_LOCATION": "ssl.keystore.location=kafka.keystore.jks",
    "KEYSTORE_PASSWORD": "ssl.keystore.password=kafka@1234",
    "TRUSTSTORE_LOCATION": "ssl.truststore.location=kafka.truststore.jks",
    "TRUSTSTORE_PASSWORD": "ssl.truststore.password=kafka@1234",
    "PRIVATE_KEY_PASSWORD": "ssl.key.password=kafka@1234",
    "BOOTSTRAP_SERVERS": "bootstrap.servers=SSL://bootstrap_server:9092",
    "AUTOCOMMIT_FLAG": "enable.auto.commit=false",
    "GROUP_ID": "group.id=",
    "OFFSET_RESET_FLAG": "auto.offset.reset=earliest",
    "TOPICS": "topics=Example_Topic_One,Example_Topic_Two",
    "BROKER_TIMEOUT": "default.api.timeout.ms=10000",
    "MAX_POLL_RECORDS": "max.poll.records=100"
}"#;

pub fn load_cfg() -> HashMap<String, String>{
    debug!("load_cfg start");
    let path = Path::new("configuration.json");
    let display = path.display();
    let mut file = match File::open(&path) {
        Err(why) => {
            error!("Error loading configuration file: {:?}", &why.to_string());
            println!("{:?}", create_default_cfg());
            process::abort()
        },
        Ok(cfg_file) => {
            debug!("load_cfg finish");
            let cfg_map = load_cfg_from_file(cfg_file).ok();
            return cfg_map.unwrap();
        },
    };
}

fn create_default_cfg() -> std::io::Result<()> {
    debug!("create_default_cfg start");
    // Serialize the data as a JSON string
    //println!("{:?}", DATA);
    let data: serde_json::Value = serde_json::from_str(DATA)?;

    // Open a file for writing
    let mut file = File::create("configuration.json")?;

    // Write the JSON string to the file
    file.write_all(data.to_string().as_bytes())?;
    debug!("create_default_cfg finish");
    Ok(())
}

fn load_cfg_from_file(mut file: File) -> std::io::Result<HashMap<String, String>> {
    debug!("load_cfg_from_file start");
    let mut cfg_str: String = String::from("");
    file.read_to_string(&mut cfg_str)?;
    let cfg_json: Value = serde_json::from_str(&cfg_str)?;
    //debug!("{:?}", cfg_json);

    let mut cfg_map: HashMap<String, String> = HashMap::new();
    if let Some(temp) = cfg_json.as_object() {
        for(key, value) in temp.iter(){
            debug!("key [{:?}], value [{:?}]", key, value);
            cfg_map.insert(String::from(key), String::from(value.as_str().unwrap_or("")));
        }
    }
    debug!("load_cfg_from_file finish");
    Ok(cfg_map)
}
