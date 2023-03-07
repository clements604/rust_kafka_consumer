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
use std::fs::metadata;

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
    "BOOTSTRAP_SERVERS": "bootstrap.servers=bootstrap_server:9092",
    "AUTOCOMMIT_FLAG": "enable.auto.commit=false",
    "GROUP_ID": "",
    "OFFSET_RESET_FLAG": "auto.offset.reset=earliest",
    "TOPICS": "quickstart-events",
    "BROKER_TIMEOUT": "default.api.timeout.ms=10000",
    "MAX_POLL_RECORDS": "max.poll.records=100"
}"#;

const PATH_STR: &str = "configuration.json";

fn get_config_path() -> &'static Path {
    Path::new(PATH_STR)
}

pub fn load_cfg() -> HashMap<String, String>{
    debug!("load_cfg start");
    match check_cfg_file(get_config_path()) {
        Err(why) => {
            error!("Error loading configuration file: {:?}", &why.to_string());
            println!("{:?}", create_default_cfg());
            process::abort()
        },
        Ok(cfg_file) => {
            debug!("load_cfg finish");
            let cfg_map = load_cfg_from_file(get_config_path());
            debug!("load_cfg finish");
            return cfg_map;
        },
    };
}

fn check_cfg_file(PATH: &Path) -> Result<String>  {
    debug!("check_cfg_file start");
    // Check if configuration file is in the same directory as the .jar file, if not create the file
    match metadata(PATH) {
        Ok(..) => {
            debug!("Configuration file exists, using existing file");
            debug!("check_cfg_file finish");
            Ok(String::from(""))
        },
        Err(..) => {
            info!("Configuration file does NOT exist, creating default.");
            create_default_cfg();
            debug!("check_cfg_file finish");
            Ok(String::from("Configuration file does NOT exist, creating default."))
        }
    }
}

fn create_default_cfg() {
    debug!("create_default_cfg start");
    // Serialize the data as a JSON string
    //println!("{:?}", DATA);
    let data: serde_json::Value = serde_json::from_str(DATA).unwrap();

    // Open a file for writing
    let mut file = match File::create("configuration.json") {
        Ok(file) => file,
        Err(why) => panic!("couldn't create {}: {}", get_config_path().display(), why),
    };

    // Write the JSON string to the file
    file.write_all(data.to_string().as_bytes());
    debug!("create_default_cfg finish");
}

fn load_cfg_from_file(path: &Path) -> HashMap<String, String>{
    debug!("load_cfg_from_file start");
    let file_contents: String = match read_to_string(get_config_path()) {
        Ok(contents) => {
            debug!("{}", contents);
            contents
        },
        Err(why) => {
            error!("{}", why);
            String::from("")
        },
    };
    let json_result: Result<Value, > = serde_json::from_str(&file_contents);
    let mut cfg_json: Value = match json_result{
        Ok(json_val) => json_val,
        Err(why) => {
            error!("{}", why);
            serde_json::from_str("{}").unwrap()
        }
    };
    let mut cfg_map: HashMap<String, String> = HashMap::new();
    if let Some(temp) = cfg_json.as_object() {
        for(key, value) in temp.iter(){
            debug!("key [{:?}], value [{:?}]", key, String::from(value.as_str().unwrap_or("")));
            cfg_map.insert(String::from(key), String::from(value.as_str().unwrap_or("")));
        }
    }
    debug!("load_cfg_from_file finish");
    return cfg_map;
}
