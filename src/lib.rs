use std::fs::{File, read_to_string, remove_file};
use std::path::{Path, PathBuf};
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
    "SSL_ENABLED": false,
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
    "BOOTSTRAP_SERVERS": "localhost:9092",
    "AUTOCOMMIT_FLAG": "enable.auto.commit=false",
    "GROUP_ID": "",
    "OFFSET_RESET_FLAG": "auto.offset.reset=earliest",
    "TOPICS": "quickstart-events",
    "BROKER_TIMEOUT": "default.api.timeout.ms=10000",
    "MAX_POLL_RECORDS": "max.poll.records=100"
}"#;

pub const DEFAULT_PATH_STR: &str = "configuration.json";

fn get_config_path() -> &'static Path {
    Path::new(DEFAULT_PATH_STR)
}

pub fn load_cfg(file_path: Option<String>) -> serde_json::Value{
    debug!("load_cfg start");
    
    let mut cfg_path = get_config_path();

    if let Some(path_str) = file_path.as_deref(){
        cfg_path = Path::new(path_str);
    }

    match check_create_file(&cfg_path) {
        Err(why) => {
            error!("Error loading configuration file: {:?}", &why.to_string());
            //println!("{:?}", create_default_cfg());
            process::abort()
        },
        Ok(_) => {
            debug!("load_cfg finish");
            let cfg_map = load_cfg_from_file(get_config_path());
            debug!("load_cfg finish");
            return cfg_map;
        },
    };
}

pub fn check_create_file(path: &Path) -> std::io::Result<()> {
    /*
     *  Checks for config file at provided path, if none exists create default at that path, 
     */
    debug!("check_create_file start");
    match metadata(path) {
        Ok(..) => {
            debug!("Configuration file exists, using existing file");
            debug!("check_cfg_file finish");
            Ok(())
        },
        Err(why) => {
            debug!("{}", why);
            debug!("Configuration file does NOT exist, creating default at provided path.");
            create_default_cfg(path.to_str().unwrap().to_string())
        }
    }
}

/*fn check_cfg_file(path: &Path) -> Result<String>  {
    debug!("check_cfg_file start");
    match metadata(path) {
        Ok(..) => {
            debug!("Configuration file exists, using existing file");
            debug!("check_cfg_file finish");
            Ok(String::from(""))
        },
        Err(..) => {
            info!("Configuration file does NOT exist, creating default.");
            create_default_cfg(String::from(DEFAULT_PATH_STR));
            debug!("check_cfg_file finish");
            Ok(String::from("Configuration file does NOT exist, creating default."))
        }
    }
}*/

fn create_default_cfg(path: String) -> std::io::Result<()> {
    debug!("create_default_cfg start");

    let data: serde_json::Value = serde_json::from_str(DATA).unwrap();
    
    let mut file = match File::create(path) {
        Ok(mut file) => {
            match file.write_all(data.to_string().as_bytes()) {
                Ok(..) => {
                    debug!("create_default_cfg finish");
                    return Ok(())
                },
                Err(why) => {
                    error!("{}", why);
                    return Err(why)
                },
            };
        },
        Err(why) => return Err(why),
    };
}

fn load_cfg_from_file(path: &Path) -> serde_json::Value {
    debug!("load_cfg_from_file start");

    let file_contents: String = match read_to_string(path) {
        Ok(contents) => {
            debug!("{}", contents);
            contents
        },
        Err(why) => {
            error!("{}", why);
            String::from("")
        },
    };

    match serde_json::from_str(&file_contents) {
        Ok(json_obj) => {
            debug!("load_cfg_from_file finish");
            json_obj
        },
        Err(why) => {
            error!("{}", why);
            debug!("load_cfg_from_file finish");
            json!("{}")
        }
    }

}

/*fn load_cfg_from_file(path: &Path) -> HashMap<String, String>{
    debug!("load_cfg_from_file start");
    let file_contents: String = match read_to_string(path) {
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
    let cfg_json: Value = match json_result{
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
}*/


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn test_check_create_file_create_default() {
        // !!! WARNING !!!
        // Running this test will override current configuration.json file
        /*  Runs check_create_file with no configuration file present to test file creation */
        let _ = remove_file(DEFAULT_PATH_STR);
        assert!(check_create_file(Path::new(DEFAULT_PATH_STR)).is_ok());
        let config_map = load_cfg_from_file(get_config_path());
        assert_eq!(config_map.is_null(), false);
        assert_eq!(config_map["SSL_ENABLED"], false);
    }

    #[test]
    fn test_check_create_file_existing() {
        /*  Runs check_create_file with existing configuration file present */
        assert!(check_create_file(Path::new(DEFAULT_PATH_STR)).is_ok());
        let config_map: serde_json::Value = load_cfg(Some(String::from(DEFAULT_PATH_STR)));
        println!("{:?}", config_map["PRIVATE_KEY_PASSWORD"].as_str());
        assert_eq!(config_map.is_null(), false);
        assert_eq!(config_map["SSL_ENABLED"], false);
    }

    #[test]
    fn test_get_config_path_existing() {
        let config_map = load_cfg_from_file(get_config_path());
        assert_eq!(config_map.is_null(), false);
        println!("###########\t{}", config_map["SSL_ENABLED"]);
        assert_eq!(config_map["SSL_ENABLED"], false);
    }

    #[test]
    fn test_load_cfg_existing() {
        let config_map: serde_json::Value = load_cfg(Some(String::from(DEFAULT_PATH_STR)));
        assert_eq!(config_map.is_null(), false);
        assert_eq!(config_map["SSL_ENABLED"], false);
    }

}