use std::fs::{File, read_to_string};
use std::path::{Path};
use std::io::prelude::*;
use serde_json::{json};
use log::{error, debug};
use std::process;
use std::fs::metadata;

const DATA: &str = r#"{
    "BOOTSTRAP_SERVERS": "localhost:9092",
    "AUTOCOMMIT_FLAG": false,
    "GROUP_ID": "",
    "OFFSET_RESET_FLAG": "earliest",
    "TOPICS": "quickstart-events"
}"#;

pub const DEFAULT_PATH_STR: &str = "configuration.json";

pub fn load_cfg(file_path: Option<String>) -> serde_json::Value{
    /*
    *   Load configuration file, if file does not exist then create at the specified path.
    *   Panics if file can't be loaded and if default can't created then loaded.
    */
    debug!("load_cfg start");
    
    let mut cfg_path = Path::new(DEFAULT_PATH_STR);

    if let Some(path_str) = file_path.as_deref(){
        cfg_path = Path::new(path_str);
    }

    match check_create_file(&cfg_path) {
        Ok(_) => {
            debug!("load_cfg finish");
            let cfg_map = load_cfg_from_file(Path::new(DEFAULT_PATH_STR));
            debug!("load_cfg finish");
            return cfg_map;
        },
        Err(why) => {
            error!("Error loading configuration file: {:?}", &why.to_string());
            //println!("{:?}", create_default_cfg());
            process::abort()
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

fn create_default_cfg(path: String) -> std::io::Result<()> {
    /*
    *   Create default configuration file at specified path.
    */
    debug!("create_default_cfg start");

    let data: serde_json::Value = serde_json::from_str(DATA).unwrap();
    
    match File::create(path) {
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
    /*
    *   Loads existing configuration from file.
    */
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn test_check_create_file_create_default() {
        // !!! WARNING !!!
        // Running this test will override current configuration.json file
        /*  Test test_check_create_file_create_default function */
        let _ = std::fs::remove_file(Path::new(DEFAULT_PATH_STR));
        assert!(check_create_file(Path::new(DEFAULT_PATH_STR)).is_ok());
        let config_map = load_cfg_from_file(Path::new(DEFAULT_PATH_STR));
        assert_eq!(config_map.is_null(), false);
        assert_eq!(config_map["AUTOCOMMIT_FLAG"].as_bool().unwrap(), false);
    }

    #[test]
    fn test_check_create_file_existing() {
        /*  Test test_check_create_file_existing function */
        assert!(check_create_file(Path::new(DEFAULT_PATH_STR)).is_ok());
        let config_map: serde_json::Value = load_cfg(Some(String::from(DEFAULT_PATH_STR)));
        assert_eq!(config_map.is_null(), false);
        assert_eq!(config_map["AUTOCOMMIT_FLAG"].as_bool().unwrap(), false);
    }

    #[test]
    fn test_get_default_config_path_existing() {
        /*  Test load_cfg_from_file function */
        let config_map = load_cfg_from_file(Path::new(DEFAULT_PATH_STR));
        assert_eq!(config_map.is_null(), false);
        assert_eq!(config_map["AUTOCOMMIT_FLAG"].as_bool().unwrap(), false);
    }

    #[test]
    fn test_load_cfg_existing() {
        /*  Test test_load_cfg_existing function */
        let config_map: serde_json::Value = load_cfg(Some(String::from(DEFAULT_PATH_STR)));
        assert_eq!(config_map.is_null(), false);
        assert_eq!(config_map["AUTOCOMMIT_FLAG"].as_bool().unwrap(), false);
    }

}