use chrono::prelude::*;

pub fn get_timestamp() -> String {
    let now = Local::now();   
    return now.format("%d/%m/%Y %H:%M:%S").to_string();
}