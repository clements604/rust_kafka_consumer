use chrono::prelude::*;

pub fn get_epoch_time() -> String {
    let now = Local::now();
    
    return now.format("%d/%m/%Y %H:%M:%S").to_string();
}