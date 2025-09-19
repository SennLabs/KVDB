use crate::db;

use db::config::DatabaseConfig;
use db::model::{KeyStore};

pub fn add(_config: DatabaseConfig, voffset: u32, key_hashmap: KeyStore, activefileid:u8, active_datafile: &mut Vec<u8>)  -> (u32, KeyStore) {

    println!("Enter key: ");
    let mut key = String::new();
    std::io::stdin().read_line(&mut key).expect("Failed to read line");
    let key = key.trim().to_string();

    if key.len() > _config.keymaxlength as usize {
        println!("Error: Key length exceeds maximum length of {}", _config.keymaxlength);
        return (voffset, key_hashmap);
    }

    println!("Enter value: ");
    let mut value = String::new();
    std::io::stdin().read_line(&mut value).expect("Failed to read line");
    let value = value.trim().to_string();

    if value.len() > _config.valuemaxlength as usize {
        println!("Error: Value length exceeds maximum length of {}", _config.valuemaxlength);
        return (voffset, key_hashmap);
    }

    return db::operations::add_kv(key, value, activefileid, voffset, key_hashmap, active_datafile);
}

pub fn get(_config: DatabaseConfig, key_hashmap: KeyStore) -> String {

    println!("Enter key to retrieve: ");
    let mut key = String::new();
    std::io::stdin().read_line(&mut key).expect("Failed to read line");
    let key = key.trim().to_string();

    if key.len() > _config.keymaxlength as usize {
        println!("Error: Key length exceeds maximum length of {}", _config.keymaxlength);
        return String::new();
    } 
    let value = db::operations::get_kv(key, key_hashmap);
    if value.is_empty() {
        return String::new();
    } else {
        return value;
    }
}
