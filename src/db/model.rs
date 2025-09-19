use std::time::SystemTime;
use std::collections::HashMap;

use crate::utilities;


// Alias
pub type Key = String;
pub type KeyStore = HashMap<Key, KeyEntry>;



// Entry for each key in memory
#[derive(Clone)]
pub struct KeyEntry {
    pub timestamp: u32,
    pub file_id: u8,
    pub vsz: u32,
    pub voffset: u32, 
}



// Each Key-Value Record
pub struct KeyValueData {
    pub timestamp: u32,
    pub ksz: u32,
    pub vsz: u32,
    pub key: String,
    pub value: String,
}


impl KeyValueData {
    pub fn new(key: String, value: String) -> Self {
        KeyValueData {
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as u32,
            ksz: key.as_bytes().len() as u32,
            vsz: value.as_bytes().len() as u32,
            key,
            value,
        }
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut byte_set: Vec<u8> = Vec::new();
        byte_set.extend(utilities::convert::u32_to_u8set(self.timestamp));
        byte_set.extend(utilities::convert::u32_to_u8set(self.ksz));
        byte_set.extend(utilities::convert::u32_to_u8set(self.vsz));
        byte_set.extend(self.key.as_bytes());
        byte_set.extend(self.value.as_bytes());
        return byte_set;
    }
}
