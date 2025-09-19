use crate::db;
use db::model::{KeyEntry, KeyStore, KeyValueData};
//use std::collections::HashMap;

use std::fs;
use std::fs::File;
use std::io::Seek;
use std::io::Read;




fn new_keyentry(kv_data: &KeyValueData, file_id: u8, entry_offset: u32) -> KeyEntry {
    KeyEntry {
        timestamp: kv_data.timestamp,
        file_id,
        vsz: kv_data.vsz as u32,
        voffset: entry_offset + 4 + 4 + 4 + kv_data.ksz as u32, // 32 bits timestamp + 32 bits keysizestore + 32 bits value size + key size
    }
}


fn build_kv(key_string: String, value_string: String, active_file_id: u8, offset: u32) -> (KeyValueData, KeyEntry, u32) {
    let kv_set = KeyValueData::new(key_string, value_string);
    let kmem_entry = new_keyentry(&kv_set, active_file_id, offset);
    let new_offset = offset + kv_set.to_bytes().len() as u32; // 32 bits timestamp + 16 bits ksz + 16 bits vsz + key size + value size
    (kv_set, kmem_entry, new_offset)
}





// was add_new_kv
pub fn add_kv(newkey: String, newvalue: String, active_file_id: u8, offset: u32, mut key_set: KeyStore, active_datafile: &mut Vec<u8>) -> (u32, KeyStore)  {

    let (keyvalue_set, newentry, new_offset) = build_kv(newkey.clone(), newvalue, active_file_id, offset);

    key_set.insert(newkey, newentry);

    for _b in keyvalue_set.to_bytes().iter() {
        active_datafile.push(*_b);
    }

    fs::write(format!("{}.kv", active_file_id), &active_datafile).unwrap();

    return (new_offset, key_set);
}


fn read_value(file_id:u8, value_length: u32, value_offset: u32) -> String {
    let mut file = File::open(format!("{}.kv", file_id)).unwrap();
    let mut buffer = vec![0; (value_length) as usize];
    file.seek(std::io::SeekFrom::Start(value_offset as u64)).unwrap();
    file.read_exact(&mut buffer).unwrap();
    return String::from_utf8(buffer).unwrap();
}

pub fn get_kv(key: String, key_map: KeyStore) -> String {
    match key_map.get(&key) {
        Some(keydata) => return read_value(keydata.file_id, keydata.vsz, keydata.voffset),
        None => return String::new()
    }
}