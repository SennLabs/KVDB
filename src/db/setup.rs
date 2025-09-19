use std::fs;
use std::fs::File;
use std::io::Seek;
use std::io::Read;

use crate::utilities;
use crate::db;
use db::config::DatabaseConfig;
use db::model::{KeyEntry, KeyStore};
use std::collections::HashMap;



pub fn _start() -> (db::config::DatabaseConfig, u8, KeyStore, (u32, Vec<u8>)) {
    let config = db::config::load_config().unwrap();
    let active_file_id: u8 = db::file::next_id(config.directorypath.as_str());
    let mem_keyhashmap: KeyStore = HashMap::new();

    if active_file_id > 0 {
        let mem_keyhashmap: KeyStore = _rebuild_memstore(db::config::load_config().unwrap(), active_file_id);
        return (config, active_file_id, mem_keyhashmap, db::file::new());
    }
    return (config, active_file_id, mem_keyhashmap, db::file::new());
}



fn _kmem_from_datafile(mut datafile: File, datafile_id: u8) -> KeyStore{

    // Set our in-file position tracker to the start
    let mut offset: u32 = 0 as u32;
    let file_size = datafile.metadata().expect("File Missing Metadata").len() as u32; // Pull the file size to all for us to check when we hit the end

    let mut kmem_list: KeyStore = HashMap::new();

    loop {

        // For each entry, the first items of fixed size (Timestamp:u32, keysize:u32, valuesize:u32)
        let mut buffer = vec![0; 12 as usize];

        datafile.seek(std::io::SeekFrom::Start(offset as u64)).unwrap();
        datafile.read_exact(&mut buffer).unwrap();

        // tmp_mem is just a holding container for the first 3 u32 chunks
        let mut tmp_mem = Vec::new();
        for u32_chunk in buffer.chunks_exact(4) {
            tmp_mem.push(utilities::convert::u8set_to_u32(u32_chunk.try_into().unwrap()));
        }
        offset = offset + 12;

        // As the order of these is set, we can get our timestamp, key length and value length
        let kv_ts: u32 = tmp_mem[0];
        let key_size: u32 = tmp_mem[1];
        let val_size: u32 = tmp_mem[2];

        // Using our now known key length, we can get all the bytes for our key string. 
        let mut key_buffer = vec![0; key_size as usize];
        datafile.seek(std::io::SeekFrom::Start(offset as u64)).unwrap();
        datafile.read_exact(&mut key_buffer).unwrap();
        let key_string: String = key_buffer.try_into().unwrap();
        offset = offset + key_size;

        // TODO: Remove this as we don't need the value string for the in-mem key store
            // Using our now known value length, we can get all the bytes for our value string. 
        let mut val_buffer = vec![0; val_size as usize];
        datafile.seek(std::io::SeekFrom::Start(offset as u64)).unwrap();
        datafile.read_exact(&mut val_buffer).unwrap();

        // 
        let _tmp_kmem: KeyEntry = KeyEntry { file_id: (datafile_id), timestamp: (kv_ts), vsz: (val_size), voffset: (offset) };
        kmem_list.insert(key_string, _tmp_kmem);

        offset = offset + val_size;
        if offset >= file_size {
            println!("EOF");
            break;
        }
    }
    return kmem_list;
}


fn _rebuild_memstore(_config: DatabaseConfig, active_file_id: u8) -> KeyStore {

    let mut memstore_hashmap: KeyStore = HashMap::new();
    
    for i in 0..active_file_id {
        let file_path = format!("{}{}.kv", _config.directorypath, i);
        println!("Checking File {}",file_path);
        match fs::exists(file_path) {
            Ok(true) => {
                let datafile = File::open(format!("{}{}.kv", _config.directorypath, i)).unwrap();
                memstore_hashmap.extend(_kmem_from_datafile(datafile, i));
            }
            Ok(false) => {
                println!("File does not exist")
            }
            Err(e)=> {
                println!("Error checking file existence: {}", e)
            }
        }
    } // End of interating through files
    return memstore_hashmap; 
}
