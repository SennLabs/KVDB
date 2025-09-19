use std::fs;
use std::fs::File;
use std::io::Seek;
use std::io::Read;
use std::time::SystemTime;
use std::collections::HashMap;


use crate::dbconfig::DatabaseConfig;


mod dbconfig;
mod utilrand;

#[derive(Clone)]
struct KMem {
    key: String,
    file_id: u8,
    timestamp: u32,
    vsz: u32,
    voffset: u32, 
}

fn kmem_from_kvdata(data: &KVData, file_id: u8, voffset: u32) -> KMem {
    KMem {
        key: data.key.clone(),
        file_id,
        timestamp: data.timestamp,
        vsz: data.vsz as u32,
        voffset: voffset + 4 + 4 + 4 + data.ksz as u32, // 32 bits timestamp + 32 bits keysizestore + 32 bits value size + key size
    }
}

// Convert the u32 to 4 u8, in big endian style
fn u32_to_4u8(x:u32) -> [u8;4] {
    let b1 : u8 = ((x >> 24) & 0xff) as u8;
    let b2 : u8 = ((x >> 16) & 0xff) as u8;
    let b3 : u8 = ((x >> 8) & 0xff) as u8;
    let b4 : u8 = (x & 0xff) as u8;
    return [b1, b2, b3, b4]
}

fn u8set_to_u32(x:[u8;4]) -> u32 {
    let b = u32::from_be_bytes(x);
    return b;
}


struct KVData {
    timestamp: u32,
    ksz: u32,
    vsz: u32,
    key: String,
    value: String,
}

impl KVData {
    fn new(key: String, value: String) -> Self {
        KVData {
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
    fn to_bytes(&self) -> Vec<u8> {
        let mut byte: Vec<u8> = Vec::new();
        let ts_bytes = u32_to_4u8(self.timestamp);
        let ksz_bytes = u32_to_4u8(self.ksz);
        let vsz_bytes = u32_to_4u8(self.vsz);
        byte.extend(&ts_bytes);
        byte.extend(&ksz_bytes);
        byte.extend(&vsz_bytes);
        byte.extend(self.key.as_bytes());
        byte.extend(self.value.as_bytes());
        byte
    }
}


fn build_kv(key_string: String, value_string: String, active_file_id: u8, offset: u32) -> (KVData, KMem, u32) {
    let new_pair = KVData::new(key_string, value_string);
    let kmem_entry = kmem_from_kvdata(&new_pair, active_file_id, offset);
    let new_offset = offset + new_pair.to_bytes().len() as u32; // 32 bits timestamp + 16 bits ksz + 16 bits vsz + key size + value size
    (new_pair, kmem_entry, new_offset)
}

fn read_value(file_id:u8, value_length: u32, value_offset: u32) -> String {
    let mut file = File::open(format!("{}.kv", file_id)).unwrap();
    let mut buffer = vec![0; (value_length) as usize];
    file.seek(std::io::SeekFrom::Start(value_offset as u64)).unwrap();
    file.read_exact(&mut buffer).unwrap();
    return String::from_utf8(buffer).unwrap();
}

fn get_kv(key: String, key_map: HashMap<String, KMem>) -> String {

    match key_map.get(&key) {
        Some(keydata) => return read_value(keydata.file_id, keydata.vsz, keydata.voffset),
        None => return String::new()
    }
}


fn init_file_id(db_dir_path: &str) -> u8 {
    let mut file_id = 0;

    for entry in fs::read_dir(db_dir_path).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let fname = path.to_str().unwrap();
        if fname.ends_with(".kv") {
            let id_str = &fname[db_dir_path.len()..fname.len()-3]; // trim path prefix and remove .kv
            if let Ok(id) = id_str.parse::<u8>() {
                if id >= file_id {
                    file_id = id + 1;
                }
            }
        }
    }
    return file_id;
}


fn init_new_file() -> (u32, Vec<u8>){
    let base_offset: u32 = 0;
    let new_datafile: Vec<u8> = Vec::new();

    return ( base_offset, new_datafile );
}



fn add_new_kv(newkey: String, newvalue: String, active_file_id: u8, offset: u32, mut key_set: HashMap<String, KMem>, active_datafile: &mut Vec<u8>) -> (u32, HashMap<String, KMem>)  {

        let (new_pair, kmem_entry, new_offset) = build_kv(newkey.clone(), newvalue, active_file_id, offset);

        key_set.insert(newkey, kmem_entry);

        for _b in new_pair.to_bytes().iter() {
            active_datafile.push(*_b);
        }

        fs::write(format!("{}.kv", active_file_id), &active_datafile).unwrap();

        return (new_offset, key_set);
    }



fn _on_start() -> (dbconfig::DatabaseConfig, u8, HashMap<String, KMem>, (u32, Vec<u8>)) {
    let config = dbconfig::load_config().unwrap();
    let active_file_id: u8 = init_file_id(config.directorypath.as_str());
    let mem_keyhashmap: HashMap<String, KMem> = HashMap::new();

    if active_file_id > 0 {
        let mem_keyhashmap: HashMap<String, KMem> = _rebuild_memstore(dbconfig::load_config().unwrap(), active_file_id);
        return (config, active_file_id, mem_keyhashmap, init_new_file());
    }
    return (config, active_file_id, mem_keyhashmap, init_new_file());
}


    // TODO: add log entry for the check and rollover
fn _check_and_rollover(active_file_id: u8, offset: u32, maxdatafilelength: u32, active_datafile: &mut Vec<u8>) -> (u8, u32, Vec<u8>) {
    if offset >= maxdatafilelength {
        let new_file_id = active_file_id + 1;
        let (new_offset, new_datafile) = init_new_file();
        //*_key_set = _new_keyset;
        return (new_file_id, new_offset, new_datafile);
    }
    return (active_file_id, offset, active_datafile.to_vec());
}



fn _kmem_from_datafile(mut datafile: File, datafile_id: u8) -> HashMap<String, KMem>{

    // Set our in-file position tracker to the start
    let mut offset: u32 = 0 as u32;
    let file_size = datafile.metadata().expect("File Missing Metadata").len() as u32; // Pull the file size to all for us to check when we hit the end

    let mut kmem_list: HashMap<String, KMem> = HashMap::new();

    loop {

        // For each entry, the first items of fixed size (Timestamp:u32, keysize:u32, valuesize:u32)
        let mut buffer = vec![0; 12 as usize];

        datafile.seek(std::io::SeekFrom::Start(offset as u64)).unwrap();
        datafile.read_exact(&mut buffer).unwrap();

        // tmp_mem is just a holding container for the first 3 u32 chunks
        let mut tmp_mem = Vec::new();
        for u32_chunk in buffer.chunks_exact(4) {
            tmp_mem.push(u8set_to_u32(u32_chunk.try_into().unwrap()));
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
        let _tmp_kmem: KMem = KMem { key: (key_string.clone()), file_id: (datafile_id), timestamp: (kv_ts), vsz: (val_size), voffset: (offset) };
        kmem_list.insert(key_string, _tmp_kmem);

        offset = offset + val_size;
        if offset >= file_size {
            println!("EOF");
            break;
        }
    }
    return kmem_list;
}


fn _rebuild_memstore(_config: DatabaseConfig, active_file_id: u8) -> HashMap<std::string::String, KMem> {

    let mut memstore_hashmap: HashMap<String, KMem> = HashMap::new();
    
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





fn main() {

    let (_config, active_file_id, mut mem_keymap, (mut offset, mut active_datafile)) = _on_start();

    let kv_test = _test_add_kv();

    for (key, value) in kv_test {
        (offset,mem_keymap) = add_new_kv(key, value, active_file_id, offset, mem_keymap, &mut active_datafile);
    }


    loop {
        let (active_file_id, mut offset, mut active_datafile) = _check_and_rollover(active_file_id, offset, _config.maxdatafilelength, &mut active_datafile);

        println!("Enter command add/get/exit: ");
        let mut input_type = String::new();
        std::io::stdin().read_line(&mut input_type).expect("Failed to read line");
        let input_type = input_type.trim().to_lowercase();


        if input_type.starts_with("a") {
            (offset, mem_keymap) = interface_cli_add(_config.clone(), offset, mem_keymap, active_file_id, &mut active_datafile);
            println!("Key-Value pair added.");
        }
        else if input_type.starts_with("g") {
            let value = interface_cli_get(_config.clone(), mem_keymap.clone());
            if value.is_empty() {
                println!("Key not found.");
            } else {
                println!("Retrieved value: {}", value);
            }
        } 
        else if input_type.starts_with("e") {
            break;
        } 
        else {
            println!("Unknown command.");
        }
    }


}


fn interface_cli_add(_config: DatabaseConfig, voffset: u32, key_hashmap: HashMap<String, KMem>, activefileid:u8, active_datafile: &mut Vec<u8>)  -> (u32, HashMap<std::string::String, KMem>) {

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

    return add_new_kv(key, value, activefileid, voffset, key_hashmap, active_datafile);
}

fn interface_cli_get(_config: DatabaseConfig, key_hashmap: HashMap<String, KMem>) -> String {

    println!("Enter key to retrieve: ");
    let mut key = String::new();
    std::io::stdin().read_line(&mut key).expect("Failed to read line");
    let key = key.trim().to_string();

    if key.len() > _config.keymaxlength as usize {
        println!("Error: Key length exceeds maximum length of {}", _config.keymaxlength);
        return String::new();
    } 
    let value = get_kv(key, key_hashmap);
    if value.is_empty() {
        return String::new();
    } else {
        return value;
    }
}


fn _test_add_kv() -> Vec<(String,String)> {

    let char_list = utilrand::build_charset(vec!["alpha_lower".to_string(),"numeric".to_string()]);

    let len_key = 12;
    let len_val = 71;
    let num_pair = 96;

    let mut kvlist: Vec<(String,String)> = Vec::new();

    for _j in 0..num_pair {
        let (key, val) = utilrand::gen_kv_pair(len_key,len_val, char_list.clone());
        kvlist.push((key,val));
    }

    kvlist.push(("user_json".to_string(),"{'name':'John', 'age':30, 'car':null}".to_string()));

    for _j in 0..num_pair {
        let (key, val) = utilrand::gen_kv_pair(len_key,len_val, char_list.clone());
        kvlist.push((key,val));
    }

    //     let kvlist = vec![
    //     ("dbpzpyju88".to_string(), "belief romanian bridge profit".to_string()),
    //     ("1e0djtng3x".to_string(), "arts started bundle disease".to_string()),
    //     ("jfdresogpj".to_string(), "repeated smoky online daffodil".to_string()),
    //     ("bamwzbnp63".to_string(), "keating post warburg johnson".to_string()),
    //     ("j347yuk8ux".to_string(), "footpath fragrant trembling seltzer limes trend blurb reliant dosage aground anime tripping".to_string()),
    //     ("udzsa7481p".to_string(), "footpath fragrant trembling seltzer limes trend blurb reliant dosage aground anime tripping".to_string()),
    //     ("3vevcj00lf".to_string(), "efforts denying billed buy".to_string()),
    //     ("qpc98fm7f4".to_string(), "whose category fonts mutual".to_string()),
    //     ("hcou91nzgr".to_string(), "easing autonomy weight five".to_string()),
    //     ("yvu2j7qf70".to_string(), "delay gradual asset centers".to_string()),
    // ];

    return kvlist;

}

// fn _test_get_kv(keyset_mem: &Vec<KMem>) {

//     let test_kv = get_kv("key3".to_string(), &keyset_mem);
//     println!("{}", test_kv);
// }