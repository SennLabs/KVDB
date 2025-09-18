use std::fs;
use std::fs::File;
use std::io::Seek;
use std::io::Read;
use std::time::SystemTime;
use std::collections::HashMap;


use crate::dbconfig::DatabaseConfig;


mod dbconfig;
mod utilrand;

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


fn get_kv(key: String, key_set: &Vec<KMem>) -> String {
    for kmem in key_set.iter() {
        //print!("Checking key: {} against {}\n", kmem.key, key);
        if kmem.key == key {
            println!("Found key: {} in file: {}, offset: {}, size: {}", kmem.key, kmem.file_id, kmem.voffset, kmem.vsz);
            let mut file = File::open(format!("{}.kv", kmem.file_id)).unwrap();
            let mut buffer = vec![0; (kmem.vsz) as usize];
            file.seek(std::io::SeekFrom::Start(kmem.voffset as u64)).unwrap();
            file.read_exact(&mut buffer).unwrap();
            return String::from_utf8(buffer).unwrap();
        }
    }
    String::new()
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


fn init_new_file() -> (u32, Vec<KMem>, Vec<u8>){
    let base_offset: u32 = 0;
    let new_keyset: Vec<KMem> = Vec::new();
    let new_datafile: Vec<u8> = Vec::new();

    return ( base_offset, new_keyset, new_datafile );
}



fn add_new_kv(newkey: String, newvalue: String, active_file_id: u8, offset: u32, key_set: &mut Vec<KMem>, active_datafile: &mut Vec<u8>) -> u32 {

        let (new_pair, kmem_entry, new_offset) = build_kv(newkey, newvalue, active_file_id, offset);

        key_set.push(kmem_entry);

        for _b in new_pair.to_bytes().iter() {
            active_datafile.push(*_b);
        }

        fs::write(format!("{}.kv", active_file_id), &active_datafile).unwrap();

        return new_offset;
    }



fn _on_start() -> (dbconfig::DatabaseConfig, u8, (u32, Vec<KMem>, Vec<u8>)) {
    let config = dbconfig::load_config().unwrap();
    let active_file_id: u8 = init_file_id(config.directorypath.as_str());

    return (config, active_file_id, init_new_file());
}



fn _check_and_rollover(active_file_id: u8, offset: u32, maxdatafilelength: u32, key_set: &mut Vec<KMem>, active_datafile: &mut Vec<u8>) -> (u8, u32) {
    if offset >= maxdatafilelength {
        let new_file_id = active_file_id + 1;
        let (new_offset, new_keyset, new_datafile) = init_new_file();
        *key_set = new_keyset;
        *active_datafile = new_datafile;
        return (new_file_id, new_offset);
    }
    return (active_file_id, offset);
}


fn _parse_datafile(mut datafile: File) {

    let mut offset: u32 = 0 as u32;

    let file_metadata = datafile.metadata().expect("File Missing Metadata");
    let file_size = file_metadata.len() as u32;
    

    loop {
        let mut buffer = vec![0; 12 as usize];
        println!("Offset {}",offset);
        datafile.seek(std::io::SeekFrom::Start(offset as u64)).unwrap();
        datafile.read_exact(&mut buffer).unwrap();

        let mut tmp_mem = Vec::new();
        for u32_chunk in buffer.chunks_exact(4) {
            tmp_mem.push(u8set_to_u32(u32_chunk.try_into().unwrap()));
        }

        offset = offset + 12;


        let key_size = tmp_mem[1];
        let mut key_buffer = vec![0; key_size as usize];

        datafile.seek(std::io::SeekFrom::Start(offset as u64)).unwrap();
        datafile.read_exact(&mut key_buffer).unwrap();

        let key_tmp: String = key_buffer.try_into().unwrap();
        
        offset = offset + key_size;

        let val_size = tmp_mem[2];
        let mut val_buffer = vec![0; val_size as usize];

        datafile.seek(std::io::SeekFrom::Start(offset as u64)).unwrap();
        datafile.read_exact(&mut val_buffer).unwrap();

        let val_tmp: String = val_buffer.try_into().unwrap();

        println!("Timecode: {}",tmp_mem[0]);
        println!("Key: {}  Value: {}",key_tmp,val_tmp);
        println!("ksz: {}  vsz: {}",key_size,val_size);
        println!("k-s: {}  v-s: {}",key_tmp.as_bytes().len() as u32,val_tmp.as_bytes().len() as u32);

        offset = offset + val_size;
        
        
        if offset >= file_size {
            println!("EOF");
            break;
        }
    }
}


fn _rebuild_memstore(_config: DatabaseConfig, active_file_id: u8) {

    let mut memstore_hashmap: HashMap<String, KMem> = HashMap::new();
    
    for i in 0..active_file_id {
        let file_path = format!("{}{}.kv", _config.directorypath, i);
        println!("Checking File {}",file_path);
        match fs::exists(file_path) {
            Ok(true) => {
                let datafile = File::open(format!("{}{}.kv", _config.directorypath, i)).unwrap();
                let tmp = _parse_datafile(datafile);
            }
            Ok(false) => {
                println!("File does not exist")
            }
            Err(e)=> {
                println!("Error checking file existence: {}", e)
            }

        }

    }
    


}





fn main() {

    let (_config, active_file_id, (mut offset, mut key_set, mut active_datafile)) = _on_start();

    _rebuild_memstore(_config, active_file_id);

    //let kv_test = _test_add_kv();

    //for (key, value) in kv_test {
    //    offset = add_new_kv(key, value, active_file_id, offset, &mut key_set, &mut active_datafile);
    //}


    // loop {

    //     let (active_file_id, mut offset) = _check_and_rollover(active_file_id, offset, _config.maxdatafilelength, &mut key_set, &mut active_datafile);

    //     println!("Enter command add/get/exit: ");
    //     let mut input_type = String::new();
    //     std::io::stdin().read_line(&mut input_type).expect("Failed to read line");
    //     let input_type = input_type.trim().to_lowercase();


    //     if input_type.starts_with("a") {

    //         println!("Enter key: ");
    //         let mut key = String::new();
    //         std::io::stdin().read_line(&mut key).expect("Failed to read line");
    //         let key = key.trim().to_string();

    //         if key.len() > _config.keymaxlength as usize {
    //             println!("Error: Key length exceeds maximum length of {}", _config.keymaxlength);
    //             continue;
    //         }

    //         println!("Enter value: ");
    //         let mut value = String::new();
    //         std::io::stdin().read_line(&mut value).expect("Failed to read line");
    //         let value = value.trim().to_string();

    //         if value.len() > _config.valuemaxlength as usize {
    //             println!("Error: Value length exceeds maximum length of {}", _config.valuemaxlength);
    //             continue;
    //         }

    //         offset = add_new_kv(key, value, active_file_id, offset, &mut key_set, &mut active_datafile);
    //         println!("Key-Value pair added.");

    //     } 
    //     else if input_type.starts_with("g") {

    //         println!("Enter key to retrieve: ");
    //         let mut key = String::new();
    //         std::io::stdin().read_line(&mut key).expect("Failed to read line");
    //         let key = key.trim().to_string();

    //         if key.len() > _config.keymaxlength as usize {
    //             println!("Error: Key length exceeds maximum length of {}", _config.keymaxlength);
    //             continue;
    //         } 

    //         let value = get_kv(key, &key_set);
    //         if value.is_empty() {
    //             println!("Key not found.");
    //         } else {
    //             println!("Retrieved value: {}", value);
    //         }

    //     } 
    //     else if input_type.starts_with("e") {
    //         break;
    //     } 
    //     else {
    //         println!("Unknown command.");
    //     }
    // }


}



fn _test_add_kv() -> Vec<(String,String)> {

    let mut char_list = utilrand::build_charset(vec!["alpha_lower".to_string(),"numeric".to_string()]);

    let len_key = 12;
    let len_val = 71;
    let num_pair = 250;

    let mut kvlist: Vec<(String,String)> = Vec::new();

    for j in 0..num_pair {
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