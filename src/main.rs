use std::fs;
use std::fs::File;
use std::io::Write;
use std::io::Seek;
use std::io::Read;
use std::time::SystemTime;
use std::str::Bytes;


mod dbconfig;


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


fn transform_u32_to_array_of_u8(x:u32) -> [u8;4] {
    let b1 : u8 = ((x >> 24) & 0xff) as u8;
    let b2 : u8 = ((x >> 16) & 0xff) as u8;
    let b3 : u8 = ((x >> 8) & 0xff) as u8;
    let b4 : u8 = (x & 0xff) as u8;
    return [b1, b2, b3, b4]
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
        let ts_bytes = transform_u32_to_array_of_u8(self.timestamp);
        let ksz_bytes = transform_u32_to_array_of_u8(self.ksz);
        let vsz_bytes = transform_u32_to_array_of_u8(self.vsz);
        byte.extend(&ts_bytes);
        byte.extend(&ksz_bytes);
        byte.extend(&vsz_bytes);
        byte.extend(self.key.as_bytes());
        byte.extend(self.value.as_bytes());
        byte
    }
}


fn set_kv(key_string: String, value_string: String, active_file_id: u8, offset: u32) -> (KVData, KMem, u32) {
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




fn main() {
    let config = dbconfig::load_config().unwrap();

    let active_file_id: u8 = init_file_id(config.directorypath.as_str());
    let (mut offset, mut key_set, mut active_datafile) = init_new_file();


    let kvlist = vec![
        ("key1".to_string(), "val1".to_string()),
        ("key2".to_string(), "1d".to_string()),
        ("key3".to_string(), "typel3".to_string()),
        ("key4".to_string(), "val4".to_string()),
        ("key5".to_string(), "1".to_string()),
        ("key6".to_string(), "val6".to_string()),
        ("key7".to_string(), "val7".to_string()),
        ("key8".to_string(), "a".to_string()),
        ("key9".to_string(), "val9".to_string()),
        ("key0".to_string(), "vThe lager typeal0".to_string()),
    ];


    for (key, value) in kvlist {
        //println!("Inserting key: {}, offset: {}", key, offset);
        let (new_pair, kmem_entry, new_offset) = set_kv(key, value, active_file_id, offset);
        key_set.push(kmem_entry);
        for _b in new_pair.to_bytes().iter() {
            active_datafile.push(*_b);
        }
        fs::write(format!("{}.kv", active_file_id), &active_datafile).unwrap();
        offset = new_offset;
    }

    let test_kv = get_kv("key3".to_string(), &key_set);
    println!("{}", test_kv);

}
