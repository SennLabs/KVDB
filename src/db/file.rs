use std::fs;
use std::fs::File;


pub type _FileData = Vec<u8>;

pub fn new() -> (u32, _FileData){
    let base_offset: u32 = 0;
    let new_datafile: _FileData = Vec::new();

    return ( base_offset, new_datafile );
}


pub fn next_id(db_dir_path: &str) -> u8 {
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

// TODO: add log entry for the check and rollover
pub fn _check_and_rollover(active_file_id: u8, offset: u32, maxdatafilelength: u32, _datafile: &mut _FileData) -> (u8, u32, _FileData) {
    if offset >= maxdatafilelength {
        let new_file_id = active_file_id + 1;
        let (new_offset, new_datafile) = new();
        //*_key_set = _new_keyset;
        return (new_file_id, new_offset, new_datafile);
    }
    return (active_file_id, offset, _datafile.to_vec());
}