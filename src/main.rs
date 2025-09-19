mod db;
mod utilities;
mod interface;


fn main() {

    let (_config, active_file_id, mut mem_keymap, (mut offset, mut active_datafile)) = db::setup::_start();

    loop {
        let (active_file_id, mut offset, mut active_datafile) = db::file::_check_and_rollover(active_file_id, offset, _config.maxdatafilelength, &mut active_datafile);

        println!("Enter command add/get/exit: ");
        let mut input_type = String::new();
        std::io::stdin().read_line(&mut input_type).expect("Failed to read line");
        let input_type = input_type.trim().to_lowercase();


        if input_type.starts_with("a") {
            (offset, mem_keymap) = interface::cli::add(_config.clone(), offset, mem_keymap, active_file_id, &mut active_datafile);
            println!("Key-Value pair added.");
        }
        else if input_type.starts_with("g") {
            let value = interface::cli::get(_config.clone(), mem_keymap.clone());
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



fn _test_add_kv() -> Vec<(String,String)> {

    let char_list = utilities::random::build_charset(vec!["alpha_lower".to_string(),"numeric".to_string()]);

    let len_key = 12;
    let len_val = 71;
    let num_pair = 96;

    let mut kvlist: Vec<(String,String)> = Vec::new();

    for _j in 0..num_pair {
        let (key, val) = utilities::random::gen_kv_pair(len_key,len_val, char_list.clone());
        kvlist.push((key,val));
    }

    kvlist.push(("user_json".to_string(),"{'name':'John', 'age':30, 'car':null}".to_string()));

    for _j in 0..num_pair {
        let (key, val) = utilities::random::gen_kv_pair(len_key,len_val, char_list.clone());
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