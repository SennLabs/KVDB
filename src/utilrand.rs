use std::collections::HashMap;

//use rand::distributions::{Alphanumeric, DistString};

use rand::{prelude::*};


fn standard_charset()  -> HashMap<String, Vec<char>> {
    let mut _charset_map: HashMap<String, Vec<char>> = HashMap::new();
    _charset_map.insert("alpha_lower".to_string(), vec!['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']);
    _charset_map.insert("alpha_upper".to_string(), vec!['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z']);
    _charset_map.insert("numeric".to_string(), vec!['0','1','2','3','4','5','6','7','8','9']);
    _charset_map.insert("special".to_string(), vec!['!','@','#','$','%','^','&','*','(',')','-','_','+','=','{','}','[',']','|','\\',':',';','\'','<','>',',','.','?','/']);

    return _charset_map;
}


pub fn build_charset(charset_options: Vec<String>) -> Vec<char> {
    let general_charset = standard_charset();
    let mut new_charset: Vec<char> = Vec::new();

    for charset_name in charset_options {
        match general_charset.get(&String::from(charset_name)) {
            Some(charlist) => new_charset.extend(charlist),
            None => println!("Charset Not Found"),
        }
    }
    
    return new_charset;
}

pub fn gen_kv_pair(key_length: u32, value_length: u32, chardict: Vec<char>) -> (String,String) {
    let mut rng = rand::rng();

    let mut key_char_vec = Vec::new();

    let mut val_char_vec = Vec::new();

    for _j in 0..key_length {
        let r = rng.random_range(0..36);
        key_char_vec.push(chardict[r]);
    }

    for _i in 0..value_length {
        let r = rng.random_range(0..36);
        val_char_vec.push(chardict[r]);
    }

    return (key_char_vec.into_iter().collect(), val_char_vec.into_iter().collect())

}


// fn example_wordlist() {
//     let wordlist = ["apple","banana","car","dog","elephant","fish","grape","house","ice","jungle","kite","lamp","moon","nest","orange","pen","queen","river","sun","tree","umbrella","violin","whale","xylophone","yarn","zebra","air","book","cloud","dance","earth","fire","glass","hill","island","jacket","key","leaf","mountain","night","ocean","plant","quiet","rain","snow","table","under","voice","wind","xenon","yard","zero","ant","bread","circle","door","egg","flag","game","hat","ink","jam","king","line","milk","net","owl","pig","quilt","road","star","train","unit","vase","wall","axe","yolk","zone","arch","brick","coin","drum","engine","farm","gold","hammer","iron","jewel","knife","log","mirror","nail","oil","pipe","quartz","rope","stone","tent","urn","valley","wheel"];
// }


// fn main() {
//     let char_opt = vec!["alpha_lower".to_string()];
//     let mut new_set = build_charset(char_opt);
// }