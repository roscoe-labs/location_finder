use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, BufRead, Write},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut place_alias_map = BTreeMap::new();
    let place_alias_file = File::open("./data/place_alias.txt").unwrap();
    let buf_reader = io::BufReader::new(place_alias_file);
    for line in buf_reader.lines() {
        let line = line.unwrap();
        let line_vec: Vec<&str> = line.split('|').map(|s| s.trim()).collect();
        if line_vec.len() == 2 {
            let place_key_vec = line_vec[0]
                .split(',')
                .map(|s| s.trim())
                .collect::<Vec<&str>>();
            let key = place_key_vec.iter().rev().fold(String::new(), |acc, x| {
                if acc.is_empty() {
                    x.to_string()
                } else {
                    acc + ", " + x
                }
            });
            place_alias_map.insert(key, line.to_string());
        }
    }

    let mut place_alias_file = File::create("./data/place_alias_sorted.txt").unwrap();
    for (_, v) in place_alias_map.iter() {
        place_alias_file.write_all(v.as_bytes())?;
        place_alias_file.write_all(b"\n")?;
    }

    Ok(())
}
