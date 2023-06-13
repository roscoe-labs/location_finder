use std::collections::HashMap;

use clap::Parser;
use location_finder::location_records_loader::{
    find_city_by_id, find_country_by_id, find_location, find_state_by_id, load_location_records,
    LocationMatchType,
};
use log::{debug, info};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    location_dataset_dir: Option<String>,
    #[arg(long)]
    locations_to_find: String,
}

#[derive(serde::Deserialize, Debug, Clone)]
pub struct LocationInput {
    pub id: u64,
    pub city: String,
    pub state: String,
    pub country: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    simple_logger::init_with_env().unwrap();
    let args = Args::parse();
    if let Some(ref location_dataset_dir) = args.location_dataset_dir {
        info!("location_dataset_dir: {}", location_dataset_dir);
    }
    let res = load_location_records(args.location_dataset_dir);
    if res.is_err() {
        info!("Error loading location records: {:?}", res);
    }
    let mut reader = csv::Reader::from_path(args.locations_to_find)?;
    let mut total_records = 0;
    let mut full_matches = 0;
    let mut city_country_matches = 0;
    let mut unmatched_states: HashMap<(String, String), u32> = HashMap::new();
    for location_input_record in reader.deserialize::<LocationInput>().flatten() {
        debug!("location_record: {:?}", location_input_record);
        total_records += 1;
        let res = find_location(
            &location_input_record.city,
            &location_input_record.state,
            &location_input_record.country,
        )?;
        match res {
            LocationMatchType::FullMatch {
                city,
                state,
                country,
            } => {
                debug!(
                    "Full match: city: {}, state: {}, country: {}",
                    city, state, country
                );
                full_matches += 1;
            }
            LocationMatchType::CityCountryMatch {
                city,
                country,
                unmatched_state,
            } => {
                debug!("City/country match: city: {}, country: {}", city, country);
                let state_record: Option<&location_finder::location_records_loader::LocationState> =
                    find_state_by_id(unmatched_state)?;
                if let Some(state_record) = state_record {
                    let city_record = find_city_by_id(city).unwrap().unwrap();
                    let country_record = find_country_by_id(country).unwrap().unwrap();
                    let k = (
                        format!(
                            "{}, {}, {}",
                            location_input_record.city,
                            location_input_record.state,
                            location_input_record.country
                        ),
                        format!(
                            "{}, {}, {}",
                            city_record.name, state_record.name, country_record.name
                        ),
                    );
                    let c = unmatched_states.get(&k).unwrap_or(&0) + 1;
                    unmatched_states.insert(k, c);
                }
                city_country_matches += 1;
            }
            LocationMatchType::NoMatch => {
                debug!("No match");
            }
        }
    }
    info!(
        "Total records: {}, matched records: {}, full matched records: {}, city country matches: {}, unmatched records: {}",
        total_records,
        full_matches + city_country_matches,
        full_matches,
        city_country_matches,
        total_records - (full_matches + city_country_matches)
    );

    let mut count_vec: Vec<_> = unmatched_states.iter().collect();
    count_vec.sort_by(|a, b| b.1.cmp(a.1));
    debug!("Unmatched states:");
    for (k, v) in count_vec.iter() {
        debug!("{:?}  => {}", k, v);
    }
    Ok(())
}
