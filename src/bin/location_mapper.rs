use std::collections::HashMap;

use clap::Parser;
use location_finder::location_finder::{
    find_location, get_city_by_id, get_country_by_id, get_state_by_id, set_location_dataset_dir,
    LocationMatchType,
};
use log::{debug, info};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    location_dataset_dir: Option<String>,
    #[arg(long)]
    locations_to_map: String,
    #[arg(long)]
    org_locations_to_map: String,
}

#[derive(serde::Deserialize, Debug, Clone)]
pub struct LocationInput {
    pub id: u64,
    pub city: String,
    pub state: String,
    pub country: String,
}

#[derive(serde::Deserialize, Debug, Clone)]
pub struct OrgRecord {
    pub id: u64,
    #[serde(rename = "orgHandle")]
    pub org_handle: String,
    pub name: String,
    pub website: String,
    #[serde(rename = "locationId")]
    pub location_id: u64,
    pub favicon: Option<String>,
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
    set_location_dataset_dir(args.location_dataset_dir);

    let mut reader = csv::Reader::from_path(args.locations_to_map)?;
    let mut location_records_total = 0;
    let mut location_records_full_match = 0;
    let mut location_records_partial_match = 0;

    let mut location_id_to_location_city_id: HashMap<u64, u64> = HashMap::new();

    let mut partial_match_locations: HashMap<(String, String), u32> = HashMap::new();
    for location_input_record in reader.deserialize::<LocationInput>().flatten() {
        debug!("location_record: {:?}", location_input_record);
        location_records_total += 1;
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
                location_records_full_match += 1;
                location_id_to_location_city_id.insert(location_input_record.id, city);
            }
            LocationMatchType::PartialMatch {
                city,
                country,
                unmatched_state,
            } => {
                debug!("City/country match: city: {}, country: {}", city, country);
                let state_record = get_state_by_id(unmatched_state).unwrap();
                let city_record = get_city_by_id(city).unwrap();
                let country_record = get_country_by_id(country).unwrap();
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
                let c = partial_match_locations.get(&k).unwrap_or(&0) + 1;
                partial_match_locations.insert(k, c);

                location_records_partial_match += 1;
            }
            LocationMatchType::NoMatch => {
                debug!("No match");
            }
        }
    }

    info!(
        "Total records: {}, matched records: {}, full matched records: {}, partial matches: {}, unmatched records: {}",
        location_records_total,
        location_records_full_match + location_records_partial_match,
        location_records_full_match,
        location_records_partial_match,
        location_records_total - (location_records_full_match + location_records_partial_match)
    );

    let mut count_vec: Vec<_> = partial_match_locations.iter().collect();
    count_vec.sort_by(|a, b| b.1.cmp(a.1));
    info!("Partial match locations:");
    for (k, v) in count_vec.iter() {
        info!("{:?}  => {}", k, v);
    }

    let mut reader = csv::Reader::from_path(args.org_locations_to_map)?;
    let mut org_records_total = 0;
    let mut org_records_full_match = 0;

    let mut org_locations_not_found: HashMap<String, u32> = HashMap::new();

    for org_record in reader.deserialize::<OrgRecord>().flatten() {
        debug!("org_record: {:?}", org_record);
        org_records_total += 1;
        let location_city_id = location_id_to_location_city_id.get(&org_record.location_id);
        if location_city_id.is_some() {
            org_records_full_match += 1;
        } else {
            let k = format!(
                "{}, {}, {}",
                org_record.city, org_record.state, org_record.country
            );
            let c = org_locations_not_found.get(&k).unwrap_or(&0) + 1;
            org_locations_not_found.insert(k, c);
        }
    }

    info!(
        "Total org records: {}, matched records: {}, unmatched records: {}",
        org_records_total,
        org_records_full_match,
        org_records_total - org_records_full_match
    );

    let mut count_vec: Vec<_> = org_locations_not_found.iter().collect();
    count_vec.sort_by(|a, b| b.1.cmp(a.1));
    info!("Org locations not found:");
    for (k, v) in count_vec.iter() {
        if v < &&10 {
            break;
        }
        info!("{:?}  => {}", k, v);
    }

    Ok(())
}
