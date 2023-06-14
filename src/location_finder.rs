use crate::error::LocationFinderError;
use log::{debug, error, info};
use multimap::MultiMap;
use serde::de::DeserializeOwned;
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{self, BufRead},
    sync::OnceLock,
    vec,
};
use unicode_normalization::UnicodeNormalization;

#[derive(serde::Deserialize, Debug, Clone)]
pub struct LocationCountry {
    pub id: u64,
    pub name: String,
    pub iso3: String,
    pub iso2: String,
    pub numeric_code: u32,
    pub phone_code: String,
    pub capital: String,
    pub currency: String,
    pub currency_name: String,
    pub currency_symbol: String,
    pub tld: String,
    pub native: String,
    pub region: String,
    pub subregion: String,
    pub timezones: String,
    pub latitude: f64,
    pub longitude: f64,
    pub emoji: String,
    #[serde(rename = "emojiU")]
    pub emoji_u: String,
}

#[derive(serde::Deserialize, Debug, Clone)]
pub struct LocationState {
    pub id: u64,
    pub name: String,
    pub country_id: u64,
    pub country_code: String,
    pub country_name: String,
    pub state_code: String,
    #[serde(rename = "type")]
    pub state_type: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
}

#[derive(serde::Deserialize, Debug, Clone)]
pub struct LocationCity {
    pub id: u64,
    pub name: String,
    pub state_id: u64,
    pub state_code: String,
    pub state_name: String,
    pub country_id: u64,
    pub country_code: String,
    pub country_name: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    #[serde(rename = "wikiDataId")]
    pub wiki_data_id: String,
}

trait LocationBase {
    fn id(&self) -> u64;
    fn name(&self) -> &str;
}

impl LocationBase for LocationCity {
    fn id(&self) -> u64 {
        self.id
    }
    fn name(&self) -> &str {
        &self.name
    }
}

impl LocationBase for LocationCountry {
    fn id(&self) -> u64 {
        self.id
    }
    fn name(&self) -> &str {
        &self.name
    }
}

impl LocationBase for LocationState {
    fn id(&self) -> u64 {
        self.id
    }
    fn name(&self) -> &str {
        &self.name
    }
}

static LOCATION_DATASET_DIR: OnceLock<String> = OnceLock::new();
fn init_location_dataset_dir() -> String {
    "./submodules/countries-states-cities-database/csv".to_string()
}
pub fn set_location_dataset_dir(location_dataset_dir: Option<String>) {
    if let Some(location_dataset_dir) = location_dataset_dir {
        LOCATION_DATASET_DIR
            .set(location_dataset_dir.clone())
            .expect("Failed to set location dataset dir");
        info!("Loading location data from: {}", location_dataset_dir);
    }
}

static CITY_ID_MAP: OnceLock<HashMap<u64, LocationCity>> = OnceLock::new();
fn init_city_id_map() -> HashMap<u64, LocationCity> {
    let location_dataset_dir = LOCATION_DATASET_DIR.get_or_init(init_location_dataset_dir);
    load_records_by_id(format!("{}/cities.csv", location_dataset_dir).as_str())
        .expect("Failed to load countries")
}
pub fn get_city_by_id(id: u64) -> Option<&'static LocationCity> {
    CITY_ID_MAP.get_or_init(init_city_id_map).get(&id)
}

static STATE_ID_MAP: OnceLock<HashMap<u64, LocationState>> = OnceLock::new();
fn init_state_id_map() -> HashMap<u64, LocationState> {
    let location_dataset_dir = LOCATION_DATASET_DIR.get_or_init(init_location_dataset_dir);
    load_records_by_id(format!("{}/states.csv", location_dataset_dir).as_str())
        .expect("Failed to load states")
}
pub fn get_state_by_id(id: u64) -> Option<&'static LocationState> {
    STATE_ID_MAP.get_or_init(init_state_id_map).get(&id)
}

static COUNTRY_ID_MAP: OnceLock<HashMap<u64, LocationCountry>> = OnceLock::new();
fn init_country_id_map() -> HashMap<u64, LocationCountry> {
    let location_dataset_dir = LOCATION_DATASET_DIR.get_or_init(init_location_dataset_dir);
    load_records_by_id(format!("{}/countries.csv", location_dataset_dir).as_str())
        .expect("Failed to load countries")
}
pub fn get_country_by_id(id: u64) -> Option<&'static LocationCountry> {
    COUNTRY_ID_MAP.get_or_init(init_country_id_map).get(&id)
}

fn load_records_by_id<T: Clone + std::fmt::Debug + LocationBase + DeserializeOwned>(
    filename: &str,
) -> Result<HashMap<u64, T>, LocationFinderError> {
    let mut id_map: HashMap<u64, T> = HashMap::new();
    let mut reader = csv::Reader::from_path(filename)?;
    for result in reader.deserialize::<T>() {
        if let Ok(location_record) = result {
            let prev_record = id_map.insert(location_record.id(), location_record.clone());
            if prev_record.is_some() {
                error!("Duplicate location record: {:?}", location_record);
                return Err(LocationFinderError::Loader);
            }
        } else {
            error!(
                "Error processing location record: {}",
                result.err().unwrap()
            );
        }
    }
    info!("Loaded {} location records from {}", id_map.len(), filename);
    Ok(id_map)
}

pub fn normalize_location_str(location_str: &str) -> String {
    location_str
        .nfkd()
        .filter(|c| c.is_ascii() && !c.is_ascii_punctuation() && !c.is_ascii_control())
        .collect::<String>()
        .split_ascii_whitespace()
        .collect::<Vec<&str>>()
        .join("_")
        .to_lowercase()
}

pub fn location_key(
    normalized_city: Option<&str>,
    normalized_state: Option<&str>,
    normalized_country: Option<&str>,
) -> String {
    let mut key_parts: Vec<&str> = Vec::new();
    if let Some(normalized_city) = normalized_city {
        key_parts.push(normalized_city);
    }
    if let Some(normalized_state) = normalized_state {
        key_parts.push(normalized_state);
    }
    if let Some(normalized_country) = normalized_country {
        key_parts.push(normalized_country);
    }
    key_parts.join("_")
}

static PLACE_ALIAS_MAP: OnceLock<MultiMap<String, String>> = OnceLock::new();
fn init_place_alias_map() -> MultiMap<String, String> {
    let mut place_alias_map = MultiMap::new();
    let place_alias_file = File::open("./data/place_alias.txt").unwrap();
    let buf_reader = io::BufReader::new(place_alias_file);
    for line in buf_reader.lines() {
        let line = line.unwrap();
        let line_vec: Vec<&str> = line.split('|').map(|s| s.trim()).collect();
        if line_vec.len() == 2 {
            place_alias_map.insert(line_vec[0].to_string(), line_vec[1].to_string());
        }
    }
    place_alias_map
}

fn find_alias_city_names(city_record: &LocationCity) -> Option<&Vec<String>> {
    let alias_place_lookup_key = format!(
        "{}, {}, {}",
        city_record.name, city_record.state_name, city_record.country_name
    );
    PLACE_ALIAS_MAP
        .get_or_init(init_place_alias_map)
        .get_vec(alias_place_lookup_key.as_str())
}
fn find_alias_state_names(state_record: &LocationState) -> Option<&Vec<String>> {
    let alias_place_lookup_key = format!("{}, {}", state_record.name, state_record.country_name);
    PLACE_ALIAS_MAP
        .get_or_init(init_place_alias_map)
        .get_vec(alias_place_lookup_key.as_str())
}

fn list_city_location_keys(
    city_record: &LocationCity,
    city_alias: Option<&str>,
    state_alias: Option<&str>,
) -> Vec<String> {
    let mut location_keys = Vec::new();
    let city_name = normalize_location_str(city_alias.unwrap_or(city_record.name()));
    let state_name = normalize_location_str(state_alias.unwrap_or(&city_record.state_name));
    let country_name = normalize_location_str(&city_record.country_name);
    location_keys.push(location_key(
        Some(&city_name),
        Some(&state_name),
        Some(&country_name),
    ));
    location_keys.push(location_key(Some(&city_name), None, Some(&country_name)));
    let state_record = get_state_by_id(city_record.state_id).unwrap();
    let state_code = normalize_location_str(&state_record.state_code);
    location_keys.push(location_key(
        Some(&city_name),
        Some(&state_code),
        Some(&country_name),
    ));
    let country_record = get_country_by_id(city_record.country_id).unwrap();
    let country_code_iso2 = normalize_location_str(&country_record.iso2);
    location_keys.push(location_key(
        Some(&city_name),
        Some(&state_code),
        Some(&country_code_iso2),
    ));
    location_keys.push(location_key(
        Some(&city_name),
        None,
        Some(&country_code_iso2),
    ));
    let country_code_iso3 = normalize_location_str(&country_record.iso3);
    location_keys.push(location_key(
        Some(&city_name),
        Some(&state_code),
        Some(&country_code_iso3),
    ));
    location_keys.push(location_key(
        Some(&city_name),
        None,
        Some(&country_code_iso3),
    ));
    location_keys
}

static CITY_NAME_MAP: OnceLock<MultiMap<String, u64>> = OnceLock::new();
fn init_city_name_map() -> MultiMap<String, u64> {
    let city_id_map = CITY_ID_MAP.get_or_init(init_city_id_map);
    city_id_map
        .values()
        .fold(MultiMap::new(), |mut city_name_map, city_record| {
            let mut location_keys_set: HashSet<String> =
                list_city_location_keys(city_record, None, None)
                    .into_iter()
                    .collect();

            if let Some(alias_place_names) = find_alias_city_names(city_record) {
                for alias_place_name in alias_place_names {
                    let name_vec: Vec<&str> =
                        alias_place_name.split(',').map(|s| s.trim()).collect();
                    if city_record.name != name_vec[0] && city_record.state_name != name_vec[1] {
                        list_city_location_keys(city_record, Some(name_vec[0]), Some(name_vec[1]))
                            .into_iter()
                            .for_each(|location_key| {
                                location_keys_set.insert(location_key);
                            });
                    }
                    if city_record.state_name != name_vec[1] {
                        list_city_location_keys(city_record, None, Some(name_vec[1]))
                            .into_iter()
                            .for_each(|location_key| {
                                location_keys_set.insert(location_key);
                            });
                    }
                    if city_record.name != name_vec[0] {
                        list_city_location_keys(city_record, Some(name_vec[0]), None)
                            .into_iter()
                            .for_each(|location_key| {
                                location_keys_set.insert(location_key);
                            });
                    }
                }
            }

            let state_record = get_state_by_id(city_record.state_id).unwrap();
            if let Some(alias_place_names) = find_alias_state_names(state_record) {
                for alias_place_name in alias_place_names {
                    let name_vec: Vec<&str> =
                        alias_place_name.split(',').map(|s| s.trim()).collect();
                    if state_record.name != name_vec[0] {
                        list_city_location_keys(city_record, None, Some(name_vec[0]))
                            .into_iter()
                            .for_each(|location_key| {
                                location_keys_set.insert(location_key);
                            });
                    }
                }
            }

            for location_key in location_keys_set {
                city_name_map.insert(location_key, city_record.id());
            }

            city_name_map
        })
}

/**
fn list_state_location_keys(state_record: &LocationState) -> Vec<String> {
    let mut location_keys = Vec::new();
    let state_name = normalize_location_str(state_record.name());
    let country_name = normalize_location_str(&state_record.country_name);
    location_keys.push(location_key(None, Some(&state_name), Some(&country_name)));
    let country_record = get_country_by_id(state_record.country_id).unwrap();
    let country_code_iso2 = normalize_location_str(&country_record.iso2);
    location_keys.push(location_key(
        None,
        Some(&state_name),
        Some(&country_code_iso2),
    ));
    let country_code_iso3 = normalize_location_str(&country_record.iso3);
    location_keys.push(location_key(
        None,
        Some(&state_name),
        Some(&country_code_iso3),
    ));
    location_keys
}

static STATE_NAME_MAP: OnceLock<MultiMap<String, u64>> = OnceLock::new();
fn init_state_name_map() -> MultiMap<String, u64> {
    let state_id_map = STATE_ID_MAP.get_or_init(init_state_id_map);
    state_id_map
        .values()
        .fold(MultiMap::new(), |mut state_name_map, state| {
            for location_key in list_state_location_keys(state) {
                state_name_map.insert(location_key, state.id());
            }
            state_name_map
        })
}

fn list_country_location_keys(country_record: &LocationCountry) -> Vec<String> {
    let mut location_keys = Vec::new();
    let country_name = normalize_location_str(country_record.name());
    location_keys.push(location_key(None, None, Some(&country_name)));
    let country_code_iso2 = normalize_location_str(&country_record.iso2);
    location_keys.push(location_key(None, None, Some(&country_code_iso2)));
    let country_code_iso3 = normalize_location_str(&country_record.iso3);
    location_keys.push(location_key(None, None, Some(&country_code_iso3)));
    location_keys
}
static COUNTRY_NAME_MAP: OnceLock<MultiMap<String, u64>> = OnceLock::new();
fn init_country_name_map() -> MultiMap<String, u64> {
    let country_id_map = COUNTRY_ID_MAP.get_or_init(init_country_id_map);
    country_id_map
        .values()
        .fold(MultiMap::new(), |mut country_name_map, country| {
            for location_key in list_country_location_keys(country) {
                country_name_map.insert(location_key, country.id());
            }
            country_name_map
        })
}
*/

pub enum LocationMatchType {
    FullMatch {
        city: u64,
        state: u64,
        country: u64,
    },
    PartialMatch {
        city: u64,
        country: u64,
        unmatched_state: u64,
    },
    NoMatch,
}

static PARTIAL_MATCH_COUNTRIES_TO_SKIP: OnceLock<HashSet<&'static str>> = OnceLock::new();
fn init_partial_match_countries_to_skip() -> HashSet<&'static str> {
    let mut countries_to_skip = HashSet::new();
    countries_to_skip.insert("United States");
    countries_to_skip
}

static PARTIAL_MATCH_COUNTRIES_TO_OVERRIDE: OnceLock<HashSet<&'static str>> = OnceLock::new();
fn init_partial_match_countries_to_override() -> HashSet<&'static str> {
    let mut countries_to_override = HashSet::new();
    countries_to_override.insert("United Kingdom");
    countries_to_override
}

pub fn find_location(
    city_in: &str,
    state_in: &str,
    country_in: &str,
) -> Result<LocationMatchType, LocationFinderError> {
    let city = normalize_location_str(city_in);
    let state = normalize_location_str(state_in);
    let country = normalize_location_str(country_in);

    let city_map_key = location_key(Some(&city), Some(&state), Some(&country));
    let city_name_matches = CITY_NAME_MAP
        .get_or_init(init_city_name_map)
        .get_vec(&city_map_key);
    if let Some(city_name_matches) = city_name_matches {
        if let Some(city_id) = city_name_matches.iter().next() {
            let city_record = get_city_by_id(*city_id).unwrap();
            let state_record = get_state_by_id(city_record.state_id).unwrap();
            let country_record = get_country_by_id(city_record.country_id).unwrap();
            return Ok(LocationMatchType::FullMatch {
                city: city_record.id,
                state: state_record.id,
                country: country_record.id,
            });
        }
    }

    let city_map_key = location_key(Some(&city), None, Some(&country));
    let city_name_matches = CITY_NAME_MAP
        .get_or_init(init_city_name_map)
        .get_vec(&city_map_key);
    let mut partial_matches: Vec<LocationMatchType> = vec![];
    if let Some(city_name_matches) = city_name_matches {
        for city_id in city_name_matches {
            let city_record = get_city_by_id(*city_id).unwrap();
            let country_record = get_country_by_id(city_record.country_id).unwrap();
            if PARTIAL_MATCH_COUNTRIES_TO_SKIP
                .get_or_init(init_partial_match_countries_to_skip)
                .get(country_record.name())
                .is_some()
            {
                continue;
            }
            if PARTIAL_MATCH_COUNTRIES_TO_OVERRIDE
                .get_or_init(init_partial_match_countries_to_override)
                .get(country_record.name())
                .is_some()
            {
                return Ok(LocationMatchType::FullMatch {
                    city: city_record.id,
                    state: city_record.state_id,
                    country: city_record.country_id,
                });
            }
            let unmatched_state_record = get_state_by_id(city_record.state_id).unwrap();
            let unmatched_state_name = normalize_location_str(unmatched_state_record.name());
            if unmatched_state_name.contains(&state) || state.contains(&unmatched_state_name) {
                debug!(
                    "Partial name match: {} vs {}",
                    state_in,
                    unmatched_state_record.name()
                );
                return Ok(LocationMatchType::FullMatch {
                    city: city_record.id,
                    state: city_record.state_id,
                    country: city_record.country_id,
                });
            }
            partial_matches.push(LocationMatchType::PartialMatch {
                city: city_record.id,
                country: city_record.country_id,
                unmatched_state: city_record.state_id,
            });
        }
        if partial_matches.len() == 1 {
            return Ok(partial_matches.into_iter().next().unwrap());
        }
    }
    Ok(LocationMatchType::NoMatch)
}
