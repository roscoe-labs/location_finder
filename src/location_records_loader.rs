use std::{collections::HashMap, sync::OnceLock};

use log::{error, info};
use serde::de::DeserializeOwned;

use crate::error::LocationFinderError;

trait LocationBase {
    fn id(&self) -> u64;
    fn name(&self) -> &str;
}

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
impl LocationBase for LocationCountry {
    fn id(&self) -> u64 {
        self.id
    }
    fn name(&self) -> &str {
        &self.name
    }
}
static COUNTRY_ID_MAP: OnceLock<HashMap<u64, LocationCountry>> = OnceLock::new();
static COUNTRY_NAME_MAP: OnceLock<HashMap<String, LocationCountry>> = OnceLock::new();

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
impl LocationBase for LocationState {
    fn id(&self) -> u64 {
        self.id
    }
    fn name(&self) -> &str {
        &self.name
    }
}
static STATE_ID_MAP: OnceLock<HashMap<u64, LocationState>> = OnceLock::new();
static STATE_NAME_MAP: OnceLock<HashMap<String, LocationState>> = OnceLock::new();

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
impl LocationBase for LocationCity {
    fn id(&self) -> u64 {
        self.id
    }
    fn name(&self) -> &str {
        &self.name
    }
}
static CITY_ID_MAP: OnceLock<HashMap<u64, LocationCity>> = OnceLock::new();
static CITY_NAME_MAP: OnceLock<HashMap<String, LocationCity>> = OnceLock::new();

pub fn load_location_records(
    location_dataset_dir: Option<String>,
) -> Result<(), LocationFinderError> {
    let location_dataset_dir = location_dataset_dir
        .unwrap_or("./submodules/countries-states-cities-database/csv".to_string());
    load_records(
        format!("{}/countries.csv", location_dataset_dir).as_str(),
        &COUNTRY_ID_MAP,
        &COUNTRY_NAME_MAP,
    )?;
    load_records(
        format!("{}/states.csv", location_dataset_dir).as_str(),
        &STATE_ID_MAP,
        &STATE_NAME_MAP,
    )?;
    load_records(
        format!("{}/cities.csv", location_dataset_dir).as_str(),
        &CITY_ID_MAP,
        &CITY_NAME_MAP,
    )?;
    Ok(())
}

fn load_records<T: Clone + LocationBase + DeserializeOwned>(
    filename: &str,
    static_id_map: &OnceLock<HashMap<u64, T>>,
    static_name_map: &OnceLock<HashMap<String, T>>,
) -> Result<(), LocationFinderError> {
    let mut id_map: HashMap<u64, T> = HashMap::new();
    let mut name_map: HashMap<String, T> = HashMap::new();
    let mut reader = csv::Reader::from_path(filename)?;
    for result in reader.deserialize::<T>() {
        if let Ok(location_record) = result {
            id_map.insert(location_record.id(), location_record.clone());
            name_map.insert(location_record.name().to_string(), location_record);
        } else {
            error!(
                "Error processing location record: {}",
                result.err().unwrap()
            );
        }
    }
    static_id_map
        .set(id_map)
        .map_err(|_| LocationFinderError::Loader)?;
    static_name_map
        .set(name_map)
        .map_err(|_| LocationFinderError::Loader)?;
    info!(
        "Loaded {} location records from {}",
        static_id_map
            .get()
            .ok_or(LocationFinderError::Loader)?
            .len(),
        filename
    );
    Ok(())
}
