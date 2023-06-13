use std::{
    collections::{HashMap, HashSet},
    sync::OnceLock,
};

use log::{debug, error, info};
use multimap::MultiMap;
use serde::de::DeserializeOwned;

use unicode_normalization::UnicodeNormalization;

use crate::error::LocationFinderError;

pub trait LocationBase {
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
static COUNTRY_NAME_MAP: OnceLock<MultiMap<String, LocationCountry>> = OnceLock::new();

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
static STATE_NAME_MAP: OnceLock<MultiMap<String, LocationState>> = OnceLock::new();

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
static CITY_NAME_MAP: OnceLock<MultiMap<String, LocationCity>> = OnceLock::new();

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

fn load_records<T: Clone + std::fmt::Debug + LocationBase + DeserializeOwned>(
    filename: &str,
    static_id_map: &OnceLock<HashMap<u64, T>>,
    static_name_map: &OnceLock<MultiMap<String, T>>,
) -> Result<(), LocationFinderError> {
    let mut id_map: HashMap<u64, T> = HashMap::new();
    let mut name_map: MultiMap<String, T> = MultiMap::new();
    let mut reader = csv::Reader::from_path(filename)?;
    for result in reader.deserialize::<T>() {
        if let Ok(location_record) = result {
            let prev_record = id_map.insert(location_record.id(), location_record.clone());
            if prev_record.is_some() {
                error!("Duplicate location record: {:?}", location_record);
                return Err(LocationFinderError::Loader);
            }
            name_map.insert(
                normalize_location_str(location_record.name()),
                location_record.clone(),
            );
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

pub enum LocationMatchType {
    FullMatch {
        city: u64,
        state: u64,
        country: u64,
    },
    CityCountryMatch {
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

static PARTIAL_MATCH_STATE_NAMES: OnceLock<HashSet<(&'static str, &'static str)>> = OnceLock::new();
fn init_partial_match_state_names() -> HashSet<(&'static str, &'static str)> {
    let mut state_names = HashSet::new();
    let state_names_vec = vec![
        ("lombardia", "lombardy"),
        ("toscana", "tuscany"),
        ("piemonte", "piedmont"),
        ("sardegna", "sardinia"),
        ("sicilia", "sicily"),
        ("puglia", "apulia"),
        ("trentinoalto_adige", "trentinosouth_tyrol"),
        ("bayern", "bavaria"),
        ("sachsen", "saxony"),
        ("niedersachsen", "lower_saxony"),
        ("rheinlandpfalz", "rhinelandpalatinate"),
        ("nordrheinwestfalen", "north_rhinewestphalia"),
        ("catalonia", "barcelona"),
        ("pais_vasco", "gipuzkoa"),
        ("pais_vasco", "bizkaia"),
        ("galicia", "pontevedra"),
        ("galicia", "a_coruna"),
        ("andalucia", "sevilla"),
        ("stockholms_lan", "stockholm_county"),
        ("skane_lan", "skane_county"),
        ("kalmar_lan", "kalmar_county"),
        ("vasternorrlands_lan", "vasternorrland_county"),
        ("vasterbottens_lan", "vasterbotten_county"),
        ("uppsala_lan", "uppsala_county"),
        ("norrbottens_lan", "norrbotten_county"),
        ("hallands_lan", "halland_county"),
        ("ostergotlands_lan", "ostergotland_county"),
        ("dalarnas_lan", "dalarna_county"),
        ("vastmanlands_lan", "vastmanland_county"),
        ("varmlands_lan", "varmland_county"),
        ("sodermanlands_lan", "sodermanland_county"),
        ("kronobergs_lan", "skane_county"),
        ("gotlands_lan", "gotland_county"),
        ("wien", "vienna"),
        ("geneve", "geneva"),
        ("nordpasdecalais", "hautsdefrance"),
        ("midipyrenees", "occitanie"),
        ("languedocroussillon", "occitanie"),
        ("provencealpescote_dazur", "provencealpescotedazur"),
        ("pays_de_la_loire", "paysdelaloire"),
        ("andhra_pradesh", "telangana"),
        ("hamerkaz", "central_district"),
        ("al_qahirah", "cairo"),
        ("adis_abeba", "addis_ababa"),
        ("na_south_africa", "gauteng"),
    ];
    for (state_name, unmatched_state_name) in state_names_vec {
        state_names.insert((state_name, unmatched_state_name));
        state_names.insert((unmatched_state_name, state_name));
    }
    state_names
}

pub fn find_location(
    city_in: &str,
    state_in: &str,
    country_in: &str,
) -> Result<LocationMatchType, LocationFinderError> {
    let city = normalize_location_str(city_in);
    let state = normalize_location_str(state_in);
    let country = normalize_location_str(country_in);
    let city_name_matches = CITY_NAME_MAP
        .get()
        .ok_or(LocationFinderError::Loader)?
        .get_vec(&city);
    let state_name_matches = STATE_NAME_MAP
        .get()
        .ok_or(LocationFinderError::Loader)?
        .get_vec(&state);
    let country_name_matches = COUNTRY_NAME_MAP
        .get()
        .ok_or(LocationFinderError::Loader)?
        .get_vec(&country);

    if let Some(city_name_matches) = city_name_matches {
        for city in city_name_matches {
            if let Some(state_name_matches) = state_name_matches {
                for state in state_name_matches {
                    if let Some(country_name_matches) = country_name_matches {
                        for country in country_name_matches {
                            if city.state_id == state.id()
                                && city.country_id == country.id()
                                && state.country_id == country.id()
                            {
                                return Ok(LocationMatchType::FullMatch {
                                    city: city.id(),
                                    state: state.id(),
                                    country: country.id(),
                                });
                            }
                        }
                    }
                }
            }
        }
        for city in city_name_matches {
            if let Some(country_name_matches) = country_name_matches {
                for country in country_name_matches {
                    if PARTIAL_MATCH_COUNTRIES_TO_SKIP
                        .get_or_init(init_partial_match_countries_to_skip)
                        .get(country.name())
                        .is_some()
                    {
                        continue;
                    }
                    if city.country_id == country.id() {
                        if PARTIAL_MATCH_COUNTRIES_TO_OVERRIDE
                            .get_or_init(init_partial_match_countries_to_override)
                            .get(country.name())
                            .is_some()
                        {
                            return Ok(LocationMatchType::FullMatch {
                                city: city.id(),
                                state: city.state_id,
                                country: country.id(),
                            });
                        }
                        if let Some(unmatched_location_state) = find_state_by_id(city.state_id)? {
                            let unmatched_state_name =
                                normalize_location_str(unmatched_location_state.name());
                            if unmatched_state_name.contains(&state)
                                || state.contains(&unmatched_state_name)
                            {
                                debug!(
                                    "Partial name match: {} vs {}",
                                    state_in,
                                    unmatched_location_state.name()
                                );
                                return Ok(LocationMatchType::FullMatch {
                                    city: city.id(),
                                    state: city.state_id,
                                    country: country.id(),
                                });
                            }
                            if PARTIAL_MATCH_STATE_NAMES
                                .get_or_init(init_partial_match_state_names)
                                .get(&(&state, unmatched_state_name.as_str()))
                                .is_some()
                            {
                                debug!(
                                    "Partial name match: {} vs {}",
                                    state_in,
                                    unmatched_location_state.name()
                                );
                                return Ok(LocationMatchType::FullMatch {
                                    city: city.id(),
                                    state: city.state_id,
                                    country: country.id(),
                                });
                            }
                        }
                        return Ok(LocationMatchType::CityCountryMatch {
                            city: city.id(),
                            country: country.id(),
                            unmatched_state: city.state_id,
                        });
                    }
                }
            }
        }
    }

    Ok(LocationMatchType::NoMatch)
}

pub fn find_state_by_id(
    state_id: u64,
) -> Result<Option<&'static LocationState>, LocationFinderError> {
    Ok(STATE_ID_MAP
        .get()
        .ok_or(LocationFinderError::Loader)?
        .get(&state_id))
}
