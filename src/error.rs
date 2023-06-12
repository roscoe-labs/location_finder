use thiserror::Error;

#[derive(Error, Debug)]
pub enum LocationFinderError {
    #[error("Error parsing CSV file")]
    CSV(#[from] csv::Error),
    #[error("Error loading location records")]
    Loader,
}
