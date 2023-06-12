use clap::Parser;
use location_finder::location_records_loader::load_location_records;
use log::info;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    location_dataset_dir: Option<String>,
}

fn main() {
    simple_logger::init_with_env().unwrap();
    let args = Args::parse();
    if let Some(ref location_dataset_dir) = args.location_dataset_dir {
        info!("location_dataset_dir: {}", location_dataset_dir);
    }
    let res = load_location_records(args.location_dataset_dir);
    if res.is_err() {
        info!("Error loading location records: {:?}", res);
    }
}
