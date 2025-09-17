use serde::Deserialize;
use config::{Config, ConfigError, File};

#[derive(Debug, Deserialize)]
pub struct DatabaseConfig {
    pub keymaxlength: u8,
    pub valuemaxlength: u16,
    pub directorypath: String,
    pub maxdatafilelength: u32,
}

fn load_config_file() -> Result<DatabaseConfig, ConfigError> {
    let settings = Config::builder()
        .add_source(File::with_name("config.json")) // Assuming a Config.json file
        .build()?;

    settings.try_deserialize()
}


pub fn load_config() -> Result<DatabaseConfig, ConfigError> {
    let _config = load_config_file()?;
    Ok(_config)
}


