#[cfg(target_os = "android")]
extern crate android_logger;

#[cfg(target_os = "android")]
use android_logger::Config;
use log::{self, SetLoggerError};
use std::env;

/// Attempt to init a logger for MMTk.
pub fn try_init() -> Result<(), SetLoggerError> {
    env::set_var("RUST_BACKTRACE", "1");
    #[cfg(target_os = "android")]
    {
        let tag = if cfg!(target_pointer_width = "32") {
            "mmtk-art32"
        } else {
            "mmtk-art64"
        };
        android_logger::init_once(
            Config::default()
                .with_max_level(log::LevelFilter::Info)
                .with_tag(tag),
        );
        return Ok(());
    }
    #[cfg(not(target_os = "android"))]
    {
        return env_logger::try_init_from_env(
            // By default, use info level logging.
            env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
        );
    }
}
