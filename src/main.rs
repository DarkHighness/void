pub(crate) mod config;
pub(crate) mod core;
pub(crate) mod utils;

use core::manager;
use std::{fmt::Arguments, path::PathBuf};

use config::Config;
use fern::colors::{Color, ColoredLevelConfig};
use log::{info, warn};
use miette::IntoDiagnostic;

fn setup_logger() -> std::result::Result<(), fern::InitError> {
    let colors = ColoredLevelConfig::new()
        .debug(Color::Cyan)
        .error(Color::Red)
        .warn(Color::Yellow)
        .info(Color::White)
        .trace(Color::Magenta);

    let make_formatter = |use_color: bool| {
        move |out: fern::FormatCallback, message: &Arguments, record: &log::Record| {
            let now = jiff::Zoned::now();
            let now = now.strftime("%Y-%m-%d %H:%M:%S");

            let target = record.target();
            let mut target = target.replacen("void", "app", 1);
            if let Some(line) = record.line() {
                target = format!("{}:{}", target, line);
            }
            let target = target;

            if use_color {
                out.finish(format_args!(
                    "[{} {} {}] {}",
                    now,
                    colors.color(record.level()),
                    target,
                    message
                ))
            } else {
                out.finish(format_args!(
                    "[{} {} {}] {}",
                    now,
                    record.level(),
                    target,
                    message
                ))
            }
        }
    };

    let log_level = std::env::var("RUST_LOG")
        .unwrap_or_else(|_| "info".to_string())
        .parse()
        .expect("Invalid log level");

    let file_dispatch = fern::Dispatch::new()
        .format(make_formatter(false))
        .level(log_level)
        .chain(fern::log_file("output.log")?);

    let stdout_dispatch = fern::Dispatch::new()
        .format(make_formatter(true))
        .level(log_level)
        .chain(std::io::stdout());

    fern::Dispatch::new()
        .chain(stdout_dispatch)
        .chain(file_dispatch)
        .apply()?;

    Ok(())
}

#[tokio::main]
pub async fn main() -> miette::Result<()> {
    console_subscriber::init();

    setup_logger().into_diagnostic()?;

    info!("Starting the application");
    let path = PathBuf::from("config.toml");
    let config = Config::load_from_file(&path)?;
    info!("Loaded config from {}", path.display());

    let ctx = tokio_util::sync::CancellationToken::new();
    let child_token = ctx.child_token();

    ctrlc::set_handler(move || {
        warn!("Received Ctrl+C, shutting down...");
        ctx.cancel();
    })
    .into_diagnostic()?;

    let mgr = manager::try_create_from_config(config)?;
    mgr.run(child_token).await?;

    info!("Application has exited");
    Ok(())
}
