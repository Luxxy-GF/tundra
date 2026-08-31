use anyhow::Context;
use axum_server::tls_rustls::RustlsConfig;
use config::PanelConfig;
use database::Database;
use keys::PanelKeys;
use routes::AppState;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;
use websocket::Hub;

mod config;
mod database;
mod keys;
mod routes;
mod websocket;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .init();

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .ok();

    let config = PanelConfig::load(&config::config_path_from_args()?)?;

    let mut sans = vec!["localhost".into(), "127.0.0.1".into()];
    sans.extend(config.public_host());
    sans.dedup();

    let keys = Arc::new(PanelKeys::load_or_create(&config.data_dir, &sans)?);
    let database = Arc::new(Database::open(&config.db_path)?);

    tracing::info!(
        fingerprint = %keys.https_fingerprint,
        "panel https certificate, set this as remote.cert_sha256 in every node config"
    );
    tracing::info!(
        jwt_pubkey = %keys.jwt_public(),
        epoch = database.epoch()?,
        "panel identity"
    );

    let state = Arc::new(AppState {
        database,
        keys: Arc::clone(&keys),
        hub: Arc::new(Hub::new()),
        admin_token: config.admin_token.clone().into(),
    });

    let tls = RustlsConfig::from_der(
        vec![keys.https_cert_der.clone()],
        keys.https_key_der.clone(),
    )
    .await
    .context("failed to build the panel TLS configuration")?;

    tracing::info!(
        bind = %config.bind,
        public_url = %config.public_url,
        "panel listening"
    );

    axum_server::bind_rustls(config.bind, tls)
        .serve(routes::router(&state).into_make_service())
        .await
        .context("failed to serve the panel API")
}
