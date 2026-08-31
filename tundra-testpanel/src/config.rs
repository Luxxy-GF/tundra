use anyhow::Context;
use serde::Deserialize;
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

#[derive(Debug, Deserialize)]
pub struct PanelConfig {
    pub bind: SocketAddr,
    pub db_path: PathBuf,
    pub admin_token: String,
    pub data_dir: PathBuf,
    pub public_url: String,
}

impl PanelConfig {
    pub fn load(path: &Path) -> Result<Self, anyhow::Error> {
        let text =
            std::fs::read_to_string(path).context(format!("failed to read {}", path.display()))?;
        let config: Self =
            serde_norway::from_str(&text).context(format!("failed to parse {}", path.display()))?;

        if config.admin_token.len() < 16 {
            return Err(anyhow::anyhow!(
                "admin_token must be at least 16 characters"
            ));
        }

        Ok(config)
    }

    // used as a SAN on the self-signed HTTPS certificate
    #[inline]
    pub fn public_host(&self) -> Option<String> {
        let rest = self
            .public_url
            .split_once("://")
            .map_or(self.public_url.as_str(), |(_, r)| r);

        let host = rest.split('/').next()?;
        let host = match host.rsplit_once(':') {
            Some((h, port)) if port.chars().all(|c| c.is_ascii_digit()) => h,
            _ => host,
        };

        (!host.is_empty()).then(|| host.trim_matches(['[', ']']).into())
    }
}

pub fn config_path_from_args() -> Result<PathBuf, anyhow::Error> {
    let mut args = std::env::args().skip(1);
    let Some(flag) = args.next() else {
        return Err(anyhow::anyhow!("missing --config <path>"));
    };

    match flag.as_str() {
        "--config" | "-c" => {
            let Some(path) = args.next() else {
                return Err(anyhow::anyhow!("--config needs a path"));
            };

            Ok(PathBuf::from(path))
        }
        other => Err(anyhow::anyhow!(
            "unexpected argument {other:?}, expected --config <path>"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(public_url: &str) -> PanelConfig {
        PanelConfig {
            bind: "0.0.0.0:8443".parse().unwrap(),
            db_path: "/tmp/x.db".into(),
            admin_token: "0123456789abcdef".into(),
            data_dir: "/tmp".into(),
            public_url: public_url.into(),
        }
    }

    // PanelConfig::public_host

    #[test]
    fn public_host_is_extracted_from_assorted_public_urls() {
        assert_eq!(
            cfg("https://panel.example.net:8443").public_host().unwrap(),
            "panel.example.net"
        );
        assert_eq!(
            cfg("https://panel.example.net").public_host().unwrap(),
            "panel.example.net"
        );
        assert_eq!(
            cfg("https://10.0.0.5:8443/").public_host().unwrap(),
            "10.0.0.5"
        );
        assert_eq!(cfg("https://[::1]:8443").public_host().unwrap(), "::1");
        assert_eq!(
            cfg("panel.example.net:8443").public_host().unwrap(),
            "panel.example.net"
        );
    }
}
