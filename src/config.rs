//! Where the archive lives: R2 when `ARCHIVE_R2_ENDPOINT` is set, otherwise a local directory.
//!
//! Names follow spec section 8: the `archive.r2.*` keys in upper snake case as environment
//! variables, and the same names as flags. Reading the proxy's `config.yaml` is left to the
//! `vortex-writer-upload` ticket.

use std::fmt;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;

#[derive(Args, Debug, Clone)]
pub struct StoreArgs {
    /// Local directory to use as the archive root when no R2 endpoint is configured.
    #[arg(long, value_name = "DIR")]
    pub out: Option<PathBuf>,

    /// R2 S3 endpoint, e.g. https://<account-id>.r2.cloudflarestorage.com. Selects R2.
    #[arg(long, env = "ARCHIVE_R2_ENDPOINT")]
    pub r2_endpoint: Option<String>,

    #[arg(long, env = "ARCHIVE_R2_BUCKET")]
    pub r2_bucket: Option<String>,

    /// Key prefix inside the bucket (or the local root), without leading or trailing slash.
    #[arg(long, env = "ARCHIVE_R2_PREFIX")]
    pub r2_prefix: Option<String>,

    #[arg(long, env = "ARCHIVE_R2_ACCESS_KEY_ID", hide_env_values = true)]
    pub r2_access_key_id: Option<String>,

    #[arg(long, env = "ARCHIVE_R2_SECRET_ACCESS_KEY", hide_env_values = true)]
    pub r2_secret_access_key: Option<String>,

    /// Base64 of a 32-byte SSE-C key. SSE-C is off when absent.
    #[arg(long, env = "ARCHIVE_R2_SSE_C_KEY", hide_env_values = true)]
    pub r2_sse_c_key: Option<String>,
}

/// A resolved archive location. The prefix is applied in both cases.
pub enum StoreConfig {
    R2(R2Config),
    Local { root: PathBuf, prefix: String },
}

pub struct R2Config {
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub sse_c_key_base64: Option<String>,
}

// Hand-written so that credentials never reach a log line.
impl fmt::Debug for R2Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("R2Config")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("sse_c", &self.sse_c_key_base64.is_some())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for StoreConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (base, prefix) = match self {
            Self::R2(r2) => (
                format!("r2 {} bucket {}", r2.endpoint, r2.bucket),
                &r2.prefix,
            ),
            Self::Local { root, prefix } => (format!("local {}", root.display()), prefix),
        };
        write!(f, "{base} prefix {prefix:?}")?;
        if let Self::R2(r2) = self {
            let sse = if r2.sse_c_key_base64.is_some() {
                "on"
            } else {
                "off"
            };
            write!(f, " sse-c {sse}")?;
        }
        Ok(())
    }
}

impl StoreArgs {
    pub fn resolve(self) -> Result<StoreConfig> {
        let prefix = normalize_prefix(self.r2_prefix.as_deref().unwrap_or(""))?;
        let Some(endpoint) = self.r2_endpoint.filter(|e| !e.is_empty()) else {
            let root = self
                .out
                .context("no ARCHIVE_R2_ENDPOINT set: pass --out <dir> to use a local archive")?;
            return Ok(StoreConfig::Local { root, prefix });
        };
        let required = |value: Option<String>, name: &str| {
            value
                .filter(|v| !v.is_empty())
                .with_context(|| format!("{name} is required when ARCHIVE_R2_ENDPOINT is set"))
        };
        Ok(StoreConfig::R2(R2Config {
            endpoint,
            bucket: required(self.r2_bucket, "ARCHIVE_R2_BUCKET")?,
            prefix,
            access_key_id: required(self.r2_access_key_id, "ARCHIVE_R2_ACCESS_KEY_ID")?,
            secret_access_key: required(self.r2_secret_access_key, "ARCHIVE_R2_SECRET_ACCESS_KEY")?,
            sse_c_key_base64: self.r2_sse_c_key.filter(|k| !k.is_empty()),
        }))
    }
}

fn normalize_prefix(prefix: &str) -> Result<String> {
    if prefix.starts_with('/') || prefix.ends_with('/') {
        bail!("ARCHIVE_R2_PREFIX {prefix:?} must not start or end with '/'");
    }
    Ok(prefix.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> StoreArgs {
        StoreArgs {
            out: None,
            r2_endpoint: None,
            r2_bucket: None,
            r2_prefix: None,
            r2_access_key_id: None,
            r2_secret_access_key: None,
            r2_sse_c_key: None,
        }
    }

    #[test]
    fn local_needs_out_and_r2_needs_credentials() {
        assert!(args().resolve().is_err());
        let local = StoreArgs {
            out: Some("/tmp/a".into()),
            ..args()
        };
        assert!(matches!(
            local.resolve().unwrap(),
            StoreConfig::Local { .. }
        ));
        let r2 = StoreArgs {
            r2_endpoint: Some("https://x.r2.cloudflarestorage.com".into()),
            r2_bucket: Some("b".into()),
            ..args()
        };
        assert!(r2.resolve().is_err());
    }

    #[test]
    fn prefix_rejects_slashes_at_the_ends() {
        let local = |p: &str| StoreArgs {
            out: Some("/tmp/a".into()),
            r2_prefix: Some(p.into()),
            ..args()
        };
        assert!(local("/v1").resolve().is_err());
        assert!(local("v1/").resolve().is_err());
        assert!(local("v1/archive").resolve().is_ok());
    }

    #[test]
    fn debug_never_prints_secrets() {
        let r2 = R2Config {
            endpoint: "e".into(),
            bucket: "b".into(),
            prefix: String::new(),
            access_key_id: "AKID-SECRET".into(),
            secret_access_key: "SECRET".into(),
            sse_c_key_base64: Some("KEY-SECRET".into()),
        };
        let debug = format!("{r2:?}");
        let shown = format!("{debug} {}", StoreConfig::R2(r2));
        assert!(!shown.contains("SECRET"), "{shown}");
    }
}
