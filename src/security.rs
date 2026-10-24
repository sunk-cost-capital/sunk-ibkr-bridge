//! Origin, host, request-signature, and replay validation.

use crate::contract::PROTOCOL_VERSION;

use axum::http::{HeaderMap, Uri};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use url::{Host, Url};

type HmacSha256 = Hmac<Sha256>;

pub(crate) const HEADER_NONCE: &str = "x-sunk-bridge-nonce";
pub(crate) const HEADER_SIGNATURE: &str = "x-sunk-bridge-signature";
pub(crate) const HEADER_TIMESTAMP: &str = "x-sunk-bridge-timestamp";
pub(crate) const HEADER_VERSION: &str = "x-sunk-bridge-version";
pub(crate) const WEBSOCKET_PROTOCOL: &str = "sunk-ibkr-v3";
/// Shared prefix of every versioned bridge WebSocket subprotocol.
const WEBSOCKET_PROTOCOL_PREFIX: &str = "sunk-ibkr-v";
const WEBSOCKET_TIMESTAMP_PREFIX: &str = "timestamp-";
const WEBSOCKET_NONCE_PREFIX: &str = "nonce-";
const WEBSOCKET_SIGNATURE_PREFIX: &str = "signature-";
const MAX_CLOCK_SKEW: Duration = Duration::from_secs(30);
const NONCE_RETENTION: Duration = Duration::from_secs(90);

/// Authentication and browser-origin configuration for the bridge.
#[derive(Clone)]
pub struct SecurityConfig {
    token: Arc<[u8]>,
    allowed_origins: Arc<HashSet<String>>,
}

impl std::fmt::Debug for SecurityConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecurityConfig")
            .field("token", &"<redacted>")
            .field("allowed_origins", &self.allowed_origins)
            .finish()
    }
}

impl SecurityConfig {
    /// Validate a shared token and exact browser-origin allowlist.
    ///
    /// # Errors
    ///
    /// Returns an error for short tokens, non-HTTP origins, or origins that
    /// include credentials, paths, queries, or fragments.
    pub fn new(token: String, allowed_origins: Vec<String>) -> Result<Self, SecurityError> {
        if token.len() < 32 {
            return Err(SecurityError::Configuration(
                "bridge token must contain at least 32 bytes".to_string(),
            ));
        }
        if allowed_origins.is_empty() {
            return Err(SecurityError::Configuration(
                "at least one allowed web origin is required".to_string(),
            ));
        }
        let mut normalized = HashSet::new();
        for origin in allowed_origins {
            let parsed = Url::parse(&origin).map_err(|error| {
                SecurityError::Configuration(format!("invalid allowed origin {origin}: {error}"))
            })?;
            if !matches!(parsed.scheme(), "http" | "https")
                || !parsed.username().is_empty()
                || parsed.password().is_some()
                || parsed.path() != "/"
                || parsed.query().is_some()
                || parsed.fragment().is_some()
            {
                return Err(SecurityError::Configuration(format!(
                    "allowed origin must contain only an HTTP(S) origin: {origin}"
                )));
            }
            normalized.insert(parsed.origin().ascii_serialization());
        }
        Ok(Self {
            token: Arc::from(token.into_bytes()),
            allowed_origins: Arc::new(normalized),
        })
    }
}

#[derive(Debug)]
pub(crate) struct SecurityState {
    config: SecurityConfig,
    seen_nonces: Mutex<HashMap<String, Instant>>,
}

impl SecurityState {
    pub(crate) fn new(config: SecurityConfig) -> Self {
        Self {
            config,
            seen_nonces: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn validate_origin<'a>(
        &self,
        headers: &'a HeaderMap,
    ) -> Result<&'a str, SecurityError> {
        let origin = required_header(headers, "origin")?;
        if !self.config.allowed_origins.contains(origin) {
            return Err(SecurityError::Origin);
        }
        Ok(origin)
    }

    pub(crate) fn validate_host(&self, headers: &HeaderMap) -> Result<(), SecurityError> {
        let authority = required_header(headers, "host")?;
        let url = Url::parse(&format!("http://{authority}/")).map_err(|_| SecurityError::Host)?;
        match url.host() {
            Some(Host::Domain("localhost")) => Ok(()),
            Some(Host::Ipv4(address)) if address.is_loopback() => Ok(()),
            Some(Host::Ipv6(address)) if address.is_loopback() => Ok(()),
            _ => Err(SecurityError::Host),
        }
    }

    pub(crate) async fn authenticate(
        &self,
        headers: &HeaderMap,
        uri: &Uri,
        body: &[u8],
    ) -> Result<String, SecurityError> {
        self.validate_host(headers)?;
        let origin = self.validate_origin(headers)?;
        let requested = required_header(headers, HEADER_VERSION)?.parse::<u8>().ok();
        if requested != Some(PROTOCOL_VERSION) {
            return Err(SecurityError::Version { requested });
        }
        let timestamp = required_header(headers, HEADER_TIMESTAMP)?;
        let nonce = required_header(headers, HEADER_NONCE)?;
        let signature = required_header(headers, HEADER_SIGNATURE)?;
        let body_hash = hex::encode(Sha256::digest(body));
        self.authenticate_parts(
            "POST",
            uri.path(),
            origin,
            timestamp,
            nonce,
            signature,
            &body_hash,
        )
        .await?;
        Ok(origin.to_string())
    }

    pub(crate) async fn authenticate_websocket(
        &self,
        headers: &HeaderMap,
        path: &str,
    ) -> Result<String, SecurityError> {
        self.validate_host(headers)?;
        let origin = self.validate_origin(headers)?;
        let protocols = required_header(headers, "sec-websocket-protocol")?;
        websocket_protocol_version(protocols)?;
        let timestamp = websocket_protocol_value(protocols, WEBSOCKET_TIMESTAMP_PREFIX)?;
        let nonce = websocket_protocol_value(protocols, WEBSOCKET_NONCE_PREFIX)?;
        let signature = websocket_protocol_value(protocols, WEBSOCKET_SIGNATURE_PREFIX)?;
        let body_hash = hex::encode(Sha256::digest([]));
        self.authenticate_parts("GET", path, origin, timestamp, nonce, signature, &body_hash)
            .await?;
        Ok(origin.to_string())
    }

    #[allow(clippy::too_many_arguments)]
    async fn authenticate_parts(
        &self,
        method: &str,
        path: &str,
        origin: &str,
        timestamp: &str,
        nonce: &str,
        signature: &str,
        body_hash: &str,
    ) -> Result<(), SecurityError> {
        let timestamp_ms = timestamp
            .parse::<u64>()
            .map_err(|_| SecurityError::Timestamp)?;
        validate_timestamp(timestamp_ms)?;
        if nonce.len() != 32 || !nonce.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(SecurityError::Nonce);
        }
        let signature_bytes = hex::decode(signature).map_err(|_| SecurityError::Signature)?;
        let canonical = format!(
            "v{PROTOCOL_VERSION}\n{method}\n{path}\n{origin}\n{timestamp}\n{nonce}\n{body_hash}"
        );
        let mut hmac =
            HmacSha256::new_from_slice(&self.config.token).map_err(|_| SecurityError::Signature)?;
        hmac.update(canonical.as_bytes());
        hmac.verify_slice(&signature_bytes)
            .map_err(|_| SecurityError::Signature)?;

        let now = Instant::now();
        let mut seen = self.seen_nonces.lock().await;
        seen.retain(|_, inserted| now.duration_since(*inserted) <= NONCE_RETENTION);
        if seen.insert(nonce.to_ascii_lowercase(), now).is_some() {
            return Err(SecurityError::Replay);
        }
        Ok(())
    }
}

/// Require exactly one versioned bridge subprotocol and that it is this one.
///
/// A peer offering a different `sunk-ibkr-v<N>` is reported as a version
/// mismatch rather than malformed authentication, so an outdated peer gets an
/// actionable message instead of a generic failure.
fn websocket_protocol_version(protocols: &str) -> Result<(), SecurityError> {
    let mut versions = protocols
        .split(',')
        .map(str::trim)
        .filter_map(|protocol| protocol.strip_prefix(WEBSOCKET_PROTOCOL_PREFIX));
    let version = versions.next().ok_or(SecurityError::WebSocketProtocol)?;
    if versions.next().is_some() {
        return Err(SecurityError::WebSocketProtocol);
    }
    let requested = version.parse::<u8>().ok();
    if requested != Some(PROTOCOL_VERSION) {
        return Err(SecurityError::Version { requested });
    }
    Ok(())
}

fn websocket_protocol_value<'a>(
    protocols: &'a str,
    prefix: &'static str,
) -> Result<&'a str, SecurityError> {
    let mut matches = protocols
        .split(',')
        .map(str::trim)
        .filter_map(|protocol| protocol.strip_prefix(prefix));
    let value = matches.next().ok_or(SecurityError::WebSocketProtocol)?;
    if value.is_empty() || matches.next().is_some() {
        return Err(SecurityError::WebSocketProtocol);
    }
    Ok(value)
}

/// Explain a protocol mismatch in terms of which side must be updated.
///
/// Every version bump is incompatible in both directions, so the message
/// names the side that is behind: an older request means the web app is stale,
/// a newer one means this bridge is.
///
/// # Arguments
///
/// * `requested` - Version the peer asked for, or none when it was unreadable.
///
/// # Returns
///
/// A user-facing message that names the version each side speaks.
pub(crate) fn protocol_version_mismatch_message(requested: Option<u8>) -> String {
    match requested {
        Some(requested) if requested < PROTOCOL_VERSION => format!(
            "This IBKR bridge speaks protocol v{PROTOCOL_VERSION}, but the request used v{requested}. \
             Reload the Sunk web app so both sides match."
        ),
        Some(requested) => format!(
            "This IBKR bridge speaks protocol v{PROTOCOL_VERSION}, but the web app needs v{requested}. \
             Update the IBKR bridge and restart it."
        ),
        None => format!(
            "This IBKR bridge speaks protocol v{PROTOCOL_VERSION} and could not read the request's \
             protocol version."
        ),
    }
}

fn required_header<'a>(
    headers: &'a HeaderMap,
    name: &'static str,
) -> Result<&'a str, SecurityError> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .ok_or(SecurityError::MissingHeader(name))
}

fn validate_timestamp(timestamp_ms: u64) -> Result<(), SecurityError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SecurityError::Timestamp)?;
    let timestamp = Duration::from_millis(timestamp_ms);
    let difference = now.abs_diff(timestamp);
    if difference > MAX_CLOCK_SKEW {
        return Err(SecurityError::Timestamp);
    }
    Ok(())
}

/// Request-security configuration or validation error.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SecurityError {
    /// Bridge configuration is unsafe or incomplete.
    Configuration(String),
    /// A required request header is absent or not valid UTF-8.
    MissingHeader(&'static str),
    /// Browser origin is not explicitly allowed.
    Origin,
    /// HTTP Host does not resolve to a loopback name or address.
    Host,
    /// Protocol version is unsupported.
    Version {
        /// Version the peer asked for, or none when it was not a version number.
        requested: Option<u8>,
    },
    /// Timestamp is malformed or outside the replay window.
    Timestamp,
    /// Nonce is malformed.
    Nonce,
    /// HMAC signature is invalid.
    Signature,
    /// Nonce has already been accepted.
    Replay,
    /// WebSocket subprotocol authentication values are missing or ambiguous.
    WebSocketProtocol,
}

impl Display for SecurityError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Configuration(message) => formatter.write_str(message),
            Self::MissingHeader(name) => write!(formatter, "missing required {name} header"),
            Self::Origin => formatter.write_str("browser origin is not allowed"),
            Self::Host => formatter.write_str("bridge Host must be loopback"),
            Self::Version { requested } => {
                formatter.write_str(&protocol_version_mismatch_message(*requested))
            }
            Self::Timestamp => formatter.write_str("bridge request timestamp is invalid or stale"),
            Self::Nonce => formatter.write_str("bridge request nonce is invalid"),
            Self::Signature => formatter.write_str("bridge request signature is invalid"),
            Self::Replay => formatter.write_str("bridge request nonce was already used"),
            Self::WebSocketProtocol => {
                formatter.write_str("bridge WebSocket authentication is invalid")
            }
        }
    }
}

impl std::error::Error for SecurityError {}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn config() -> SecurityConfig {
        SecurityConfig::new(
            "0123456789abcdef0123456789abcdef".to_string(),
            vec!["https://sunkcostcapital.com".to_string()],
        )
        .expect("valid security fixture")
    }

    #[test]
    fn rejects_non_origin_allowlist_values() {
        assert!(
            SecurityConfig::new(
                "0123456789abcdef0123456789abcdef".to_string(),
                vec!["https://sunkcostcapital.com/path".to_string()],
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_non_loopback_host() {
        let state = SecurityState::new(config());
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_static("192.168.1.5:17654"));
        assert_eq!(state.validate_host(&headers), Err(SecurityError::Host));
    }

    #[test]
    fn selects_exactly_one_current_websocket_protocol_version() {
        assert_eq!(websocket_protocol_version("sunk-ibkr-v3, nonce-00"), Ok(()));
        assert_eq!(
            websocket_protocol_version("sunk-ibkr-v2, nonce-00"),
            Err(SecurityError::Version { requested: Some(2) })
        );
        assert_eq!(
            websocket_protocol_version("sunk-ibkr-vx"),
            Err(SecurityError::Version { requested: None })
        );
        assert_eq!(
            websocket_protocol_version("sunk-ibkr-v2, sunk-ibkr-v3"),
            Err(SecurityError::WebSocketProtocol)
        );
        assert_eq!(
            websocket_protocol_version("nonce-00"),
            Err(SecurityError::WebSocketProtocol)
        );
    }

    #[test]
    fn names_the_side_that_must_update() {
        assert!(protocol_version_mismatch_message(Some(2)).contains("Reload the Sunk web app"));
        assert!(protocol_version_mismatch_message(Some(4)).contains("Update the IBKR bridge"));
        assert_eq!(
            SecurityError::Version { requested: Some(2) }.to_string(),
            protocol_version_mismatch_message(Some(2))
        );
    }

    #[tokio::test]
    async fn authenticates_websocket_protocols_once() {
        let state = SecurityState::new(config());
        let origin = "https://sunkcostcapital.com";
        let path = "/v1/client-exchange/stream";
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();
        let nonce = "0123456789abcdef0123456789abcdef";
        let body_hash = hex::encode(Sha256::digest([]));
        let canonical = format!(
            "v{PROTOCOL_VERSION}\nGET\n{path}\n{origin}\n{timestamp}\n{nonce}\n{body_hash}"
        );
        let mut hmac =
            HmacSha256::new_from_slice(b"0123456789abcdef0123456789abcdef").expect("test token");
        hmac.update(canonical.as_bytes());
        let signature = hex::encode(hmac.finalize().into_bytes());
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_static("127.0.0.1:17654"));
        headers.insert("origin", HeaderValue::from_static(origin));
        headers.insert(
            "sec-websocket-protocol",
            HeaderValue::from_str(&format!(
                "{WEBSOCKET_PROTOCOL}, {WEBSOCKET_TIMESTAMP_PREFIX}{timestamp}, {WEBSOCKET_NONCE_PREFIX}{nonce}, {WEBSOCKET_SIGNATURE_PREFIX}{signature}"
            ))
            .unwrap(),
        );

        let mut ambiguous_headers = headers.clone();
        ambiguous_headers.insert(
            "sec-websocket-protocol",
            HeaderValue::from_str(&format!(
                "{WEBSOCKET_PROTOCOL}, {WEBSOCKET_TIMESTAMP_PREFIX}{timestamp}, {WEBSOCKET_TIMESTAMP_PREFIX}{timestamp}, {WEBSOCKET_NONCE_PREFIX}{nonce}, {WEBSOCKET_SIGNATURE_PREFIX}{signature}"
            ))
            .unwrap(),
        );
        assert_eq!(
            state.authenticate_websocket(&ambiguous_headers, path).await,
            Err(SecurityError::WebSocketProtocol)
        );

        assert_eq!(
            state.authenticate_websocket(&headers, path).await,
            Ok(origin.to_string())
        );
        assert_eq!(
            state.authenticate_websocket(&headers, path).await,
            Err(SecurityError::Replay)
        );
    }
}
