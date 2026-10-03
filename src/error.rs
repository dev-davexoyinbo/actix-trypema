//! Configuration, key-extraction, and backend-failure types.

use std::{fmt, sync::Arc, time::Duration};

use actix_web::{HttpResponse, ResponseError, http::StatusCode};
use trypema::TrypemaError;

use crate::backend::Local;

/// An invalid middleware configuration, reported by `build()` and validated constructors.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConfigError {
    /// No namespace was configured.
    #[error("missing namespace: call namespace(..) before build()")]
    MissingNamespace,

    /// The namespace does not match `[a-z0-9-]{1,32}`.
    #[error("invalid namespace: {0}")]
    InvalidNamespace(String),

    /// No key extractor was configured.
    #[error("missing key extractor: call extractor(..) before build()")]
    MissingExtractor,

    /// No rate was configured.
    #[error("missing rate: call rate(..) or rate_fn(..) before build()")]
    MissingRate,

    /// A trusted-proxy entry is invalid.
    #[error("invalid trusted proxy: {0}")]
    InvalidTrustedProxy(String),

    /// A header name is invalid.
    #[error("invalid header name: {0}")]
    InvalidHeaderName(String),

    /// An IPv6 prefix length is outside `1..=128`.
    #[error("invalid ipv6 prefix length {0}: must be between 1 and 128")]
    InvalidIpv6PrefixLen(u8),

    /// A `BackendErrorPolicy::Fallback` backend uses a different window than the primary.
    #[error("fallback backend window must match the primary backend window")]
    FallbackWindowMismatch,

    /// A fixed rate admits less than one request per window, so every request would be
    /// rejected.
    #[error("rate admits less than one request per window: raise the rate or widen the window")]
    ZeroCapacity,

    /// The backend timeout is zero, so every backend call would fail.
    #[error("backend timeout must be greater than zero")]
    ZeroBackendTimeout,
}

/// A key-extraction failure, handled according to [`KeyErrorPolicy`].
///
/// With the default policy the error renders as `400 Bad Request` through
/// [`ResponseError`]; the response body never echoes request contents.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum KeyError {
    /// The request has no peer address (for example a test request without one).
    #[error("request has no peer address")]
    MissingPeerAddr,

    /// The configured key header is absent from the request.
    #[error("missing key header: {name}")]
    MissingHeader {
        /// The configured header name.
        name: Box<str>,
    },

    /// A trusted forwarding chain contained a malformed hop or exceeded the hop cap.
    #[error("malformed forwarding chain")]
    MalformedForwardedChain,

    /// A key value was unusable (wrong charset or over the length cap).
    #[error("invalid key value: {reason}")]
    InvalidValue {
        /// Why the value was rejected.
        reason: &'static str,
    },
}

impl ResponseError for KeyError {
    fn status_code(&self) -> StatusCode {
        StatusCode::BAD_REQUEST
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code())
            .content_type("text/plain; charset=utf-8")
            .body("invalid rate limiting key")
    }
} // end impl

/// What to do when key extraction fails.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum KeyErrorPolicy {
    /// Reject the request, rendering the [`KeyError`] through [`ResponseError`]. The default.
    #[default]
    Reject,
    /// Skip the limiter and forward the request.
    Bypass,
    /// Fall back to the namespace-wide shared key, as if [`Global`](crate::extract::Global)
    /// were the extractor.
    UseGlobalKey,
}

/// Why a Redis or hybrid backend call failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BackendFailure {
    /// The call exceeded the configured `backend_timeout`.
    #[error("backend call timed out after {0:?}")]
    Timeout(Duration),

    /// trypema reported an error, such as an unreachable Redis.
    #[error(transparent)]
    Error(TrypemaError),
}

/// What to do when a Redis or hybrid backend call fails or times out.
///
/// The default is [`FailOpen`](Self::FailOpen): a rate limiter outage should not become an API
/// outage. Security-sensitive routes such as login or OTP verification should use
/// [`FailClosed`](Self::FailClosed).
#[derive(Clone, Default)]
#[non_exhaustive]
pub enum BackendErrorPolicy {
    /// Admit the request. The default.
    #[default]
    FailOpen,
    /// Reject the request with `503 Service Unavailable`.
    FailClosed,
    /// Run the same admission check on a local backend, with the same derived key, rate, and
    /// cost. Each app instance then enforces its own quota while the backend is unavailable, so
    /// `n` instances admit up to `n` times the limit, and those counts are not carried back.
    /// The local backend must use the primary's window. Recorded outcomes report the local
    /// decision (`Admitted`, `Limited`), not `BackendError`.
    Fallback(Local),
    /// Decide per failure.
    Custom(Arc<dyn Fn(&BackendFailure) -> ErrorAction + Send + Sync>),
}

impl fmt::Debug for BackendErrorPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FailOpen => formatter.write_str("FailOpen"),
            Self::FailClosed => formatter.write_str("FailClosed"),
            Self::Fallback(_) => formatter.write_str("Fallback(..)"),
            Self::Custom(_) => formatter.write_str("Custom(..)"),
        }
    }
} // end impl

/// The decision a [`BackendErrorPolicy::Custom`] function returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorAction {
    /// Admit the request.
    Admit,
    /// Reject the request with the given status.
    Reject {
        /// The response status to use.
        status: StatusCode,
    },
}
