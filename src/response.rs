//! Rejection responses and rate limit headers.

use std::{sync::Arc, time::Duration};

use actix_web::{
    HttpResponse, HttpResponseBuilder,
    http::{
        StatusCode,
        header::{HeaderMap, HeaderName, HeaderValue, RETRY_AFTER, TryIntoHeaderValue},
    },
};

use crate::builder::{Config, HeaderMode, Strategy};

/// Everything a rejection response can say about why it happened.
///
/// Passed to the [`RejectionResponder`] and inserted into the rejection response's extensions,
/// where outer middleware such as `ErrorHandlers` can observe it.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RejectionInfo {
    /// The rejecting instance's namespace.
    pub namespace: Arc<str>,
    /// The derived limiter key (see [`derived_key`](crate::derived_key)); hand it to the
    /// provider to inspect or reset the offender.
    pub key: Arc<str>,
    /// The strategy that rejected.
    pub strategy: Strategy,
    /// The advertised per-window capacity.
    pub limit: u64,
    /// Best-effort wait until capacity frees up; `None` when no capacity will free up or the
    /// suppressed strategy has no configured hint.
    pub retry_after: Option<Duration>,
    /// Capacity that frees up after waiting, when known.
    pub remaining_after_waiting: Option<u64>,
    /// The suppression factor, for suppressed rejections.
    pub suppression_factor: Option<f64>,
}

/// Builds rejection responses.
///
/// The builder arrives as `429 Too Many Requests` with `retry-after` and the rate limit headers
/// already set; add a body, and change the status if needed.
///
/// # Examples
///
/// ```
/// use actix_trypema::{RejectionInfo, RejectionResponder};
/// use actix_web::{HttpResponse, HttpResponseBuilder, http::StatusCode};
///
/// /// Sheds load with 503 instead of 429.
/// struct ShedResponder;
///
/// impl RejectionResponder for ShedResponder {
///     fn respond(&self, info: &RejectionInfo, mut builder: HttpResponseBuilder) -> HttpResponse {
///         builder
///             .status(StatusCode::SERVICE_UNAVAILABLE)
///             .body(format!("{} is overloaded", info.namespace))
///     }
/// }
/// ```
pub trait RejectionResponder: Send + Sync + 'static {
    /// Produce the rejection response.
    fn respond(&self, info: &RejectionInfo, builder: HttpResponseBuilder) -> HttpResponse;
}

/// The default responder: `text/plain`, `Too many requests, retry in {n}s`.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultResponder;

impl RejectionResponder for DefaultResponder {
    fn respond(&self, info: &RejectionInfo, mut builder: HttpResponseBuilder) -> HttpResponse {
        let body = match info.retry_after.map(retry_after_secs) {
            Some(secs) => format!("Too many requests, retry in {secs}s"),
            None => "Too many requests".to_string(),
        };

        builder.content_type("text/plain; charset=utf-8").body(body)
    }
} // end impl

/// A JSON responder: `{"error": "rate_limited", "namespace", "retry_after_seconds"}`.
#[cfg(feature = "json")]
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonResponder;

#[cfg(feature = "json")]
impl RejectionResponder for JsonResponder {
    fn respond(&self, info: &RejectionInfo, mut builder: HttpResponseBuilder) -> HttpResponse {
        builder.json(serde_json::json!({
            "error": "rate_limited",
            "namespace": &*info.namespace,
            "retry_after_seconds": info.retry_after.map(retry_after_secs),
        }))
    }
} // end impl

/// Build the rejection response: status, `retry-after`, rate limit headers, the responder's
/// body, and the [`RejectionInfo`] extension.
pub(crate) fn build_rejection(config: &Config, info: &RejectionInfo) -> HttpResponse {
    let mut builder = HttpResponse::build(StatusCode::TOO_MANY_REQUESTS);

    if let Some(retry_after) = info.retry_after {
        builder.insert_header((
            RETRY_AFTER,
            HeaderValue::from(retry_after_secs(retry_after)),
        ));
    }

    match config.header_mode {
        HeaderMode::Off => {}
        HeaderMode::Legacy => {
            builder.insert_header((
                HeaderName::from_static("x-ratelimit-limit"),
                HeaderValue::from(info.limit),
            ));

            if let Some(factor) = info.suppression_factor {
                builder.insert_header((
                    HeaderName::from_static("x-ratelimit-suppression"),
                    format!("{factor:.3}"),
                ));
            }
        }
        HeaderMode::Ietf => {
            let window_secs = config.window_size.as_seconds();
            builder.insert_header((
                HeaderName::from_static("ratelimit-policy"),
                ietf_policy(&info.namespace, info.limit, window_secs),
            ));

            // Without a retry hint the full window is the only honest reset.
            let reset_secs = info.retry_after.map_or(window_secs, retry_after_secs);
            builder.insert_header((
                HeaderName::from_static("ratelimit"),
                ietf_ratelimit(&info.namespace, 0, reset_secs),
            ));
        }
    }

    let mut response = config.responder.respond(info, builder);
    response.extensions_mut().insert(info.clone());

    tracing::debug!(
        namespace = &*info.namespace,
        strategy = info.strategy.as_str(),
        retry_after_secs = info.retry_after.map(retry_after_secs),
        "request rejected"
    );

    response
} // end fn build_rejection

/// Headers added to an admitted response once the handler has produced it.
pub(crate) struct AdmitHeaders {
    mode: HeaderMode,
    namespace: Arc<str>,
    limit: u64,
    window_secs: u64,
    remaining: Option<u64>,
    suppression: Option<f64>,
}

impl AdmitHeaders {
    /// The headers for an admitted request, or `None` when headers are off.
    pub(crate) fn new(
        config: &Config,
        limit: u64,
        remaining: Option<u64>,
        suppression: Option<f64>,
    ) -> Option<Self> {
        if config.header_mode == HeaderMode::Off {
            return None;
        }

        Some(Self {
            mode: config.header_mode,
            namespace: Arc::clone(&config.namespace),
            limit,
            window_secs: config.window_size.as_seconds(),
            remaining,
            suppression,
        })
    } // end constructor

    /// Add the headers without overwriting anything the handler already set.
    pub(crate) fn apply(self, headers: &mut HeaderMap) {
        match self.mode {
            // `AdmitHeaders::new` returns `None` for `Off`, so there is never a value to apply.
            HeaderMode::Off => debug_assert!(false, "AdmitHeaders built with headers off"),
            HeaderMode::Legacy => {
                insert_if_absent(headers, "x-ratelimit-limit", HeaderValue::from(self.limit));

                if let Some(remaining) = self.remaining {
                    insert_if_absent(
                        headers,
                        "x-ratelimit-remaining",
                        HeaderValue::from(remaining),
                    );
                }

                if let Some(factor) = self.suppression {
                    insert_if_absent(headers, "x-ratelimit-suppression", format!("{factor:.3}"));
                }
            }
            HeaderMode::Ietf => {
                insert_if_absent(
                    headers,
                    "ratelimit-policy",
                    ietf_policy(&self.namespace, self.limit, self.window_secs),
                );

                // The draft makes `r` mandatory, so `ratelimit` needs the remaining read.
                if let Some(remaining) = self.remaining {
                    insert_if_absent(
                        headers,
                        "ratelimit",
                        ietf_ratelimit(&self.namespace, remaining, self.window_secs),
                    );
                }
            }
        }
    } // end method apply
} // end impl

// Callers pass numbers as `HeaderValue::from(n)` (digits written once, never validated) and text
// as a `String` (its buffer is handed over), so no value is formatted and then parsed again.
fn insert_if_absent(headers: &mut HeaderMap, name: &'static str, value: impl TryIntoHeaderValue) {
    let name = HeaderName::from_static(name);

    if !headers.contains_key(&name)
        && let Ok(value) = value.try_into_value()
    {
        headers.insert(name, value);
    }
} // end fn insert_if_absent

fn ietf_policy(namespace: &str, limit: u64, window_secs: u64) -> String {
    format!("\"{namespace}\";q={limit};w={window_secs}")
}

fn ietf_ratelimit(namespace: &str, remaining: u64, reset_secs: u64) -> String {
    format!("\"{namespace}\";r={remaining};t={reset_secs}")
}

/// A trypema retry hint, or `None` when it is zero: no live capacity will free up (for example
/// a cost above the whole capacity), so advertising a wait would be a lie.
pub(crate) fn nonzero_duration(duration: Duration) -> Option<Duration> {
    (!duration.is_zero()).then_some(duration)
}

/// Whole seconds, rounded up, minimum 1, saturating at `u64::MAX` for an enormous configured
/// hint. Callers pass only nonzero durations.
pub(crate) fn retry_after_secs(duration: Duration) -> u64 {
    duration
        .as_secs()
        .saturating_add(u64::from(duration.subsec_nanos() > 0))
        .max(1)
}
