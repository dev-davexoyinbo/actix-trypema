//! Per-request outcomes that handlers can read.

use std::{
    fmt,
    future::{Ready, ready},
    sync::{Arc, Once},
    time::Duration,
};

use actix_web::{
    Error, FromRequest, HttpMessage, HttpRequest,
    dev::{Payload, ServiceRequest},
};

/// What one middleware instance decided for the current request.
///
/// Recorded when the limiter is built with
/// [`record_outcome(true)`](crate::TrypemaLimiterBuilder::record_outcome) or in
/// [`permissive`](crate::TrypemaLimiterBuilder::permissive) (shadow) mode; read it in handlers
/// through [`RateLimitInfo`].
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum LimitOutcome {
    /// The request was admitted.
    #[non_exhaustive]
    Admitted {
        /// The advertised per-window capacity.
        limit: u64,
        /// Remaining capacity, when `remaining_header(true)` made it available.
        remaining: Option<u64>,
    },
    /// The limiter rejected the request; in permissive mode it was forwarded anyway.
    #[non_exhaustive]
    Limited {
        /// Best-effort wait until capacity frees up.
        retry_after: Option<Duration>,
    },
    /// The suppressed strategy decided this request probabilistically.
    #[non_exhaustive]
    Suppressed {
        /// Current suppression factor (0.0 = none, 1.0 = full).
        factor: f64,
        /// Whether the limiter admitted this call.
        admitted: bool,
    },
    /// The request bypassed the limiter: excluded, allow-listed, cost 0, or a key-extraction
    /// failure under [`KeyErrorPolicy::Bypass`](crate::KeyErrorPolicy::Bypass) or in permissive
    /// mode.
    Bypassed,
    /// The backend failed and the configured policy admitted the request.
    BackendError,
}

/// Recorded outcomes in recording order: outermost instance first. Never empty.
#[derive(Clone)]
struct Outcomes(Vec<(Arc<str>, LimitOutcome)>);

/// Record `outcome` for the instance named `namespace` on the request.
pub(crate) fn record(req: &ServiceRequest, namespace: &Arc<str>, outcome: LimitOutcome) {
    let entry = (Arc::clone(namespace), outcome);
    let mut extensions = req.extensions_mut();

    match extensions.get_mut::<Outcomes>() {
        Some(outcomes) => outcomes.0.push(entry),
        None => {
            extensions.insert(Outcomes(vec![entry]));
        }
    }
} // end fn record

/// Extractor giving handlers the [`LimitOutcome`]s recorded for the request.
///
/// Reading it consumes nothing, so it can be extracted more than once. Extraction fails with
/// `500 Internal Server Error` when no instance recorded an outcome — the route is not wrapped,
/// or neither `record_outcome(true)` nor permissive mode is set. The response body stays
/// generic; the cause is logged once at `warn`.
///
/// # Examples
///
/// ```
/// use actix_trypema::{LimitOutcome, RateLimitInfo};
/// use actix_web::{HttpResponse, Responder};
///
/// async fn handler(info: RateLimitInfo) -> impl Responder {
///     match info.outcome() {
///         LimitOutcome::Limited { .. } => HttpResponse::Ok().body("degraded"),
///         _ => HttpResponse::Ok().body("full"),
///     }
/// }
/// ```
#[derive(Clone)]
pub struct RateLimitInfo {
    outcomes: Outcomes,
}

impl RateLimitInfo {
    /// The most recently recorded outcome — from the innermost instance.
    pub fn outcome(&self) -> &LimitOutcome {
        let (_, outcome) = self
            .outcomes
            .0
            .last()
            .expect("recorded outcomes are never empty");
        outcome
    }

    /// The outcome recorded by the instance with this namespace, when several are stacked.
    pub fn for_namespace(&self, namespace: &str) -> Option<&LimitOutcome> {
        self.outcomes
            .0
            .iter()
            .find(|(recorded, _)| &**recorded == namespace)
            .map(|(_, outcome)| outcome)
    }
} // end impl

impl fmt::Debug for RateLimitInfo {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_map()
            .entries(
                self.outcomes
                    .0
                    .iter()
                    .map(|(namespace, outcome)| (namespace, outcome)),
            )
            .finish()
    }
} // end impl

impl FromRequest for RateLimitInfo {
    type Error = Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        let outcomes = req.extensions().get::<Outcomes>().cloned();

        let Some(outcomes) = outcomes else {
            // A configuration mistake, not a client error: explain it to the operator once, and
            // keep the configuration out of the response body.
            static WARN_ONCE: Once = Once::new();
            WARN_ONCE.call_once(|| {
                tracing::warn!(
                    "RateLimitInfo extracted with no outcome recorded: wrap the route in a \
                     TrypemaLimiter built with record_outcome(true) or permissive(true)"
                );
            });

            return ready(Err(actix_web::error::ErrorInternalServerError(
                "rate limit outcome unavailable",
            )));
        };

        ready(Ok(Self { outcomes }))
    } // end method from_request
} // end impl
