//! actix-web middleware for the [trypema](https://docs.rs/trypema) sliding-window rate limiter.
//!
//! The middleware admits or rejects requests using any trypema backend — local (in-process),
//! Redis (distributed), or hybrid — and either strategy (absolute allow/reject or probabilistic
//! suppression). Key extraction, rates, request cost, headers, rejection responses, and backend
//! failure handling are configurable per middleware instance.
//!
//! # Quick start
//!
//! ```
//! use actix_trypema::{Local, TrypemaLimiter, extract::PeerIp};
//! use actix_web::{App, web};
//! use trypema::{RateLimit, WindowSize};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Build the backend ONCE, outside HttpServer::new: inside the server closure every worker
//! // would get its own limiter, multiplying the effective limit by the worker count.
//! let backend = Local::new(WindowSize::seconds_or_panic(60))?;
//!
//! let limiter = TrypemaLimiter::builder(backend)
//!     .namespace("ip")
//!     .extractor(PeerIp::default())
//!     .rate(RateLimit::per_second_or_panic(10.0))
//!     .build()?;
//!
//! let app = App::new()
//!     .wrap(limiter.clone())
//!     .route("/", web::get().to(|| async { "hello" }));
//! # let _ = app;
//! # Ok(())
//! # }
//! ```
//!
//! # Middleware placement
//!
//! `App::wrap` runs middleware in reverse registration order: the last `.wrap(..)` runs first.
//! Put IP-keyed limiters outermost, before authentication, so unauthenticated floods are
//! limited. Put identity-keyed limiters (API key, user id) after the authentication layer that
//! validates the identity. An `exclude` predicate sees the path as this middleware receives it,
//! so register `NormalizePath` to run before the limiter when exclusions rely on normalized
//! paths.
//!
//! # Feature flags
//!
//! - `redis`: the Redis and hybrid backends, on trypema's tokio Redis client (the runtime
//!   actix-web runs on).
//! - `json`: a JSON rejection responder.
//! - `metrics`: decision counters through the `metrics` facade — every request counts one of
//!   admitted, rejected, or bypassed, and backend failures also count as errors — plus a
//!   backend-latency histogram.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod backend;
mod builder;
mod client_ip;
mod error;
pub mod extract;
mod key;
mod middleware;
mod outcome;
mod response;

#[cfg(feature = "redis")]
pub use backend::{AsyncBackend, Hybrid, Redis};
pub use backend::{Backend, Local};
pub use builder::{HeaderMode, Strategy, TrypemaLimiter, TrypemaLimiterBuilder};
pub use error::{
    BackendErrorPolicy, BackendFailure, ConfigError, ErrorAction, KeyError, KeyErrorPolicy,
};
pub use key::derived_key;
pub use middleware::{LocalFuture, TrypemaMiddleware};
pub use outcome::{LimitOutcome, RateLimitInfo};
#[cfg(feature = "json")]
pub use response::JsonResponder;
pub use response::{DefaultResponder, RejectionInfo, RejectionResponder};

#[cfg(test)]
mod tests;
