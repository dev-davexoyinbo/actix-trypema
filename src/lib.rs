//! actix-web middleware for the [trypema](https://docs.rs/trypema) sliding-window rate limiter.
//!
//! One middleware covers every trypema backend: local (in process), Redis (exact limits shared
//! across instances) and hybrid (shared through a local fast path), with either strategy
//! (absolute allow/reject or probabilistic suppression). The backend is trypema's own provider
//! or limiter, in an `Arc`. Keys, rates, request costs, headers, rejection responses and backend
//! failure handling are set per middleware instance.
//!
//! This crate re-exports [`trypema`], so `actix_trypema::trypema::RateLimiterBuilder` is the
//! trypema version this crate uses.
//!
//! The [actix-web guide](https://trypema.davidoyinbo.com/actix-web) covers every option with
//! examples.
//!
//! # Quick start
//!
//! ```
//! use std::sync::Arc;
//!
//! use actix_trypema::{TrypemaLimiter, extract::PeerIp};
//! use actix_trypema::trypema::{RateLimit, RateLimiterBuilder, WindowSize, local::AbsoluteLocalRateLimiter};
//! use actix_web::{App, web};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Build the backend ONCE, outside HttpServer::new: inside the server closure every worker
//! // would get its own limiter, multiplying the effective limit by the worker count.
//! let backend: Arc<AbsoluteLocalRateLimiter> = RateLimiterBuilder::local()
//!     .window_size(WindowSize::seconds_or_panic(60))
//!     .build()?;
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
//! Requests over the limit get `429 Too Many Requests` with `retry-after`, and never reach the
//! handler. Every limiter needs a backend, an extractor, a rate and a namespace
//! (`[a-z0-9-]{1,32}`), which keeps limiters sharing a backend apart.
//! [`build`](TrypemaLimiterBuilder::build) reports configuration mistakes as a [`ConfigError`].
//!
//! # Backends
//!
//! A backend is an `Arc` of a trypema provider or of one trypema limiter. Build it with
//! trypema's `RateLimiterBuilder`: `local()`, `redis(connection)` or `hybrid(connection)`, then
//! the window and other settings, then `build()` with the type you want.
//!
//! | Backend | Feature | Provider (both strategies) | Single limiters | Shared across instances |
//! |---|---|---|---|---|
//! | Local | none | `LocalRateLimiterProvider` | `AbsoluteLocalRateLimiter`, `SuppressedLocalRateLimiter` | No: each process counts on its own. |
//! | Redis | `redis` | `RedisRateLimiterProvider` | `AbsoluteRedisRateLimiter`, `SuppressedRedisRateLimiter` | Yes, exactly; one Redis round trip per request. |
//! | Hybrid | `redis` | `HybridRateLimiterProvider` | `AbsoluteHybridRateLimiter`, `SuppressedHybridRateLimiter` | Yes, eventually; most requests are decided from local state. |
//!
//! With a provider, [`strategy`](TrypemaLimiterBuilder::strategy) chooses the strategy (absolute
//! by default). A single limiter runs its own strategy, and its builder has no `strategy`. A
//! limiter and a provider with the same Redis prefix share their counts.
//!
//! Redis and hybrid calls are bounded by `backend_timeout`, and failures follow
//! [`BackendErrorPolicy`]: fail open (the default), fail closed, fall back to a local provider,
//! or decide per failure.
//!
//! # Keys
//!
//! The [`extract`] module decides who a request counts against:
//!
//! - [`PeerIp`](extract::PeerIp) keys by the TCP peer, grouping IPv6 clients by `/64`.
//! - [`RealIp`](extract::RealIp) reads `X-Forwarded-For`, RFC 7239 `Forwarded` or a CDN header,
//!   but only when the TCP peer is one of your [`TrustedProxies`](extract::TrustedProxies).
//! - [`Header`](extract::Header) keys by a header such as an API key, optionally hashed.
//! - [`Global`](extract::Global) puts every request in one bucket, and
//!   [`Composite`](extract::Composite) joins two extractors.
//! - [`extractor_fn`](TrypemaLimiterBuilder::extractor_fn) takes a closure, and
//!   [`KeyExtractor`](extract::KeyExtractor) is the trait for your own extractors.
//!
//! ```
//! use std::sync::Arc;
//!
//! use actix_trypema::{KeyError, TrypemaLimiter, extract::{RealIp, TrustedProxies}};
//! use actix_trypema::trypema::{RateLimit, RateLimiterBuilder, WindowSize, local::LocalRateLimiterProvider};
//! use actix_web::HttpMessage;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let backend: Arc<LocalRateLimiterProvider> = RateLimiterBuilder::local()
//!     .window_size(WindowSize::seconds_or_panic(60))
//!     .build()?;
//!
//! // Behind load balancers in 10.0.0.0/16: the client is the first untrusted address in
//! // X-Forwarded-For, read from the right. Other peers are keyed by their own address.
//! let per_ip = TrypemaLimiter::builder(backend.clone())
//!     .namespace("ip")
//!     .extractor(RealIp::xff(TrustedProxies::new(["10.0.0.0/16"])?))
//!     .rate(RateLimit::per_minute_or_panic(600.0))
//!     .build()?;
//!
//! /// Inserted by your authentication middleware.
//! #[derive(Clone)]
//! struct UserId(u64);
//!
//! // A custom key: the authenticated user. Wrap this limiter inside the authentication layer.
//! let per_user = TrypemaLimiter::builder(backend)
//!     .namespace("user")
//!     .extractor_fn(|req| {
//!         req.extensions()
//!             .get::<UserId>()
//!             .map(|user| user.0.to_string())
//!             .ok_or(KeyError::InvalidValue { reason: "request is not authenticated" })
//!     })
//!     .rate(RateLimit::per_minute_or_panic(300.0))
//!     .build()?;
//! # let _ = (per_ip, per_user);
//! # Ok(())
//! # }
//! ```
//!
//! When extraction fails, [`KeyErrorPolicy`] decides: reject with `400` (the default), bypass, or
//! count the request in one shared bucket.
//!
//! # Limits and responses
//!
//! [`rate_fn`](TrypemaLimiterBuilder::rate_fn) sets per-request rates such as plan tiers (rates
//! are sticky per key, so put the tier in the key), [`cost_fn`](TrypemaLimiterBuilder::cost_fn)
//! charges expensive requests more, [`exclude`](TrypemaLimiterBuilder::exclude) skips requests,
//! and the suppressed strategy sheds load gradually. [`HeaderMode`] picks the rate limit headers,
//! and a [`RejectionResponder`] builds the rejection body.
//!
//! [`permissive`](TrypemaLimiterBuilder::permissive) runs a limiter in shadow mode, and handlers
//! read each decision through [`RateLimitInfo`]. [`derived_key`] returns the key a limiter
//! stores, for resetting or re-rating one client through your backend value.
//!
//! # Middleware placement
//!
//! `App::wrap` runs middleware in reverse registration order: the last `.wrap(..)` runs first.
//! Put IP-keyed limiters outermost, before authentication, so unauthenticated floods are
//! limited. Put identity-keyed limiters (API key, user id) after the authentication layer that
//! validates the identity. Path parameters are visible to limiters that wrap the scope or
//! resource declaring them. An `exclude` predicate sees the path as this middleware receives
//! it, so register `NormalizePath` to run before the limiter when exclusions rely on
//! normalized paths.
//!
//! This version needs trypema 3.1.
//!
//! # Feature flags
//!
//! - `redis`: the Redis and hybrid backends (two providers and four limiters), on trypema's
//!   tokio Redis client (the runtime actix-web runs on). It turns on `trypema/redis-tokio`.
//! - `json`: a JSON rejection responder.
//! - `metrics`: decision counters through the `metrics` facade — every request counts one of
//!   admitted, rejected, or bypassed, and backend failures also count as errors — plus a
//!   latency histogram for backend calls that await Redis.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]
#![forbid(unsafe_code)]

#[cfg(feature = "redis")]
mod async_middleware;
mod backend;
mod builder;
mod client_ip;
mod error;
pub mod extract;
mod key;
mod middleware;
mod outcome;
mod response;

pub use trypema;

#[cfg(feature = "redis")]
pub use backend::AsyncBackend;
pub use backend::{Backend, ProviderBackend};
pub use builder::{HeaderMode, Strategy, TrypemaLimiter, TrypemaLimiterBuilder};
pub use error::{
    BackendErrorPolicy, BackendFailure, ConfigError, ErrorAction, KeyError, KeyErrorPolicy,
};
pub use key::derived_key;
pub use middleware::{LimiterFuture, TrypemaMiddleware};
pub use outcome::{LimitOutcome, RateLimitInfo};
#[cfg(feature = "json")]
pub use response::JsonResponder;
pub use response::{DefaultResponder, RejectionInfo, RejectionResponder};

#[cfg(test)]
mod tests;
