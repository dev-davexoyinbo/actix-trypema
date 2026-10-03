//! Redis and hybrid backend integration tests.
//!
//! They need a Redis 7.2+ reachable through `REDIS_URL` (default `redis://127.0.0.1:16379/`,
//! matching `compose.yaml`; only plain `host:port` URLs are supported here). Backend failures are
//! injected with a cuttable TCP proxy in front of Redis: backends are built while it forwards,
//! then the connections are severed and new ones blackholed, so the middleware's timeout and
//! error policy fire deterministically without stopping the shared Redis.

#![cfg(feature = "redis")]

#[path = "../common/mod.rs"]
mod common;
mod flaky_proxy;
mod support;

mod failures;
mod hybrid;
mod limiting;
#[cfg(feature = "metrics")]
mod telemetry;
