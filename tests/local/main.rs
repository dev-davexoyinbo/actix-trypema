//! Integration tests for the local backend, driven through actix's test harness.

#[path = "../common/mod.rs"]
mod common;
mod support;

mod bypass;
mod config;
mod headers;
mod keys;
mod limiting;
mod outcomes;
mod rejections;
mod server;
mod suppressed;
#[cfg(feature = "metrics")]
mod telemetry;
