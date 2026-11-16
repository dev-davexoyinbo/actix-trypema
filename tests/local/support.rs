//! Helpers for the local-backend integration tests.

use std::sync::Arc;

use actix_trypema::derived_key;
use trypema::{BucketSize, RateLimiterBuilder, WindowSize, local::LocalRateLimiterProvider};

use crate::common::{CLIENT_KEY, WINDOW_SECS};

/// A second client, for isolation checks.
pub const OTHER_CLIENT: &str = "198.51.100.9:9000";

/// A local provider on the shared window, with 10 ms buckets and cleanup off.
pub fn backend() -> Arc<LocalRateLimiterProvider> {
    RateLimiterBuilder::local()
        .window_size(WindowSize::seconds_or_panic(WINDOW_SECS))
        .bucket_size(BucketSize::milliseconds_or_panic(10))
        .disable_cleanup()
        .build()
        .unwrap()
}

/// Absolute-strategy usage the limiter recorded for `CLIENT` under `namespace`.
pub fn usage(backend: &LocalRateLimiterProvider, namespace: &str) -> u64 {
    backend.absolute().get(&derived_key(namespace, CLIENT_KEY))
}
