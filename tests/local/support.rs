//! Helpers for the local-backend integration tests.

use actix_trypema::{Local, derived_key};
use trypema::{BucketSize, RateLimiterBuilder, WindowSize};

use crate::common::{CLIENT_KEY, WINDOW_SECS};

/// A second client, for isolation checks.
pub const OTHER_CLIENT: &str = "198.51.100.9:9000";

/// A local backend on the shared window, with 10 ms buckets and cleanup off.
pub fn backend() -> Local {
    Local::configured(WindowSize::seconds_or_panic(WINDOW_SECS), |builder| {
        builder
            .bucket_size(BucketSize::milliseconds_or_panic(10))
            .disable_cleanup()
    })
    .unwrap()
}

/// Absolute-strategy usage the limiter recorded for `CLIENT` under `namespace`.
pub fn usage(backend: &Local, namespace: &str) -> u64 {
    backend
        .provider()
        .absolute()
        .get(&derived_key(namespace, CLIENT_KEY))
}
