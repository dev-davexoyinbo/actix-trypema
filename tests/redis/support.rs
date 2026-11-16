//! Backend and request helpers for the Redis integration tests.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use actix_trypema::{AsyncBackend, TrypemaLimiterBuilder};
use redis::aio::ConnectionManagerConfig;
use trypema::{
    BucketSize, RateLimiterBuilder, WindowSize,
    local::LocalRateLimiterProvider,
    redis::{ConnectionManager, RedisKey, RedisRateLimiterProvider},
};

use crate::common::{self, WINDOW_SECS};

/// `REDIS_URL`, defaulting to the `compose.yaml` Redis.
pub fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:16379/".to_string())
}

/// A connection manager for `url`.
///
/// The redis-rs defaults (500 ms per response, 1 s per connection attempt) are close enough to a
/// call slowed by a loaded machine to fail it. Thirty seconds is far above every middleware
/// `backend_timeout` in these tests, so a slow call never fails inside redis-rs, and a failure
/// that a test sees within seconds can only come from the middleware's own timeout.
pub async fn connection_manager(url: &str) -> ConnectionManager {
    let config = ConnectionManagerConfig::new()
        .set_response_timeout(Some(Duration::from_secs(30)))
        .set_connection_timeout(Some(Duration::from_secs(30)));

    redis::Client::open(url)
        .unwrap()
        .get_connection_manager_with_config(config)
        .await
        .unwrap()
}

/// A key prefix that no other call, test or run uses, so tests never share Redis state.
///
/// The counter keeps two calls in one process apart even when the clock reads the same value
/// (macOS clocks tick in microseconds); the process id and the full timestamp keep runs apart.
pub fn unique_prefix() -> RedisKey {
    static NEXT: AtomicU64 = AtomicU64::new(0);

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
    RedisKey::try_from(format!("at-test-{}-{nanos}-{sequence}", std::process::id())).unwrap()
}

/// A Redis provider under `prefix` on the shared window, with 10 ms buckets and cleanup off.
pub async fn redis_backend(url: &str, prefix: &RedisKey) -> Arc<RedisRateLimiterProvider> {
    RateLimiterBuilder::redis(connection_manager(url).await)
        .prefix(prefix.clone())
        .window_size(WindowSize::seconds_or_panic(WINDOW_SECS))
        .bucket_size(BucketSize::milliseconds_or_panic(10))
        .disable_cleanup()
        .build()
        .unwrap()
}

/// A local provider on a window of `window_secs`, with trypema's defaults otherwise, for
/// fallbacks.
pub fn local_provider(window_secs: u64) -> Arc<LocalRateLimiterProvider> {
    RateLimiterBuilder::local()
        .window_size(WindowSize::seconds_or_panic(window_secs))
        .build()
        .unwrap()
}

/// `common::per_ip` with a 10 s backend timeout.
///
/// The 200 ms default would fail open a Redis call that a loaded machine delays, which admits a
/// request that a test expects to be rejected. Tests of the timeout itself set a short one.
pub fn per_ip<B: AsyncBackend>(
    backend: &B,
    namespace: &str,
    limit: f64,
) -> TrypemaLimiterBuilder<B> {
    common::per_ip(backend, namespace, limit).backend_timeout(Duration::from_secs(10))
}
