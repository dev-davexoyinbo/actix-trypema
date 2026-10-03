//! Backend and request helpers for the Redis integration tests.

use actix_trypema::Redis;
use trypema::{
    BucketSize, RateLimiterBuilder, WindowSize,
    redis::{ConnectionManager, RedisKey},
};

use crate::common::WINDOW_SECS;

/// `REDIS_URL`, defaulting to the `compose.yaml` Redis.
pub fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:16379/".to_string())
}

/// A connection manager for `url`.
pub async fn connection_manager(url: &str) -> ConnectionManager {
    redis::Client::open(url)
        .unwrap()
        .get_connection_manager()
        .await
        .unwrap()
}

/// A key prefix unique to this run, so tests never share Redis state.
pub fn unique_prefix() -> RedisKey {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    RedisKey::try_from(format!("at-test-{}-{nanos}", std::process::id())).unwrap()
}

/// A Redis backend under `prefix` on the shared window, with 10 ms buckets and cleanup off.
pub async fn redis_backend(url: &str, prefix: &RedisKey) -> Redis {
    Redis::configured(
        connection_manager(url).await,
        WindowSize::seconds_or_panic(WINDOW_SECS),
        |builder| {
            builder
                .prefix(prefix.clone())
                .bucket_size(BucketSize::milliseconds_or_panic(10))
                .disable_cleanup()
        },
    )
    .unwrap()
}
