//! The six Redis and hybrid backend types: two providers and four single limiters.

use std::{sync::Arc, time::Duration};

use actix_trypema::{RejectionInfo, Strategy, derived_key};
use actix_web::{App, dev::ServiceResponse, http::StatusCode, test, web};
use trypema::{
    BucketSize, RateLimiterBuilder, WindowSize,
    hybrid::{
        AbsoluteHybridRateLimiter, HybridBackend, HybridRateLimiterProvider,
        SuppressedHybridRateLimiter, SyncInterval,
    },
    redis::{
        AbsoluteRedisRateLimiter, RedisBackend, RedisKey, RedisRateLimiterProvider,
        SuppressedRedisRateLimiter,
    },
};

use crate::{
    common::{CLIENT, CLIENT_KEY, WINDOW_SECS, get, status_or_error},
    support::{connection_manager, per_ip, redis_url, unique_prefix},
};

/// The settings of `support::redis_backend` under `prefix`, for a target chosen by the caller.
async fn redis_builder(prefix: &RedisKey) -> RateLimiterBuilder<RedisBackend> {
    RateLimiterBuilder::redis(connection_manager(&redis_url()).await)
        .prefix(prefix.clone())
        .window_size(WindowSize::seconds_or_panic(WINDOW_SECS))
        .bucket_size(BucketSize::milliseconds_or_panic(10))
        .disable_cleanup()
}

/// A hybrid builder under `prefix` on the shared window, with cleanup off.
async fn hybrid_builder(prefix: &RedisKey) -> RateLimiterBuilder<HybridBackend> {
    RateLimiterBuilder::hybrid(connection_manager(&redis_url()).await)
        .prefix(prefix.clone())
        .window_size(WindowSize::seconds_or_panic(WINDOW_SECS))
        .sync_interval(SyncInterval::milliseconds_or_panic(10))
        .disable_cleanup()
}

/// Send `count` requests from `CLIENT` through `limiter` and collect the statuses.
macro_rules! statuses {
    ($limiter:expr, $count:expr) => {{
        let app = test::init_service(
            App::new()
                .wrap($limiter)
                .route("/", web::get().to(|| async { "ok" })),
        )
        .await;
        let mut statuses = Vec::new();

        for _ in 0..$count {
            statuses.push(status_or_error(&app, get(CLIENT).to_request()).await);
        }

        statuses
    }};
}

/// Send requests from `CLIENT` through `limiter` until one is rejected, and return it.
macro_rules! first_rejection {
    ($limiter:expr) => {{
        let app = test::init_service(
            App::new()
                .wrap($limiter)
                .route("/", web::get().to(|| async { "ok" })),
        )
        .await;
        let mut rejected = None;

        for _ in 0..10 {
            let response = test::call_service(&app, get(CLIENT).to_request()).await;

            if response.status() == StatusCode::TOO_MANY_REQUESTS {
                rejected = Some(response);
                break;
            }
        }

        rejected.expect("a rejection within 10 requests")
    }};
}

fn rejection_info<B>(response: &ServiceResponse<B>) -> RejectionInfo {
    response
        .response()
        .extensions()
        .get::<RejectionInfo>()
        .cloned()
        .unwrap()
}

const LIMITED: [StatusCode; 3] = [
    StatusCode::OK,
    StatusCode::OK,
    StatusCode::TOO_MANY_REQUESTS,
];

#[actix_web::test]
async fn every_redis_and_hybrid_backend_type_limits_requests() {
    let redis_provider: Arc<RedisRateLimiterProvider> =
        redis_builder(&unique_prefix()).await.build().unwrap();
    let redis_absolute: Arc<AbsoluteRedisRateLimiter> =
        redis_builder(&unique_prefix()).await.build().unwrap();
    let redis_suppressed: Arc<SuppressedRedisRateLimiter> =
        redis_builder(&unique_prefix()).await.build().unwrap();
    let hybrid_provider: Arc<HybridRateLimiterProvider> =
        hybrid_builder(&unique_prefix()).await.build().unwrap();
    let hybrid_absolute: Arc<AbsoluteHybridRateLimiter> =
        hybrid_builder(&unique_prefix()).await.build().unwrap();
    let hybrid_suppressed: Arc<SuppressedHybridRateLimiter> =
        hybrid_builder(&unique_prefix()).await.build().unwrap();

    let limiters = (
        per_ip(&redis_provider, "rp", 2.0).build().unwrap(),
        per_ip(&redis_absolute, "ra", 2.0).build().unwrap(),
        per_ip(&redis_suppressed, "rs", 2.0).build().unwrap(),
        per_ip(&hybrid_provider, "hp", 2.0).build().unwrap(),
        per_ip(&hybrid_absolute, "ha", 2.0).build().unwrap(),
        per_ip(&hybrid_suppressed, "hs", 2.0).build().unwrap(),
    );

    assert_eq!(statuses!(limiters.0, 3), LIMITED, "redis provider");
    assert_eq!(statuses!(limiters.1, 3), LIMITED, "redis absolute limiter");
    assert_eq!(
        statuses!(limiters.2, 3),
        LIMITED,
        "redis suppressed limiter"
    );
    assert_eq!(statuses!(limiters.3, 3), LIMITED, "hybrid provider");
    assert_eq!(statuses!(limiters.4, 3), LIMITED, "hybrid absolute limiter");
    assert_eq!(
        statuses!(limiters.5, 3),
        LIMITED,
        "hybrid suppressed limiter"
    );

    // Key administration uses the caller's own value, with the derived key.
    let key = |namespace| derived_key(namespace, CLIENT_KEY);
    let redis_key = |namespace| RedisKey::try_from(key(namespace)).unwrap();
    assert_eq!(
        redis_provider
            .absolute()
            .get(&redis_key("rp"))
            .await
            .unwrap(),
        2
    );
    assert_eq!(redis_absolute.get(&redis_key("ra")).await.unwrap(), 2);
    assert!(redis_suppressed.get(&redis_key("rs")).await.unwrap().total >= 2);
}

#[actix_web::test]
async fn each_redis_and_hybrid_limiter_runs_its_own_strategy() {
    let redis_absolute: Arc<AbsoluteRedisRateLimiter> =
        redis_builder(&unique_prefix()).await.build().unwrap();
    let redis_suppressed: Arc<SuppressedRedisRateLimiter> =
        redis_builder(&unique_prefix()).await.build().unwrap();
    let hybrid_absolute: Arc<AbsoluteHybridRateLimiter> =
        hybrid_builder(&unique_prefix()).await.build().unwrap();
    let hybrid_suppressed: Arc<SuppressedHybridRateLimiter> =
        hybrid_builder(&unique_prefix()).await.build().unwrap();

    // An absolute rejection has no suppression factor; a suppressed one always has one.
    let info = rejection_info(&first_rejection!(
        per_ip(&redis_absolute, "ra", 2.0).build().unwrap()
    ));
    assert_eq!(info.strategy, Strategy::Absolute, "redis absolute limiter");
    assert!(info.suppression_factor.is_none(), "redis absolute limiter");

    let info = rejection_info(&first_rejection!(
        per_ip(&redis_suppressed, "rs", 2.0).build().unwrap()
    ));
    assert_eq!(
        info.strategy,
        Strategy::Suppressed,
        "redis suppressed limiter"
    );
    assert!(
        info.suppression_factor.is_some(),
        "redis suppressed limiter"
    );

    let info = rejection_info(&first_rejection!(
        per_ip(&hybrid_absolute, "ha", 2.0).build().unwrap()
    ));
    assert_eq!(info.strategy, Strategy::Absolute, "hybrid absolute limiter");
    assert!(info.suppression_factor.is_none(), "hybrid absolute limiter");

    let info = rejection_info(&first_rejection!(
        per_ip(&hybrid_suppressed, "hs", 2.0).build().unwrap()
    ));
    assert_eq!(
        info.strategy,
        Strategy::Suppressed,
        "hybrid suppressed limiter"
    );
    assert!(
        info.suppression_factor.is_some(),
        "hybrid suppressed limiter"
    );
}

#[actix_web::test]
async fn a_redis_limiter_and_provider_with_one_prefix_share_counts() {
    let prefix = unique_prefix();
    let limiter_backend: Arc<AbsoluteRedisRateLimiter> =
        redis_builder(&prefix).await.build().unwrap();
    let provider_backend: Arc<RedisRateLimiterProvider> =
        redis_builder(&prefix).await.build().unwrap();

    let app = test::init_service(
        App::new()
            .service(
                web::resource("/limiter")
                    .wrap(per_ip(&limiter_backend, "ip", 3.0).build().unwrap())
                    .to(|| async { "ok" }),
            )
            .service(
                web::resource("/provider")
                    .wrap(per_ip(&provider_backend, "ip", 3.0).build().unwrap())
                    .to(|| async { "ok" }),
            ),
    )
    .await;

    for path in ["/limiter", "/provider", "/limiter"] {
        assert_eq!(
            status_or_error(&app, get(CLIENT).uri(path).to_request()).await,
            StatusCode::OK,
            "{path}"
        );
    }

    assert_eq!(
        status_or_error(&app, get(CLIENT).uri("/provider").to_request()).await,
        StatusCode::TOO_MANY_REQUESTS,
        "3 shared admissions, then rejection through the provider"
    );

    let key = RedisKey::try_from(derived_key("ip", CLIENT_KEY)).unwrap();
    assert_eq!(limiter_backend.get(&key).await.unwrap(), 3);
    assert_eq!(provider_backend.absolute().get(&key).await.unwrap(), 3);
}

#[actix_web::test]
async fn a_hybrid_limiter_and_provider_with_one_prefix_share_counts() {
    let prefix = unique_prefix();
    let limiter_backend: Arc<AbsoluteHybridRateLimiter> =
        hybrid_builder(&prefix).await.build().unwrap();
    let provider_backend: Arc<HybridRateLimiterProvider> =
        hybrid_builder(&prefix).await.build().unwrap();

    let app = test::init_service(
        App::new()
            .service(
                web::resource("/limiter")
                    .wrap(per_ip(&limiter_backend, "ip", 3.0).build().unwrap())
                    .to(|| async { "ok" }),
            )
            .service(
                web::resource("/provider")
                    .wrap(per_ip(&provider_backend, "ip", 3.0).build().unwrap())
                    .to(|| async { "ok" }),
            ),
    )
    .await;

    for _ in 0..3 {
        assert_eq!(
            status_or_error(&app, get(CLIENT).uri("/limiter").to_request()).await,
            StatusCode::OK
        );
    }

    // The limiter flushes its local count to Redis on the 10 ms sync interval. The provider
    // reads Redis on its first request for the key, and then sees the shared window full.
    let key = RedisKey::try_from(derived_key("ip", CLIENT_KEY)).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);

    while provider_backend.absolute().get(&key).await.unwrap() < 3 {
        assert!(
            std::time::Instant::now() < deadline,
            "the limiter flushed its count"
        );
        actix_web::rt::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(
        status_or_error(&app, get(CLIENT).uri("/provider").to_request()).await,
        StatusCode::TOO_MANY_REQUESTS,
        "the provider enforces the count of the limiter"
    );
}
