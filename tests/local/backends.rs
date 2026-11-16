//! The three local backend types: a provider and the two single limiters.

use std::sync::Arc;

use actix_trypema::{RejectionInfo, Strategy, TrypemaLimiter, derived_key, extract::PeerIp};
use actix_web::{App, dev::ServiceResponse, http::StatusCode, test, web};
use trypema::{
    BucketSize, RateLimit, RateLimiterBuilder, WindowSize,
    local::{AbsoluteLocalRateLimiter, SuppressedLocalRateLimiter},
};

use crate::{
    common::{CLIENT, CLIENT_KEY, WINDOW_SECS, get, header, per_ip, status_or_error},
    support::{backend, usage},
};

/// The builder settings of `support::backend`, for a target chosen by the caller.
fn local_builder() -> trypema::RateLimiterBuilder<trypema::local::LocalBackend> {
    RateLimiterBuilder::local()
        .window_size(WindowSize::seconds_or_panic(WINDOW_SECS))
        .bucket_size(BucketSize::milliseconds_or_panic(10))
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
async fn every_local_backend_type_limits_requests() {
    let provider = backend();
    let absolute: Arc<AbsoluteLocalRateLimiter> = local_builder().build().unwrap();
    let suppressed: Arc<SuppressedLocalRateLimiter> = local_builder().build().unwrap();

    let provider_limiter = per_ip(&provider, "provider", 2.0).build().unwrap();
    let absolute_limiter = per_ip(&absolute, "absolute", 2.0).build().unwrap();
    let suppressed_limiter = per_ip(&suppressed, "suppressed", 2.0).build().unwrap();

    assert_eq!(statuses!(provider_limiter, 3), LIMITED, "provider");
    assert_eq!(statuses!(absolute_limiter, 3), LIMITED, "absolute limiter");
    assert_eq!(
        statuses!(suppressed_limiter, 3),
        LIMITED,
        "suppressed limiter"
    );

    // Key administration uses the caller's own value, with the derived key.
    assert_eq!(usage(&provider, "provider"), 2);
    assert_eq!(absolute.get(&derived_key("absolute", CLIENT_KEY)), 2);
    assert!(suppressed.get(&derived_key("suppressed", CLIENT_KEY)).total >= 2);
}

#[actix_web::test]
async fn a_single_limiter_runs_its_own_strategy() {
    let absolute: Arc<AbsoluteLocalRateLimiter> = local_builder().build().unwrap();
    let suppressed: Arc<SuppressedLocalRateLimiter> = local_builder().build().unwrap();

    // Absolute: a plain rejection at the limit, with a retry hint.
    let rejected = first_rejection!(per_ip(&absolute, "abs", 2.0).build().unwrap());
    let info = rejection_info(&rejected);
    assert_eq!(info.strategy, Strategy::Absolute);
    assert!(info.suppression_factor.is_none());
    assert!(header(&rejected, "retry-after").is_some());
    assert_eq!(header(&rejected, "x-ratelimit-suppression"), None);

    // Suppressed: with the default hard limit factor of 1.0, the call after capacity is fully
    // suppressed, and a suppressed rejection carries no retry hint of its own.
    let rejected = first_rejection!(per_ip(&suppressed, "sup", 2.0).build().unwrap());
    let info = rejection_info(&rejected);
    assert_eq!(info.strategy, Strategy::Suppressed);
    assert_eq!(info.suppression_factor, Some(1.0));
    assert_eq!(header(&rejected, "retry-after"), None);
    assert_eq!(header(&rejected, "x-ratelimit-suppression"), Some("1.000"));
}

#[actix_web::test]
async fn advertised_limit_uses_the_window_of_a_limiter_backend() {
    let two_minutes: Arc<AbsoluteLocalRateLimiter> = RateLimiterBuilder::local()
        .window_size(WindowSize::minutes_or_panic(2))
        .build()
        .unwrap();
    let limiter = TrypemaLimiter::builder(two_minutes)
        .namespace("ip")
        .extractor(PeerIp::default())
        .rate(RateLimit::per_minute_or_panic(3.0))
        .build()
        .unwrap();

    let app = test::init_service(
        App::new()
            .wrap(limiter)
            .route("/", web::get().to(|| async { "ok" })),
    )
    .await;

    // 3 per minute over the limiter's 2-minute window: capacity 6.
    let response = test::call_service(&app, get(CLIENT).to_request()).await;
    assert_eq!(header(&response, "x-ratelimit-limit"), Some("6"));
}
