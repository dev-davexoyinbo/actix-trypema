//! The hybrid backend.

use actix_trypema::{
    Hybrid, LimitOutcome, RateLimitInfo, Strategy, TrypemaLimiter, extract::PeerIp,
};
use actix_web::{App, HttpResponse, Responder, http::StatusCode, test, web};
use trypema::{BucketSize, RateLimit, RateLimiterBuilder, WindowSize};

use crate::{
    common::{CLIENT, WINDOW_SECS, get, header, per_ip, status_or_error},
    support::{connection_manager, redis_url, unique_prefix},
};

#[actix_web::test]
async fn hybrid_limits_and_reports_remaining_through_estimates() {
    let url = redis_url();
    let prefix = unique_prefix();
    let backend = Hybrid::configured(
        connection_manager(&url).await,
        WindowSize::seconds_or_panic(WINDOW_SECS),
        |builder| {
            builder
                .prefix(prefix)
                .bucket_size(BucketSize::milliseconds_or_panic(10))
                .disable_cleanup()
        },
    )
    .unwrap();

    let limiter = TrypemaLimiter::builder(backend)
        .namespace("hy")
        .extractor(PeerIp::default())
        .rate(RateLimit::per_minute_or_panic(3.0))
        .remaining_header(true)
        .build()
        .unwrap();

    let app = test::init_service(
        App::new()
            .wrap(limiter)
            .route("/", web::get().to(|| async { "ok" })),
    )
    .await;

    // The remaining header flows through get_estimate, trypema's no-steady-state-Redis read.
    let mut last_remaining = None;

    for _ in 0..3 {
        let response = test::call_service(&app, get(CLIENT).to_request()).await;
        assert_eq!(response.status(), StatusCode::OK);
        last_remaining = header(&response, "x-ratelimit-remaining").map(str::to_string);
    }

    assert_eq!(last_remaining.as_deref(), Some("0"));
    assert_eq!(
        status_or_error(&app, get(CLIENT).to_request()).await,
        StatusCode::TOO_MANY_REQUESTS,
        "the hybrid local path enforces the shared window"
    );
}

/// A hybrid backend on a fresh prefix, with cleanup off.
async fn hybrid() -> Hybrid {
    Hybrid::configured(
        connection_manager(&redis_url()).await,
        WindowSize::seconds_or_panic(WINDOW_SECS),
        |builder| builder.prefix(unique_prefix()).disable_cleanup(),
    )
    .unwrap()
}

#[actix_web::test]
async fn hybrid_decides_from_local_state_without_the_remaining_header() {
    let limiter = per_ip(&hybrid().await, "hy-sync", 3.0).build().unwrap();
    let app = test::init_service(
        App::new()
            .wrap(limiter)
            .route("/", web::get().to(|| async { "ok" })),
    )
    .await;

    // The first request refreshes from Redis; the next two are admitted from local state.
    for request in 1..=3 {
        let response = test::call_service(&app, get(CLIENT).to_request()).await;
        assert_eq!(response.status(), StatusCode::OK, "request {request}");
        assert_eq!(header(&response, "x-ratelimit-limit"), Some("3"));
    }

    // Exhaustion commits to Redis; the rejection after it is served from local state.
    for request in ["exhausting", "cached"] {
        let response = test::call_service(&app, get(CLIENT).to_request()).await;
        assert_eq!(
            response.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "{request} rejection"
        );
        assert!(
            header(&response, "retry-after").is_some(),
            "{request} rejection carries retry-after"
        );
    }
}

#[actix_web::test]
async fn hybrid_shadow_mode_records_local_rejections() {
    let limiter = per_ip(&hybrid().await, "hy-shadow", 3.0)
        .permissive(true)
        .build()
        .unwrap();

    async fn handler(info: RateLimitInfo) -> impl Responder {
        match info.outcome() {
            LimitOutcome::Admitted { .. } => HttpResponse::Ok().body("admitted"),
            LimitOutcome::Limited { .. } => HttpResponse::Ok().body("limited"),
            _ => HttpResponse::Ok().body("other"),
        }
    }

    let app = test::init_service(App::new().wrap(limiter).route("/", web::get().to(handler))).await;
    let mut bodies = Vec::new();

    for _ in 0..5 {
        let response = test::call_service(&app, get(CLIENT).to_request()).await;
        assert_eq!(response.status(), StatusCode::OK);
        bodies.push(test::read_body(response).await);
    }

    // The fourth rejection is decided through Redis, the fifth from the local rejection cache.
    assert_eq!(
        bodies,
        ["admitted", "admitted", "admitted", "limited", "limited"]
    );
}

#[actix_web::test]
async fn hybrid_suppressed_strategy_sheds_from_local_state() {
    let limiter = per_ip(&hybrid().await, "hy-sup", 3.0)
        .strategy(Strategy::Suppressed)
        .build()
        .unwrap();
    let app = test::init_service(
        App::new()
            .wrap(limiter)
            .route("/", web::get().to(|| async { "ok" })),
    )
    .await;

    let mut statuses = Vec::new();

    for _ in 0..5 {
        statuses.push(status_or_error(&app, get(CLIENT).to_request()).await);
    }

    // The request reaching the hard limit is admitted through Redis and caches full
    // suppression; later requests are declined from local state.
    assert_eq!(
        statuses,
        [
            StatusCode::OK,
            StatusCode::OK,
            StatusCode::OK,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::TOO_MANY_REQUESTS,
        ]
    );
}
