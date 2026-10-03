//! The hybrid backend.

use actix_trypema::{Hybrid, TrypemaLimiter, extract::PeerIp};
use actix_web::{App, http::StatusCode, test, web};
use trypema::{BucketSize, RateLimit, RateLimiterBuilder, WindowSize};

use crate::{
    common::{CLIENT, WINDOW_SECS, get, header, status_or_error},
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
