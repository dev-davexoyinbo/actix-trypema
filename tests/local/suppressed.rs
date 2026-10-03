//! The suppressed strategy: retry hints and load shedding.

use std::time::Duration;

use actix_trypema::{Local, Strategy, TrypemaLimiter, extract::PeerIp};
use actix_web::{App, http::StatusCode, test, web};
use trypema::{BucketSize, RateLimit, RateLimiterBuilder, WindowSize};

use crate::{
    common::{CLIENT, get, header, per_ip},
    support::backend,
};

#[actix_web::test]
async fn suppressed_rejections_omit_or_carry_the_configured_hint() {
    let backend = backend();

    let without_hint = per_ip(&backend, "plain", 2.0)
        .strategy(Strategy::Suppressed)
        .build()
        .unwrap();
    let with_hint = per_ip(&backend, "hinted", 2.0)
        .strategy(Strategy::Suppressed)
        .suppressed_retry_hint(Duration::from_secs(2))
        .build()
        .unwrap();
    let zero_hint = per_ip(&backend, "zero", 2.0)
        .strategy(Strategy::Suppressed)
        .suppressed_retry_hint(Duration::ZERO)
        .build()
        .unwrap();

    let app = test::init_service(
        App::new()
            .service(
                web::resource("/plain")
                    .wrap(without_hint)
                    .to(|| async { "ok" }),
            )
            .service(
                web::resource("/hinted")
                    .wrap(with_hint)
                    .to(|| async { "ok" }),
            )
            .service(web::resource("/zero").wrap(zero_hint).to(|| async { "ok" })),
    )
    .await;

    // With the default hard limit factor of 1.0, the call after capacity is fully suppressed.
    for path in ["/plain", "/hinted", "/zero"] {
        for _ in 0..2 {
            let admitted = test::call_service(&app, get(CLIENT).uri(path).to_request()).await;
            assert_eq!(admitted.status(), StatusCode::OK, "{path}");
        }
    }

    let plain = test::call_service(&app, get(CLIENT).uri("/plain").to_request()).await;
    assert_eq!(plain.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(header(&plain, "retry-after"), None);
    assert_eq!(header(&plain, "x-ratelimit-suppression"), Some("1.000"));

    let hinted = test::call_service(&app, get(CLIENT).uri("/hinted").to_request()).await;
    assert_eq!(hinted.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(header(&hinted, "retry-after"), Some("2"));

    // A zero hint promises nothing, so it sends nothing.
    let zero = test::call_service(&app, get(CLIENT).uri("/zero").to_request()).await;
    assert_eq!(zero.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(header(&zero, "retry-after"), None);
}

#[actix_web::test]
async fn suppressed_overload_admits_roughly_the_target() {
    let one_second = Local::configured(WindowSize::seconds_or_panic(1), |builder| {
        builder
            .bucket_size(BucketSize::milliseconds_or_panic(10))
            .disable_cleanup()
    })
    .unwrap();

    let limiter = TrypemaLimiter::builder(one_second)
        .namespace("shed")
        .extractor(PeerIp::default())
        .rate(RateLimit::per_second_or_panic(10.0))
        .strategy(Strategy::Suppressed)
        .build()
        .unwrap();

    let app = test::init_service(
        App::new()
            .wrap(limiter)
            .route("/", web::get().to(|| async { "ok" })),
    )
    .await;

    let mut admitted = 0;

    // 3x overload in one window. Suppression is probabilistic, so the bounds are loose; the
    // statistical convergence itself is covered by trypema's own suppressed-strategy tests.
    for _ in 0..30 {
        let response = test::call_service(&app, get(CLIENT).to_request()).await;

        if response.status() == StatusCode::OK {
            admitted += 1;
        }
    }

    assert!((6..=20).contains(&admitted), "admitted {admitted} of 30");
}
