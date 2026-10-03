//! Key extraction failures and per-key rates.

use actix_trypema::{KeyErrorPolicy, TrypemaLimiter, extract::Header};
use actix_web::{
    App,
    http::StatusCode,
    test::{self, TestRequest},
    web,
};
use trypema::RateLimit;

use crate::{
    common::{CLIENT, get, per_ip, status_or_error},
    support::backend,
};

#[actix_web::test]
async fn key_error_policies_reject_bypass_or_share_the_global_bucket() {
    let backend = backend();

    let reject = per_ip(&backend, "r", 1.0).build().unwrap();
    let bypass = per_ip(&backend, "b", 1.0)
        .on_key_error(KeyErrorPolicy::Bypass)
        .build()
        .unwrap();
    let global = per_ip(&backend, "g", 1.0)
        .on_key_error(KeyErrorPolicy::UseGlobalKey)
        .build()
        .unwrap();
    let missing_header = TrypemaLimiter::builder(backend.clone())
        .namespace("h")
        .extractor(Header::new("x-api-key").unwrap())
        .rate(RateLimit::per_minute_or_panic(1.0))
        .build()
        .unwrap();

    let app = test::init_service(
        App::new()
            .service(web::resource("/r").wrap(reject).to(|| async { "ok" }))
            .service(web::resource("/b").wrap(bypass).to(|| async { "ok" }))
            .service(web::resource("/g").wrap(global).to(|| async { "ok" }))
            .service(
                web::resource("/h")
                    .wrap(missing_header)
                    .to(|| async { "ok" }),
            ),
    )
    .await;

    // No peer address: the default policy rejects through ResponseError (400).
    assert_eq!(
        status_or_error(&app, TestRequest::get().uri("/r").to_request()).await,
        StatusCode::BAD_REQUEST
    );

    for _ in 0..3 {
        let bypassed = test::call_service(&app, TestRequest::get().uri("/b").to_request()).await;
        assert_eq!(bypassed.status(), StatusCode::OK);
    }

    // UseGlobalKey: peerless requests share one bucket of capacity 1.
    let first = test::call_service(&app, TestRequest::get().uri("/g").to_request()).await;
    assert_eq!(first.status(), StatusCode::OK);
    let second = test::call_service(&app, TestRequest::get().uri("/g").to_request()).await;
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);

    // A missing key header follows the same policy machinery.
    assert_eq!(
        status_or_error(&app, get(CLIENT).uri("/h").to_request()).await,
        StatusCode::BAD_REQUEST
    );
}

#[actix_web::test]
async fn tiered_rates_via_key_and_rate_fn() {
    let backend = backend();
    let limiter = TrypemaLimiter::builder(backend.clone())
        .namespace("tier")
        .extractor(Header::new("x-api-key").unwrap())
        .rate_fn(|_req, key| {
            if key.starts_with("pro-") {
                RateLimit::per_minute_or_panic(100.0)
            } else {
                RateLimit::per_minute_or_panic(1.0)
            }
        })
        .build()
        .unwrap();

    let app = test::init_service(
        App::new()
            .wrap(limiter)
            .route("/", web::get().to(|| async { "ok" })),
    )
    .await;

    let with_key = |key: &str| get(CLIENT).insert_header(("x-api-key", key)).to_request();

    for _ in 0..5 {
        let response = test::call_service(&app, with_key("pro-1")).await;
        assert_eq!(response.status(), StatusCode::OK, "pro tier");
    }

    let first = test::call_service(&app, with_key("basic-1")).await;
    assert_eq!(first.status(), StatusCode::OK);

    let second = test::call_service(&app, with_key("basic-1")).await;
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS, "basic tier");
}
