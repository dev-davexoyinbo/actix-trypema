//! Rate limit response headers.

use actix_trypema::HeaderMode;
use actix_web::{App, HttpResponse, http::StatusCode, test, web};

use crate::{
    common::{CLIENT, get, header, per_ip},
    support::backend,
};

#[actix_web::test]
async fn handler_set_headers_are_not_overwritten() {
    let backend = backend();
    let limiter = per_ip(&backend, "ip", 5.0).build().unwrap();

    let app = test::init_service(App::new().wrap(limiter).route(
        "/",
        web::get().to(|| async {
            HttpResponse::Ok()
                .insert_header(("x-ratelimit-limit", "custom"))
                .finish()
        }),
    ))
    .await;

    let response = test::call_service(&app, get(CLIENT).to_request()).await;
    assert_eq!(header(&response, "x-ratelimit-limit"), Some("custom"));
}

#[actix_web::test]
async fn header_mode_off_sends_no_rate_limit_headers() {
    let backend = backend();
    let limiter = per_ip(&backend, "off", 1.0)
        .headers(HeaderMode::Off)
        .remaining_header(true)
        .build()
        .unwrap();

    let app = test::init_service(
        App::new()
            .wrap(limiter)
            .route("/", web::get().to(|| async { "ok" })),
    )
    .await;

    let admitted = test::call_service(&app, get(CLIENT).to_request()).await;
    let rejected = test::call_service(&app, get(CLIENT).to_request()).await;
    assert_eq!(rejected.status(), StatusCode::TOO_MANY_REQUESTS);

    for response in [&admitted, &rejected] {
        let names: Vec<_> = response
            .headers()
            .keys()
            .map(|name| name.as_str().to_string())
            .filter(|name| name.starts_with("x-ratelimit") || name.starts_with("ratelimit"))
            .collect();
        assert!(names.is_empty(), "{names:?}");
    }
}

#[actix_web::test]
async fn remaining_header_reports_saturating_quota() {
    let backend = backend();
    let limiter = per_ip(&backend, "rem", 3.0)
        .remaining_header(true)
        .build()
        .unwrap();

    let app = test::init_service(
        App::new()
            .wrap(limiter)
            .route("/", web::get().to(|| async { "ok" })),
    )
    .await;

    for remaining in ["2", "1", "0"] {
        let response = test::call_service(&app, get(CLIENT).to_request()).await;
        assert_eq!(header(&response, "x-ratelimit-remaining"), Some(remaining));
    }
}

#[actix_web::test]
async fn ietf_headers_follow_the_draft_fields() {
    let backend = backend();

    let plain = per_ip(&backend, "one", 2.0)
        .headers(HeaderMode::Ietf)
        .build()
        .unwrap();
    let with_remaining = per_ip(&backend, "two", 2.0)
        .headers(HeaderMode::Ietf)
        .remaining_header(true)
        .build()
        .unwrap();

    let app = test::init_service(
        App::new()
            .service(web::resource("/one").wrap(plain).to(|| async { "ok" }))
            .service(
                web::resource("/two")
                    .wrap(with_remaining)
                    .to(|| async { "ok" }),
            ),
    )
    .await;

    let admitted = test::call_service(&app, get(CLIENT).uri("/one").to_request()).await;
    assert_eq!(
        header(&admitted, "ratelimit-policy"),
        Some("\"one\";q=2;w=60")
    );
    // Without the remaining read, the mandatory `r` field cannot be produced on admits.
    assert_eq!(header(&admitted, "ratelimit"), None);

    let with_r = test::call_service(&app, get(CLIENT).uri("/two").to_request()).await;
    assert_eq!(header(&with_r, "ratelimit"), Some("\"two\";r=1;t=60"));

    test::call_service(&app, get(CLIENT).uri("/one").to_request()).await;
    let rejected = test::call_service(&app, get(CLIENT).uri("/one").to_request()).await;
    assert_eq!(rejected.status(), StatusCode::TOO_MANY_REQUESTS);

    // On rejection `t` is the same retry hint `retry-after` carries.
    let retry_after = header(&rejected, "retry-after").unwrap();
    assert_eq!(
        header(&rejected, "ratelimit"),
        Some(format!("\"one\";r=0;t={retry_after}").as_str())
    );
}
