//! Shadow mode and handler-visible outcomes.

use actix_trypema::{LimitOutcome, RateLimitInfo, Strategy};
use actix_web::{
    App, HttpResponse, Responder,
    http::StatusCode,
    test::{self, TestRequest},
    web,
};

use crate::{
    common::{CLIENT, get, header, per_ip},
    support::backend,
};

#[actix_web::test]
async fn permissive_mode_admits_everything_and_reports_outcomes() {
    let backend = backend();
    let absolute = per_ip(&backend, "shadow", 2.0)
        .permissive(true)
        .build()
        .unwrap();
    let suppressed = per_ip(&backend, "shadow-sup", 2.0)
        .strategy(Strategy::Suppressed)
        .permissive(true)
        .build()
        .unwrap();

    async fn handler(info: RateLimitInfo) -> impl Responder {
        match info.outcome() {
            LimitOutcome::Limited { .. } => HttpResponse::Ok().body("limited"),
            LimitOutcome::Admitted { .. } => HttpResponse::Ok().body("admitted"),
            LimitOutcome::Suppressed {
                admitted: false, ..
            } => HttpResponse::Ok().body("suppressed"),
            _ => HttpResponse::Ok().body("other"),
        }
    }

    let app = test::init_service(
        App::new()
            .service(web::resource("/abs").wrap(absolute).to(handler))
            .service(web::resource("/sup").wrap(suppressed).to(handler)),
    )
    .await;

    let mut bodies = Vec::new();

    for _ in 0..4 {
        let response = test::call_service(&app, get(CLIENT).uri("/abs").to_request()).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(header(&response, "x-ratelimit-limit"), Some("2"));
        bodies.push(test::read_body(response).await);
    }

    assert_eq!(bodies, ["admitted", "admitted", "limited", "limited"]);

    // Past the hard limit the suppressed strategy declines with factor 1.0; shadow mode
    // forwards the request and advertises the factor.
    for _ in 0..2 {
        test::call_service(&app, get(CLIENT).uri("/sup").to_request()).await;
    }

    let shed = test::call_service(&app, get(CLIENT).uri("/sup").to_request()).await;
    assert_eq!(shed.status(), StatusCode::OK);
    assert_eq!(header(&shed, "x-ratelimit-suppression"), Some("1.000"));
    assert_eq!(test::read_body(shed).await, "suppressed");
}

#[actix_web::test]
async fn permissive_mode_bypasses_key_errors() {
    let shadow = per_ip(&backend(), "shadow", 1.0)
        .permissive(true)
        .build()
        .unwrap();

    async fn handler(info: RateLimitInfo) -> impl Responder {
        match info.outcome() {
            LimitOutcome::Bypassed => HttpResponse::Ok().body("bypassed"),
            _ => HttpResponse::Ok().body("other"),
        }
    }

    let app = test::init_service(App::new().wrap(shadow).route("/", web::get().to(handler))).await;

    // No peer address: the default policy would answer 400, but shadow mode never rejects.
    let response = test::call_service(&app, TestRequest::default().to_request()).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(test::read_body(response).await, "bypassed");
}

#[actix_web::test]
async fn outcomes_are_recorded_only_when_enabled() {
    let backend = backend();
    let recording = per_ip(&backend, "rec", 5.0)
        .record_outcome(true)
        .build()
        .unwrap();
    let silent = per_ip(&backend, "quiet", 5.0).build().unwrap();

    async fn handler(info: RateLimitInfo) -> impl Responder {
        match info.outcome() {
            LimitOutcome::Admitted { limit, .. } => HttpResponse::Ok().body(limit.to_string()),
            _ => HttpResponse::Ok().body("other"),
        }
    }

    let app = test::init_service(
        App::new()
            .service(web::resource("/rec").wrap(recording).to(handler))
            .service(web::resource("/quiet").wrap(silent).to(handler))
            .service(web::resource("/bare").to(handler)),
    )
    .await;

    let recorded = test::call_service(&app, get(CLIENT).uri("/rec").to_request()).await;
    assert_eq!(test::read_body(recorded).await, "5");

    // Without recording, or without the middleware at all, extraction fails with 500.
    for path in ["/quiet", "/bare"] {
        let response = test::call_service(&app, get(CLIENT).uri(path).to_request()).await;
        assert_eq!(
            response.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "{path}"
        );

        // The body stays generic: configuration details go to the log, not to clients.
        assert_eq!(
            test::read_body(response).await,
            "rate limit outcome unavailable",
            "{path}"
        );
    }
}
