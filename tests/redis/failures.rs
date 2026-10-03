//! Backend failure policies, timeouts, and recovery.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use actix_trypema::{
    BackendErrorPolicy, BackendFailure, ErrorAction, LimitOutcome, Local, RateLimitInfo,
    derived_key,
};
use actix_web::{App, HttpResponse, Responder, http::StatusCode, test, web};
use trypema::WindowSize;

use crate::{
    common::{CLIENT, CLIENT_KEY, WINDOW_SECS, get, per_ip, status_or_error},
    flaky_proxy::FlakyProxy,
    support::{redis_backend, redis_url, unique_prefix},
};

#[actix_web::test]
async fn backend_error_policies_decide_unreachable_backends() {
    let proxy = FlakyProxy::start();
    let short_timeout = Duration::from_millis(50);
    let local_fallback = Local::new(WindowSize::seconds_or_panic(WINDOW_SECS)).unwrap();
    let observed_failure = Arc::new(AtomicBool::new(false));
    let observer = Arc::clone(&observed_failure);

    let fail_open = per_ip(
        &redis_backend(&proxy.url(), &unique_prefix()).await,
        "open",
        1.0,
    )
    .backend_timeout(short_timeout)
    .record_outcome(true)
    .build()
    .unwrap();
    let fail_closed = per_ip(
        &redis_backend(&proxy.url(), &unique_prefix()).await,
        "closed",
        1.0,
    )
    .on_backend_error(BackendErrorPolicy::FailClosed)
    .backend_timeout(short_timeout)
    .build()
    .unwrap();
    let fallback = per_ip(
        &redis_backend(&proxy.url(), &unique_prefix()).await,
        "fallback",
        2.0,
    )
    .on_backend_error(BackendErrorPolicy::Fallback(local_fallback.clone()))
    .backend_timeout(short_timeout)
    .build()
    .unwrap();
    let custom = per_ip(
        &redis_backend(&proxy.url(), &unique_prefix()).await,
        "custom",
        1.0,
    )
    .on_backend_error(BackendErrorPolicy::Custom(Arc::new(move |failure| {
        // A severed connection surfaces as a trypema error; a hung one as a timeout.
        if matches!(
            failure,
            BackendFailure::Error(_) | BackendFailure::Timeout(_)
        ) {
            observer.store(true, Ordering::SeqCst);
        }

        ErrorAction::Reject {
            status: StatusCode::IM_A_TEAPOT,
        }
    })))
    .backend_timeout(short_timeout)
    .build()
    .unwrap();

    async fn outcome(info: RateLimitInfo) -> impl Responder {
        match info.outcome() {
            LimitOutcome::BackendError => HttpResponse::Ok().body("backend error"),
            _ => HttpResponse::Ok().body("other"),
        }
    }

    let app = test::init_service(
        App::new()
            .service(web::resource("/open").wrap(fail_open).to(outcome))
            .service(
                web::resource("/closed")
                    .wrap(fail_closed)
                    .to(|| async { "ok" }),
            )
            .service(
                web::resource("/fallback")
                    .wrap(fallback)
                    .to(|| async { "ok" }),
            )
            .service(web::resource("/custom").wrap(custom).to(|| async { "ok" })),
    )
    .await;

    proxy.cut();

    // FailOpen (the default): the outage never rejects, and handlers can see it happened.
    for _ in 0..3 {
        let response = test::call_service(&app, get(CLIENT).uri("/open").to_request()).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(test::read_body(response).await, "backend error");
    }

    // FailClosed: 503, and the policy fires within the timeout bound.
    let started = Instant::now();
    assert_eq!(
        status_or_error(&app, get(CLIENT).uri("/closed").to_request()).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(
        started.elapsed() < short_timeout + Duration::from_millis(200),
        "policy fired after {:?}",
        started.elapsed()
    );

    // Fallback: the local backend enforces its quota per instance, under the same key.
    for expected in [
        StatusCode::OK,
        StatusCode::OK,
        StatusCode::TOO_MANY_REQUESTS,
    ] {
        assert_eq!(
            status_or_error(&app, get(CLIENT).uri("/fallback").to_request()).await,
            expected
        );
    }

    assert_eq!(
        local_fallback
            .provider()
            .absolute()
            .get(&derived_key("fallback", CLIENT_KEY)),
        2,
        "the fallback ran under the same derived key"
    );

    // Custom: the policy function saw the typed failure and chose the status.
    assert_eq!(
        status_or_error(&app, get(CLIENT).uri("/custom").to_request()).await,
        StatusCode::IM_A_TEAPOT
    );
    assert!(
        observed_failure.load(Ordering::SeqCst),
        "the policy function received the failure"
    );
}

#[actix_web::test]
async fn fallback_on_another_window_is_rejected_at_build() {
    let backend = redis_backend(&redis_url(), &unique_prefix()).await;
    let other_window = Local::new(WindowSize::seconds_or_panic(WINDOW_SECS + 1)).unwrap();

    let result = per_ip(&backend, "fallback", 1.0)
        .on_backend_error(BackendErrorPolicy::Fallback(other_window))
        .build();

    assert!(matches!(
        result,
        Err(actix_trypema::ConfigError::FallbackWindowMismatch)
    ));
}

#[actix_web::test]
async fn zero_backend_timeout_is_rejected_at_build() {
    let backend = redis_backend(&redis_url(), &unique_prefix()).await;

    let result = per_ip(&backend, "timeout", 1.0)
        .backend_timeout(Duration::ZERO)
        .build();

    assert!(matches!(
        result,
        Err(actix_trypema::ConfigError::ZeroBackendTimeout)
    ));
}

#[actix_web::test]
async fn enforcement_resumes_when_the_backend_recovers() {
    let proxy = FlakyProxy::start();
    let limiter = per_ip(
        &redis_backend(&proxy.url(), &unique_prefix()).await,
        "recover",
        1.0,
    )
    .on_backend_error(BackendErrorPolicy::FailClosed)
    .backend_timeout(Duration::from_millis(50))
    .build()
    .unwrap();

    let app = test::init_service(
        App::new()
            .wrap(limiter)
            .route("/", web::get().to(|| async { "ok" })),
    )
    .await;

    proxy.cut();
    assert_eq!(
        status_or_error(&app, get(CLIENT).to_request()).await,
        StatusCode::SERVICE_UNAVAILABLE
    );

    proxy.restore();

    // The connection manager reconnects in the background; wait for the first admission.
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        let status = status_or_error(&app, get(CLIENT).to_request()).await;

        if status == StatusCode::OK {
            break;
        }

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(Instant::now() < deadline, "backend never recovered");
        actix_web::rt::time::sleep(Duration::from_millis(50)).await;
    }

    // Enforcement is back: the quota of one is spent.
    assert_eq!(
        status_or_error(&app, get(CLIENT).to_request()).await,
        StatusCode::TOO_MANY_REQUESTS
    );
}

#[actix_web::test]
async fn permissive_mode_admits_on_backend_errors() {
    let proxy = FlakyProxy::start();
    let limiter = per_ip(
        &redis_backend(&proxy.url(), &unique_prefix()).await,
        "shadow",
        1.0,
    )
    .on_backend_error(BackendErrorPolicy::FailClosed)
    .backend_timeout(Duration::from_millis(50))
    .permissive(true)
    .build()
    .unwrap();

    let app = test::init_service(
        App::new()
            .wrap(limiter)
            .route("/", web::get().to(|| async { "ok" })),
    )
    .await;

    proxy.cut();

    // Permissive overrides even FailClosed: shadow mode never rejects.
    assert_eq!(
        status_or_error(&app, get(CLIENT).to_request()).await,
        StatusCode::OK
    );
}
