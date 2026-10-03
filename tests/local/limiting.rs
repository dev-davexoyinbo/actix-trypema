//! Core admission: limits, rejections, the backend window, stacking, and cost.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use actix_trypema::{
    Local, RateLimitInfo, RejectionInfo, TrypemaLimiter, derived_key, extract::PeerIp,
};
use actix_web::{App, HttpResponse, Responder, http::StatusCode, test, web};
use trypema::{RateLimit, WindowSize};

use crate::{
    common::{CLIENT, CLIENT_KEY, WINDOW_SECS, get, header, per_ip},
    support::{OTHER_CLIENT, backend, usage},
};

// The factory is cloned into every worker's `HttpServer::new` closure.
const _: () = {
    const fn assert_bounds<T: Clone + Send + Sync + 'static>() {}

    assert_bounds::<TrypemaLimiter<Local>>();
};

#[actix_web::test]
async fn absolute_limits_then_rejects_with_retry_after() {
    let backend = backend();
    let handled = Arc::new(AtomicUsize::new(0));
    let handler_count = Arc::clone(&handled);

    let app = test::init_service(
        App::new()
            .wrap(per_ip(&backend, "ip", 3.0).build().unwrap())
            .route(
                "/",
                web::get().to(move || {
                    handler_count.fetch_add(1, Ordering::SeqCst);
                    async { "ok" }
                }),
            ),
    )
    .await;

    for attempt in 0..3 {
        let response = test::call_service(&app, get(CLIENT).to_request()).await;
        assert_eq!(response.status(), StatusCode::OK, "attempt {attempt}");
        assert_eq!(header(&response, "x-ratelimit-limit"), Some("3"));
    }

    let rejected = test::call_service(&app, get(CLIENT).to_request()).await;
    assert_eq!(rejected.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(header(&rejected, "x-ratelimit-limit"), Some("3"));

    let retry_after: u64 = header(&rejected, "retry-after").unwrap().parse().unwrap();
    assert!((1..=WINDOW_SECS).contains(&retry_after), "{retry_after}");

    // The rejection carries its RejectionInfo for outer middleware; its key is the operator
    // handle into the provider.
    let info = rejected
        .response()
        .extensions()
        .get::<RejectionInfo>()
        .cloned()
        .unwrap();
    assert_eq!(&*info.key, derived_key("ip", CLIENT_KEY));
    assert_eq!(info.limit, 3);

    // Rejected requests never reached the handler, and another client is unaffected.
    assert_eq!(handled.load(Ordering::SeqCst), 3);
    let other = test::call_service(&app, get(OTHER_CLIENT).to_request()).await;
    assert_eq!(other.status(), StatusCode::OK);
    assert_eq!(handled.load(Ordering::SeqCst), 4);
}

#[actix_web::test]
async fn advertised_limit_uses_the_backend_window() {
    let two_minutes = Local::new(WindowSize::minutes_or_panic(2)).unwrap();
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

    // 3 per minute over the backend's 2-minute window: capacity 6, with no second place to
    // state the window.
    let response = test::call_service(&app, get(CLIENT).to_request()).await;
    assert_eq!(header(&response, "x-ratelimit-limit"), Some("6"));
}

#[actix_web::test]
async fn stacked_instances_are_isolated_and_the_outer_is_charged() {
    let backend = backend();
    let outer = per_ip(&backend, "outer", 2.0)
        .record_outcome(true)
        .build()
        .unwrap();
    let inner = per_ip(&backend, "inner", 1.0)
        .record_outcome(true)
        .build()
        .unwrap();

    async fn handler(info: RateLimitInfo) -> impl Responder {
        assert!(info.for_namespace("outer").is_some());
        assert!(info.for_namespace("inner").is_some());
        // The innermost instance recorded last.
        assert_eq!(info.outcome(), info.for_namespace("inner").unwrap());
        HttpResponse::Ok()
    }

    // wrap() runs in reverse registration order: `outer` first.
    let app = test::init_service(
        App::new()
            .wrap(inner)
            .wrap(outer)
            .route("/", web::get().to(handler)),
    )
    .await;

    let first = test::call_service(&app, get(CLIENT).to_request()).await;
    assert_eq!(first.status(), StatusCode::OK);

    let second = test::call_service(&app, get(CLIENT).to_request()).await;
    assert_eq!(
        second.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "inner rejects"
    );

    // Namespaces keep the two instances' counters apart on the shared provider, and the outer
    // instance's quota was consumed even though the inner one denied the request.
    assert_eq!(usage(&backend, "outer"), 2);
    assert_eq!(usage(&backend, "inner"), 1);
}

#[actix_web::test]
async fn cost_above_capacity_overshoots_once_locally() {
    let backend = backend();
    let limiter = per_ip(&backend, "cost", 3.0)
        .cost_fn(|_| 5)
        .build()
        .unwrap();

    let app = test::init_service(
        App::new()
            .wrap(limiter)
            .route("/", web::get().to(|| async { "ok" })),
    )
    .await;

    // Local absolute admission checks before adding, so one oversized request overshoots …
    let first = test::call_service(&app, get(CLIENT).to_request()).await;
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(usage(&backend, "cost"), 5);

    // … and the window is saturated afterwards.
    let second = test::call_service(&app, get(CLIENT).to_request()).await;
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
}
