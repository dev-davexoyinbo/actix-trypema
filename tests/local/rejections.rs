//! Rejection responses and request bodies.

use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use actix_trypema::{RejectionInfo, RejectionResponder};
use actix_web::{
    App, HttpResponse, HttpResponseBuilder,
    dev::{Payload, Service, ServiceRequest, Transform, fn_service},
    error::PayloadError,
    http::StatusCode,
    test,
    web::{self, Bytes},
};

use crate::{
    common::{CLIENT, get, header, per_ip},
    support::backend,
};

#[actix_web::test]
async fn custom_responder_sets_status_and_body() {
    struct ShedResponder;

    impl RejectionResponder for ShedResponder {
        fn respond(&self, info: &RejectionInfo, mut builder: HttpResponseBuilder) -> HttpResponse {
            builder
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .body(format!("{} is overloaded", info.namespace))
        }
    }

    let backend = backend();
    let limiter = per_ip(&backend, "shed", 1.0)
        .responder(ShedResponder)
        .build()
        .unwrap();

    let app = test::init_service(
        App::new()
            .wrap(limiter)
            .route("/", web::get().to(|| async { "ok" })),
    )
    .await;

    test::call_service(&app, get(CLIENT).to_request()).await;
    let rejected = test::call_service(&app, get(CLIENT).to_request()).await;

    assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
    // The builder arrived with the rate limit headers already set.
    assert_eq!(header(&rejected, "x-ratelimit-limit"), Some("1"));
    assert_eq!(test::read_body(rejected).await, "shed is overloaded");
}

#[cfg(feature = "json")]
#[actix_web::test]
async fn json_responder_renders_the_rejection() {
    let backend = backend();
    let limiter = per_ip(&backend, "api", 1.0)
        .responder(actix_trypema::JsonResponder)
        .build()
        .unwrap();

    let app = test::init_service(
        App::new()
            .wrap(limiter)
            .route("/", web::get().to(|| async { "ok" })),
    )
    .await;

    test::call_service(&app, get(CLIENT).to_request()).await;
    let rejected = test::call_service(&app, get(CLIENT).to_request()).await;
    assert_eq!(rejected.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(header(&rejected, "content-type"), Some("application/json"));

    let body: serde_json::Value = test::read_body_json(rejected).await;
    assert_eq!(body["error"], "rate_limited");
    assert_eq!(body["namespace"], "api");
    assert!(body["retry_after_seconds"].as_u64().unwrap() >= 1);
}

/// A request payload that records whether anything polled it.
struct TrackingPayload(Arc<AtomicBool>);

impl futures_core::Stream for TrackingPayload {
    type Item = Result<Bytes, PayloadError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.store(true, Ordering::SeqCst);
        Poll::Ready(None)
    }
} // end impl

fn tracked_request(path: &str) -> (ServiceRequest, Arc<AtomicBool>) {
    let polled = Arc::new(AtomicBool::new(false));
    let mut req = get(CLIENT).uri(path).to_srv_request();
    let stream: Pin<Box<dyn futures_core::Stream<Item = Result<Bytes, PayloadError>>>> =
        Box::pin(TrackingPayload(Arc::clone(&polled)));
    req.set_payload(Payload::from(stream));
    (req, polled)
}

#[actix_web::test]
async fn rejected_and_bypassed_request_bodies_are_never_polled() {
    let limiter = per_ip(&backend(), "body", 1.0)
        .exclude(|req| req.path() == "/skip")
        .build()
        .unwrap();

    // The inner service never reads bodies, so any poll would be the middleware's.
    let middleware = limiter
        .new_transform(fn_service(|req: ServiceRequest| async move {
            Ok::<_, actix_web::Error>(req.into_response(HttpResponse::Ok().finish()))
        }))
        .await
        .unwrap();

    middleware.call(get(CLIENT).to_srv_request()).await.unwrap();

    let (rejected, rejected_polled) = tracked_request("/");
    let response = middleware.call(rejected).await.unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(!rejected_polled.load(Ordering::SeqCst), "rejected body");

    let (bypassed, bypassed_polled) = tracked_request("/skip");
    let response = middleware.call(bypassed).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!bypassed_polled.load(Ordering::SeqCst), "bypassed body");
}
