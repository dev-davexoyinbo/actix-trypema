//! Decision counters behind the `metrics` feature.

use std::sync::Arc;

use actix_web::{
    App,
    http::StatusCode,
    test::{TestRequest, call_service, init_service},
    web,
};

use trypema::{RateLimiterBuilder, WindowSize, local::SuppressedLocalRateLimiter};

use crate::{
    common::{CLIENT, WINDOW_SECS, get, per_ip, recorder::CountingRecorder, status_or_error},
    support::backend,
};

#[test]
fn decisions_are_counted_by_namespace_and_strategy() {
    let recorder = CountingRecorder::default();

    metrics::with_local_recorder(&recorder, || {
        actix_web::rt::System::new().block_on(async {
            let limiter = per_ip(&backend(), "m", 1.0)
                .exclude(|req| req.path() == "/skip")
                .build()
                .unwrap();
            let app = init_service(
                App::new()
                    .wrap(limiter)
                    .route("/", web::get().to(|| async { "ok" }))
                    .route("/skip", web::get().to(|| async { "ok" })),
            )
            .await;

            for path in ["/", "/", "/skip"] {
                call_service(&app, get(CLIENT).uri(path).to_request()).await;
            }

            // No peer address: key extraction fails, and the 400 counts as a rejection.
            let key_error = status_or_error(&app, TestRequest::default().to_request()).await;
            assert_eq!(key_error, StatusCode::BAD_REQUEST);
        });
    });

    let labels = "{namespace=m,strategy=absolute}";
    assert_eq!(
        recorder.value(&format!("ratelimit_admitted_total{labels}")),
        1
    );
    assert_eq!(
        recorder.value(&format!("ratelimit_rejected_total{labels}")),
        2
    );
    assert_eq!(
        recorder.value(&format!("ratelimit_bypassed_total{labels}")),
        1
    );
}

#[test]
fn a_single_limiter_labels_its_own_strategy() {
    let recorder = CountingRecorder::default();
    let suppressed: Arc<SuppressedLocalRateLimiter> = RateLimiterBuilder::local()
        .window_size(WindowSize::seconds_or_panic(WINDOW_SECS))
        .disable_cleanup()
        .build()
        .unwrap();

    metrics::with_local_recorder(&recorder, || {
        actix_web::rt::System::new().block_on(async {
            let limiter = per_ip(&suppressed, "m-sup", 1.0).build().unwrap();
            let app = init_service(
                App::new()
                    .wrap(limiter)
                    .route("/", web::get().to(|| async { "ok" })),
            )
            .await;

            for _ in 0..2 {
                call_service(&app, get(CLIENT).to_request()).await;
            }
        });
    });

    let labels = "{namespace=m-sup,strategy=suppressed}";
    assert_eq!(
        recorder.value(&format!("ratelimit_admitted_total{labels}")),
        1
    );
    assert_eq!(
        recorder.value(&format!("ratelimit_rejected_total{labels}")),
        1
    );
}
