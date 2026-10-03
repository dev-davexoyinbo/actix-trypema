//! Decision counters on backend failures, behind the `metrics` feature.

use std::time::Duration;

use actix_trypema::BackendErrorPolicy;
use actix_web::{
    App,
    test::{call_service, init_service},
    web,
};

use crate::{
    common::{CLIENT, get, per_ip, recorder::CountingRecorder},
    flaky_proxy::FlakyProxy,
    support::{redis_backend, unique_prefix},
};

#[test]
fn backend_failures_count_one_verdict_each() {
    let recorder = CountingRecorder::default();

    metrics::with_local_recorder(&recorder, || {
        actix_web::rt::System::new().block_on(async {
            let proxy = FlakyProxy::start();
            let fail_open = per_ip(
                &redis_backend(&proxy.url(), &unique_prefix()).await,
                "open",
                1.0,
            )
            .backend_timeout(Duration::from_millis(50))
            .build()
            .unwrap();
            let fail_closed = per_ip(
                &redis_backend(&proxy.url(), &unique_prefix()).await,
                "closed",
                1.0,
            )
            .on_backend_error(BackendErrorPolicy::FailClosed)
            .backend_timeout(Duration::from_millis(50))
            .build()
            .unwrap();

            let app = init_service(
                App::new()
                    .service(web::resource("/open").wrap(fail_open).to(|| async { "ok" }))
                    .service(
                        web::resource("/closed")
                            .wrap(fail_closed)
                            .to(|| async { "ok" }),
                    ),
            )
            .await;

            proxy.cut();

            for path in ["/open", "/closed"] {
                call_service(&app, get(CLIENT).uri(path).to_request()).await;
            }
        });
    });

    // Each failure counts once, plus exactly one verdict: FailOpen admits, FailClosed rejects.
    for (namespace, verdict) in [("open", "admitted"), ("closed", "rejected")] {
        let labels = format!("{{namespace={namespace},strategy=absolute}}");
        assert_eq!(
            recorder.value(&format!("ratelimit_backend_errors_total{labels}")),
            1,
            "{namespace}"
        );
        assert_eq!(
            recorder.value(&format!("ratelimit_{verdict}_total{labels}")),
            1,
            "{namespace}"
        );
    }
}
