//! Backend failure policies, timeouts, and recovery.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use actix_trypema::{
    BackendErrorPolicy, BackendFailure, ErrorAction, LimitOutcome, RateLimitInfo, derived_key,
};
use actix_web::{App, HttpResponse, Responder, http::StatusCode, test, web};
use trypema::{
    BucketSize, RateLimiterBuilder, WindowSize,
    redis::{AbsoluteRedisRateLimiter, SuppressedRedisRateLimiter},
};

use crate::{
    common::{CLIENT, CLIENT_KEY, WINDOW_SECS, get, status_or_error},
    flaky_proxy::FlakyProxy,
    support::{
        connection_manager, local_provider, per_ip, redis_backend, redis_url, unique_prefix,
    },
};

#[actix_web::test]
async fn backend_error_policies_decide_unreachable_backends() {
    let proxy = FlakyProxy::start();
    let short_timeout = Duration::from_millis(50);
    let local_fallback = local_provider(WINDOW_SECS);
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

    // FailClosed: 503, and the middleware's timeout, not the backend's, ends the wait. The
    // connection managers wait 30 s before they give up (`support::connection_manager`), so a
    // policy that fires within 5 s came from the 50 ms middleware timeout; 5 s is a wide margin
    // for a loaded machine.
    let started = Instant::now();
    assert_eq!(
        status_or_error(&app, get(CLIENT).uri("/closed").to_request()).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
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
    let other_window = local_provider(WINDOW_SECS + 1);

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
    let timeouts = Arc::new(AtomicUsize::new(0));
    let timeout_counter = Arc::clone(&timeouts);

    // FailClosed, plus a count of timeouts: a call that times out can still reach Redis and
    // count, and only that explains a rejection before the first admission after recovery.
    let limiter = per_ip(
        &redis_backend(&proxy.url(), &unique_prefix()).await,
        "recover",
        1.0,
    )
    .on_backend_error(BackendErrorPolicy::Custom(Arc::new(move |failure| {
        if matches!(failure, BackendFailure::Timeout(_)) {
            timeout_counter.fetch_add(1, Ordering::SeqCst);
        }

        ErrorAction::Reject {
            status: StatusCode::SERVICE_UNAVAILABLE,
        }
    })))
    .backend_timeout(Duration::from_secs(1))
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
    timeouts.store(0, Ordering::SeqCst);

    // The connection manager reconnects in the background; wait for the backend to decide.
    let deadline = Instant::now() + Duration::from_secs(30);

    let first_decision = loop {
        let status = status_or_error(&app, get(CLIENT).to_request()).await;

        if status != StatusCode::SERVICE_UNAVAILABLE {
            break status;
        }

        assert!(Instant::now() < deadline, "backend never recovered");
        actix_web::rt::time::sleep(Duration::from_millis(50)).await;
    };

    match first_decision {
        // The quota of one is spent by the admission.
        StatusCode::OK => {}
        // A call that timed out after the restore reached Redis and spent the quota.
        StatusCode::TOO_MANY_REQUESTS => assert!(
            timeouts.load(Ordering::SeqCst) > 0,
            "rejected before any admission, and no call timed out to spend the quota"
        ),
        other => panic!("unexpected status {other} after recovery"),
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

/// A single Redis limiter under a fresh prefix on `url`, on the shared window.
async fn redis_limiter<T>(url: &str) -> Arc<T>
where
    T: trypema::RateLimiterTarget<Backend = trypema::redis::RedisBackend>,
{
    RateLimiterBuilder::redis(connection_manager(url).await)
        .prefix(unique_prefix())
        .window_size(WindowSize::seconds_or_panic(WINDOW_SECS))
        .bucket_size(BucketSize::milliseconds_or_panic(10))
        .disable_cleanup()
        .build()
        .unwrap()
}

#[actix_web::test]
async fn fallback_window_must_match_a_limiter_primary() {
    let absolute: Arc<AbsoluteRedisRateLimiter> = redis_limiter(&redis_url()).await;
    let suppressed: Arc<SuppressedRedisRateLimiter> = redis_limiter(&redis_url()).await;

    let mismatched = per_ip(&absolute, "fallback", 1.0)
        .on_backend_error(BackendErrorPolicy::Fallback(local_provider(
            WINDOW_SECS + 1,
        )))
        .build();
    assert!(matches!(
        mismatched,
        Err(actix_trypema::ConfigError::FallbackWindowMismatch)
    ));

    let mismatched = per_ip(&suppressed, "fallback", 1.0)
        .on_backend_error(BackendErrorPolicy::Fallback(local_provider(
            WINDOW_SECS + 1,
        )))
        .build();
    assert!(matches!(
        mismatched,
        Err(actix_trypema::ConfigError::FallbackWindowMismatch)
    ));

    let matching = per_ip(&absolute, "fallback", 1.0)
        .on_backend_error(BackendErrorPolicy::Fallback(local_provider(WINDOW_SECS)))
        .build();
    assert!(matching.is_ok());
}

#[actix_web::test]
async fn a_provider_fallback_runs_the_strategy_of_a_limiter_primary() {
    let proxy = FlakyProxy::start();
    let short_timeout = Duration::from_millis(50);
    let local_fallback = local_provider(WINDOW_SECS);

    let absolute: Arc<AbsoluteRedisRateLimiter> = redis_limiter(&proxy.url()).await;
    let suppressed: Arc<SuppressedRedisRateLimiter> = redis_limiter(&proxy.url()).await;

    let absolute_limiter = per_ip(&absolute, "fb-abs", 2.0)
        .on_backend_error(BackendErrorPolicy::Fallback(local_fallback.clone()))
        .backend_timeout(short_timeout)
        .build()
        .unwrap();
    let suppressed_limiter = per_ip(&suppressed, "fb-sup", 2.0)
        .on_backend_error(BackendErrorPolicy::Fallback(local_fallback.clone()))
        .backend_timeout(short_timeout)
        .build()
        .unwrap();

    let app = test::init_service(
        App::new()
            .service(
                web::resource("/abs")
                    .wrap(absolute_limiter)
                    .to(|| async { "ok" }),
            )
            .service(
                web::resource("/sup")
                    .wrap(suppressed_limiter)
                    .to(|| async { "ok" }),
            ),
    )
    .await;

    proxy.cut();

    for path in ["/abs", "/sup"] {
        for expected in [
            StatusCode::OK,
            StatusCode::OK,
            StatusCode::TOO_MANY_REQUESTS,
        ] {
            assert_eq!(
                status_or_error(&app, get(CLIENT).uri(path).to_request()).await,
                expected,
                "{path}"
            );
        }
    }

    // The absolute primary counted on the absolute side of the provider, the suppressed
    // primary on the suppressed side.
    let absolute_key = derived_key("fb-abs", CLIENT_KEY);
    let suppressed_key = derived_key("fb-sup", CLIENT_KEY);
    assert_eq!(local_fallback.absolute().get(&absolute_key), 2);
    assert_eq!(local_fallback.suppressed().get(&absolute_key).total, 0);
    assert_eq!(local_fallback.absolute().get(&suppressed_key), 0);
    assert!(local_fallback.suppressed().get(&suppressed_key).total >= 2);

    // A suppressed rejection from the fallback carries its suppression factor.
    let rejected = test::call_service(&app, get(CLIENT).uri("/sup").to_request()).await;
    assert_eq!(rejected.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        rejected
            .headers()
            .get("x-ratelimit-suppression")
            .and_then(|value| value.to_str().ok()),
        Some("1.000")
    );
}
