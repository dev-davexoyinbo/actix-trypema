//! The middleware's own admit path and allow-list lookup allocate nothing, for the client-IP and
//! header extractors.
//!
//! This lives in its own test binary because it installs a global counting allocator, which
//! needs `unsafe impl` — forbidden inside the crate itself. The `metrics` feature is excluded:
//! the `metrics` facade builds a labelled key, which allocates, on every recorded decision.

#![cfg(not(feature = "metrics"))]

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    net::SocketAddr,
};

use actix_trypema::{
    HeaderMode, Local, TrypemaLimiter,
    extract::{Header, KeyExtractor, PeerIp, RealIp, TrustedProxies},
};
use actix_web::{
    HttpResponse,
    dev::{Service, ServiceRequest, Transform, fn_service},
    test::TestRequest,
};
use trypema::{RateLimit, WindowSize};

/// Counts allocations made on the current thread while counting is switched on.
struct CountingAllocator;

thread_local! {
    static IS_COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATION_COUNT: Cell<usize> = const { Cell::new(0) };
}

// SAFETY: every method forwards to the system allocator unchanged; counting only touches
// const-initialized thread locals, which never allocate.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if IS_COUNTING.try_with(Cell::get).unwrap_or(false) {
            let _ = ALLOCATION_COUNT.try_with(|count| count.set(count.get() + 1));
        }

        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn count_allocations(work: impl FnOnce()) -> usize {
    ALLOCATION_COUNT.with(|count| count.set(0));
    IS_COUNTING.with(|flag| flag.set(true));
    work();
    IS_COUNTING.with(|flag| flag.set(false));
    ALLOCATION_COUNT.with(Cell::get)
}

fn request_from(peer: &str) -> TestRequest {
    TestRequest::default().peer_addr(peer.parse::<SocketAddr>().unwrap())
}

/// The middleware over a no-op handler, keyed by `extractor`, with headers off.
///
/// The handler's future captures the request without allocating, so `call()` measures only the
/// middleware's work: extraction, key composition, the limiter, and forwarding.
async fn middleware_for(
    extractor: impl KeyExtractor,
    allow_keys: &[&str],
) -> impl Service<ServiceRequest, Error = actix_web::Error> {
    let limiter = TrypemaLimiter::builder(Local::new(WindowSize::seconds_or_panic(60)).unwrap())
        .namespace("alloc")
        .extractor(extractor)
        .rate(RateLimit::per_second_or_panic(1e9))
        .headers(HeaderMode::Off)
        .allow_keys(allow_keys.iter().copied())
        .build()
        .unwrap();

    limiter
        .new_transform(fn_service(|req: ServiceRequest| async move {
            Ok::<_, actix_web::Error>(req.into_response(HttpResponse::Ok().finish()))
        }))
        .await
        .unwrap()
}

/// Allocations made by `call()` once the request's key is known to the provider.
async fn steady_state_allocations(
    middleware: &impl Service<ServiceRequest, Error = actix_web::Error>,
    request: impl Fn() -> ServiceRequest,
) -> usize {
    // The first request inserts the key into the provider, which allocates.
    middleware.call(request()).await.unwrap();

    let measured = request();
    count_allocations(|| drop(middleware.call(measured)))
}

#[actix_web::test]
async fn peer_ip_admit_path_and_allow_list_do_not_allocate() {
    let middleware = middleware_for(PeerIp::default(), &["198.51.100.1"]).await;

    let admitted = || request_from("203.0.113.7:9000").to_srv_request();
    let allow_listed = || request_from("198.51.100.1:9000").to_srv_request();

    assert_eq!(
        steady_state_allocations(&middleware, admitted).await,
        0,
        "admit path"
    );
    assert_eq!(
        steady_state_allocations(&middleware, allow_listed).await,
        0,
        "allow-list bypass"
    );
}

#[actix_web::test]
async fn real_ip_behind_a_trusted_proxy_does_not_allocate() {
    let extractor = RealIp::xff(TrustedProxies::new_or_panic(["10.0.0.0/8"]));
    let middleware = middleware_for(extractor, &[]).await;

    let forwarded = || {
        request_from("10.0.0.1:9000")
            .insert_header(("x-forwarded-for", "203.0.113.9"))
            .to_srv_request()
    };

    assert_eq!(steady_state_allocations(&middleware, forwarded).await, 0);
}

#[actix_web::test]
async fn header_keys_do_not_allocate() {
    let extractors = [
        ("raw", Header::new_or_panic("x-api-key")),
        ("hashed", Header::new_or_panic("x-api-key").hashed()),
    ];

    for (label, extractor) in extractors {
        let middleware = middleware_for(extractor, &[]).await;

        let keyed = || {
            request_from("203.0.113.7:9000")
                .insert_header(("x-api-key", "key-1"))
                .to_srv_request()
        };

        assert_eq!(
            steady_state_allocations(&middleware, keyed).await,
            0,
            "{label}"
        );
    }
}
