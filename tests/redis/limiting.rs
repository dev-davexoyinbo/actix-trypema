//! Shared limits across instances, persisted keys, and cost.

use actix_trypema::derived_key;
use actix_web::{App, http::StatusCode, test, web};

use crate::{
    common::{CLIENT, CLIENT_KEY, WINDOW_SECS, get, header, per_ip, status_or_error},
    support::{redis_backend, redis_url, unique_prefix},
};

#[actix_web::test]
async fn instances_share_one_limit_and_persist_derived_keys() {
    let url = redis_url();
    let prefix = unique_prefix();

    // Two middleware instances on two backends with one prefix simulate two app instances.
    let first = per_ip(&redis_backend(&url, &prefix).await, "ip", 3.0)
        .build()
        .unwrap();
    let second = per_ip(&redis_backend(&url, &prefix).await, "ip", 3.0)
        .build()
        .unwrap();

    let app = test::init_service(
        App::new()
            .service(web::resource("/a").wrap(first).to(|| async { "ok" }))
            .service(web::resource("/b").wrap(second).to(|| async { "ok" })),
    )
    .await;

    for path in ["/a", "/b", "/a"] {
        assert_eq!(
            status_or_error(&app, get(CLIENT).uri(path).to_request()).await,
            StatusCode::OK,
            "{path}"
        );
    }

    let rejected = test::call_service(&app, get(CLIENT).uri("/b").to_request()).await;
    assert_eq!(
        rejected.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "3 shared admissions, then rejection across instances"
    );

    let retry_after: u64 = header(&rejected, "retry-after").unwrap().parse().unwrap();
    assert!((1..=WINDOW_SECS).contains(&retry_after), "{retry_after}");

    // The persisted keys embed the documented derived-key encoding.
    let mut sync_connection = redis::Client::open(url.as_str())
        .unwrap()
        .get_connection()
        .unwrap();
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{prefix}*"))
        .query(&mut sync_connection)
        .unwrap();

    let derived = derived_key("ip", CLIENT_KEY);
    assert!(!keys.is_empty());
    assert!(
        keys.iter().any(|key| key.contains(&derived)),
        "{keys:?} should embed {derived}"
    );
}

#[actix_web::test]
async fn cost_above_capacity_rejects_without_retry_after() {
    let url = redis_url();
    let limiter = per_ip(&redis_backend(&url, &unique_prefix()).await, "cost", 3.0)
        .cost_fn(|_| 5)
        .build()
        .unwrap();

    let app = test::init_service(
        App::new()
            .wrap(limiter)
            .route("/", web::get().to(|| async { "ok" })),
    )
    .await;

    // Redis rejects any cost above remaining capacity, and a zero hint means no retry-after:
    // advertising a wait would promise something that can never succeed.
    let response = test::call_service(&app, get(CLIENT).to_request()).await;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(header(&response, "retry-after").is_none());
}
