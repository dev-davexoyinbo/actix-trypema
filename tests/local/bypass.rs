//! Exclusions, allow lists, and zero-cost requests.

use actix_web::{
    App,
    http::{Method, StatusCode},
    test, web,
};

use crate::{
    common::{CLIENT, CLIENT_KEY, get, header, per_ip},
    support::{OTHER_CLIENT, backend, usage},
};

#[actix_web::test]
async fn exclusion_allow_list_and_zero_cost_bypass_the_limiter() {
    let backend = backend();

    let post_only = per_ip(&backend, "m", 1.0)
        .exclude(|req| req.method() != Method::POST)
        .build()
        .unwrap();
    let health_excluded = per_ip(&backend, "x", 1.0)
        .exclude(|req| req.path() == "/healthz")
        .build()
        .unwrap();
    let allow_listed = per_ip(&backend, "a", 1.0)
        .allow_keys([CLIENT_KEY])
        .build()
        .unwrap();
    let free = per_ip(&backend, "c", 1.0).cost_fn(|_| 0).build().unwrap();

    let app = test::init_service(
        App::new()
            .service(web::resource("/m").wrap(post_only).to(|| async { "ok" }))
            .service(
                web::resource("/healthz")
                    .wrap(health_excluded)
                    .to(|| async { "ok" }),
            )
            .service(web::resource("/a").wrap(allow_listed).to(|| async { "ok" }))
            .service(web::resource("/c").wrap(free).to(|| async { "ok" })),
    )
    .await;

    for path in ["/m", "/healthz", "/a", "/c"] {
        for _ in 0..3 {
            let response = test::call_service(&app, get(CLIENT).uri(path).to_request()).await;
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            assert_eq!(header(&response, "x-ratelimit-limit"), None, "{path}");
        }
    }

    // Bypasses never reached the limiter: no usage recorded under any namespace.
    for namespace in ["m", "x", "a", "c"] {
        assert_eq!(usage(&backend, namespace), 0, "{namespace}");
    }

    // The exclusion is precise: POSTs on the POST-only route are limited.
    let first_post = test::call_service(
        &app,
        get(CLIENT).method(Method::POST).uri("/m").to_request(),
    )
    .await;
    assert_eq!(first_post.status(), StatusCode::OK);
    let second_post = test::call_service(
        &app,
        get(CLIENT).method(Method::POST).uri("/m").to_request(),
    )
    .await;
    assert_eq!(second_post.status(), StatusCode::TOO_MANY_REQUESTS);

    // The allow list is per extracted key: another client on /a is limited normally.
    let first = test::call_service(&app, get(OTHER_CLIENT).uri("/a").to_request()).await;
    assert_eq!(first.status(), StatusCode::OK);
    let second = test::call_service(&app, get(OTHER_CLIENT).uri("/a").to_request()).await;
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[actix_web::test]
async fn exclusions_and_allow_lists_accumulate_across_calls() {
    let backend = backend();
    let limiter = per_ip(&backend, "acc", 1.0)
        .exclude(|req| req.path() == "/healthz")
        .exclude(|req| req.path() == "/metrics")
        .allow_keys(["198.51.100.1"])
        .allow_keys(["198.51.100.2"])
        .build()
        .unwrap();

    let app = test::init_service(
        App::new()
            .wrap(limiter)
            .default_service(web::to(|| async { "ok" })),
    )
    .await;

    // Both exclusions hold, not just the last one.
    for path in ["/healthz", "/metrics"] {
        for _ in 0..2 {
            let response = test::call_service(&app, get(CLIENT).uri(path).to_request()).await;
            assert_eq!(response.status(), StatusCode::OK, "{path}");
        }
    }

    // Both allow-list entries hold.
    for peer in ["198.51.100.1:9000", "198.51.100.2:9000"] {
        for _ in 0..2 {
            let response = test::call_service(&app, get(peer).to_request()).await;
            assert_eq!(response.status(), StatusCode::OK, "{peer}");
        }
    }

    // Everything else is still limited.
    test::call_service(&app, get(CLIENT).to_request()).await;
    let limited = test::call_service(&app, get(CLIENT).to_request()).await;
    assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
}
