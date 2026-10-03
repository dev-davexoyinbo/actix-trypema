//! A distributed login limiter: Redis-backed, keyed by the real client IP behind a load
//! balancer, failing closed because this route is security-sensitive.
//!
//! Needs Redis (see `compose.yaml`):
//! `REDIS_URL=redis://127.0.0.1:16379/ cargo run --example redis_login --features redis`
//!
//! Six `curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8080/login` calls print five
//! `200`s and a `429`; the limit is shared by every instance pointing at the same Redis. Stop
//! Redis (`docker compose stop`) and logins get `503` instead of slipping through.

use std::time::Duration;

use actix_trypema::{
    BackendErrorPolicy, Redis, TrypemaLimiter,
    extract::{RealIp, TrustedProxies},
};
use actix_web::{App, HttpServer, web};
use trypema::{RateLimit, WindowSize};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:16379/".to_string());
    let connection = redis::Client::open(url)
        .expect("valid REDIS_URL")
        .get_connection_manager()
        .await
        .expect("redis reachable");

    let backend = Redis::new(connection, WindowSize::minutes_or_panic(1)).expect("valid backend");

    // Headers are only trusted when the TCP peer is one of these load balancers; everything
    // else is keyed by its own address. A limiter outage rejects logins instead of waving
    // brute force through.
    let limiter = TrypemaLimiter::builder(backend)
        .namespace("login")
        .extractor(RealIp::xff(TrustedProxies::new_or_panic(["10.0.0.0/8"])))
        .rate(RateLimit::per_minute_or_panic(5.0))
        .on_backend_error(BackendErrorPolicy::FailClosed)
        .backend_timeout(Duration::from_millis(30))
        .build()
        .expect("valid middleware configuration");

    HttpServer::new(move || {
        App::new().service(
            web::resource("/login")
                .wrap(limiter.clone())
                .route(web::post().to(|| async { "login attempt\n" })),
        )
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
