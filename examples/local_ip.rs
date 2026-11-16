//! Per-IP rate limiting with the in-process backend.
//!
//! Run with `cargo run --example local_ip`, then send six requests:
//! `for i in $(seq 6); do curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/; done`
//! The first five print `200` and the sixth `429`. `/healthz` is never limited.

use std::sync::Arc;

use actix_trypema::{
    TrypemaLimiter,
    extract::PeerIp,
    trypema::{RateLimit, RateLimiterBuilder, WindowSize, local::AbsoluteLocalRateLimiter},
};
use actix_web::{App, HttpServer, web};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Build the backend ONCE, outside HttpServer::new. Inside the closure every worker would
    // get its own limiter, multiplying the effective limit by the worker count.
    let backend: Arc<AbsoluteLocalRateLimiter> = RateLimiterBuilder::local()
        .window_size(WindowSize::seconds_or_panic(60))
        .build()
        .expect("valid backend");

    let limiter = TrypemaLimiter::builder(backend)
        .namespace("ip")
        .extractor(PeerIp::default())
        .rate(RateLimit::per_minute_or_panic(5.0))
        .exclude(|req| req.path() == "/healthz")
        .build()
        .expect("valid middleware configuration");

    HttpServer::new(move || {
        App::new()
            .wrap(limiter.clone())
            .route("/", web::get().to(|| async { "hello\n" }))
            .route("/healthz", web::get().to(|| async { "ok\n" }))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
