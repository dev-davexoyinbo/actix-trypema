//! Shadow (permissive) mode on the hybrid backend: every request is admitted, and the handler
//! reads what enforcement would have done.
//!
//! The Suppressed strategy records declined usage, so shadow accounting stays accurate under
//! overload — with Absolute, rejected calls record nothing.
//!
//! Needs Redis (see `compose.yaml`):
//! `REDIS_URL=redis://127.0.0.1:16379/ cargo run --example hybrid_shadow_mode --features redis`
//!
//! `for i in $(seq 30); do curl -s localhost:8080/; done` prints only `200` responses, switching
//! from `within limits` to `would be shed` once the 2-per-second budget is spent.

use actix_trypema::{
    Hybrid, LimitOutcome, RateLimitInfo, Strategy, TrypemaLimiter, extract::PeerIp,
};
use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use trypema::{RateLimit, WindowSize};

async fn handler(info: RateLimitInfo) -> impl Responder {
    match info.outcome() {
        LimitOutcome::Suppressed {
            factor,
            admitted: false,
            ..
        } => HttpResponse::Ok().body(format!("would be shed (factor {factor:.3})\n")),
        _ => HttpResponse::Ok().body("within limits\n"),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:16379/".to_string());
    let connection = redis::Client::open(url)
        .expect("valid REDIS_URL")
        .get_connection_manager()
        .await
        .expect("redis reachable");

    let backend = Hybrid::new(connection, WindowSize::seconds_or_panic(10)).expect("valid backend");

    let limiter = TrypemaLimiter::builder(backend)
        .namespace("shadow")
        .extractor(PeerIp::default())
        .rate(RateLimit::per_second_or_panic(2.0))
        .strategy(Strategy::Suppressed)
        .permissive(true)
        .build()
        .expect("valid middleware configuration");

    HttpServer::new(move || {
        App::new()
            .wrap(limiter.clone())
            .route("/", web::get().to(handler))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
