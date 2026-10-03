//! Plan-tier rates keyed on a validated API key.
//!
//! Keys are checked against a known set before they become rate-limiting keys — never key on a
//! raw, unauthenticated header, since every attacker-chosen value would mint a fresh bucket.
//! The tier is part of the key (trypema rates are sticky per key), so a plan change lands in a
//! new bucket.
//!
//! Run with `cargo run --example api_key_tiers`, then compare the tiers:
//! `for i in $(seq 4); do curl -s -o /dev/null -w '%{http_code} ' -H 'x-api-key: basic-key' localhost:8080/; done`
//! prints `200 200 200 429`, while the same loop with `pro-key` prints only `200`s. An unknown
//! key gets `400`.

use actix_trypema::{KeyError, Local, TrypemaLimiter};
use actix_web::{App, HttpServer, web};
use trypema::{RateLimit, WindowSize};

/// Stand-in for a real key store: API key → (account id, plan).
const API_KEYS: [(&str, &str, &str); 2] = [
    ("basic-key", "account-1", "basic"),
    ("pro-key", "account-2", "pro"),
];

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let backend = Local::new(WindowSize::seconds_or_panic(60)).expect("valid backend");

    let limiter = TrypemaLimiter::builder(backend)
        .namespace("apikey")
        // The key is `{plan}:{account}`: validated, credential-free, and tier-aware.
        .extractor_fn(|req| {
            let presented = req
                .headers()
                .get("x-api-key")
                .and_then(|value| value.to_str().ok())
                .ok_or(KeyError::InvalidValue {
                    reason: "missing api key",
                })?;

            let (_, account, plan) = API_KEYS
                .iter()
                .find(|(key, _, _)| *key == presented)
                .ok_or(KeyError::InvalidValue {
                    reason: "unknown api key",
                })?;

            Ok(format!("{plan}:{account}"))
        })
        .rate_fn(|_req, key| {
            if key.starts_with("pro:") {
                RateLimit::per_minute_or_panic(100.0)
            } else {
                RateLimit::per_minute_or_panic(3.0)
            }
        })
        .remaining_header(true)
        .build()
        .expect("valid middleware configuration");

    HttpServer::new(move || {
        App::new()
            .wrap(limiter.clone())
            .route("/", web::get().to(|| async { "hello\n" }))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
