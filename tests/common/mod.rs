//! Helpers shared by the integration test binaries.

#[cfg(feature = "metrics")]
pub mod recorder;

use std::net::SocketAddr;

use actix_trypema::{Backend, TrypemaLimiter, TrypemaLimiterBuilder, extract::PeerIp};
use actix_web::{
    body::MessageBody,
    dev::{Service, ServiceResponse},
    http::StatusCode,
    test::{self, TestRequest},
};
use trypema::RateLimit;

/// The client address requests come from.
pub const CLIENT: &str = "203.0.113.7:9000";

/// The key `PeerIp` extracts for `CLIENT`.
pub const CLIENT_KEY: &str = "203.0.113.7";

/// The window every test backend uses.
pub const WINDOW_SECS: u64 = 60;

/// A GET request from `peer`.
pub fn get(peer: &str) -> TestRequest {
    TestRequest::default().peer_addr(peer.parse::<SocketAddr>().unwrap())
}

/// A response header as text, when present and valid.
pub fn header<'a>(response: &'a ServiceResponse<impl MessageBody>, name: &str) -> Option<&'a str> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
}

/// A per-IP limiter on `backend` allowing `limit` requests per 60-second window.
pub fn per_ip<B: Backend>(backend: &B, namespace: &str, limit: f64) -> TrypemaLimiterBuilder<B> {
    TrypemaLimiter::builder(backend.clone())
        .namespace(namespace)
        .extractor(PeerIp::default())
        .rate(RateLimit::per_minute_or_panic(limit))
}

/// Status of a response, or of the error a handler extractor produced.
pub async fn status_or_error<S, R, B>(app: &S, req: R) -> StatusCode
where
    S: Service<R, Response = ServiceResponse<B>, Error = actix_web::Error>,
{
    match test::try_call_service(app, req).await {
        Ok(response) => response.status(),
        Err(error) => error.as_response_error().status_code(),
    }
}
