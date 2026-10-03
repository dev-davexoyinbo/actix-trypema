//! The Redis and hybrid backends' service: the shared pipeline plus timeouts and failure policy.

use std::{
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use actix_web::{
    Error, HttpResponse,
    body::{EitherBody, MessageBody},
    dev::{Service, ServiceRequest, ServiceResponse, forward_ready},
    http::StatusCode,
};
use trypema::{TrypemaError, redis::RedisKey};

use crate::{
    BackendErrorPolicy, BackendFailure, ErrorAction,
    backend::{AsyncBackend, Local},
    builder::{Config, window_limit},
    middleware::{
        Decision, Mapped, PreDecision, TrypemaMiddleware, admitted_outcome, map_decision,
        permissive_outcome, pre_decide, record_backend_latency, record_decision, record_outcome,
        remaining,
    },
    outcome::LimitOutcome,
    response::{AdmitHeaders, build_rejection},
};

impl<S, Bd, B> Service<ServiceRequest> for TrypemaMiddleware<S, B>
where
    S: Service<ServiceRequest, Response = ServiceResponse<Bd>, Error = Error> + 'static,
    Bd: MessageBody + 'static,
    B: AsyncBackend,
{
    type Response = ServiceResponse<EitherBody<Bd>>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        // Everything that borrows the request runs before the first await.
        let pre = pre_decide(&self.config, &req);
        let config = Arc::clone(&self.config);
        let backend = self.backend.clone();
        let service = Rc::clone(&self.service);

        Box::pin(async move {
            let (key, rate, cost) = match pre {
                PreDecision::Bypass => {
                    record_decision(&config, Decision::Bypassed);
                    record_outcome(&config, &req, || LimitOutcome::Bypassed);
                    return forward(&*service, req, None).await;
                }
                PreDecision::KeyRejected(error) => {
                    record_decision(&config, Decision::Rejected);
                    return Err(error.into());
                }
                PreDecision::Proceed { key, rate, cost } => (key, rate, cost),
            };

            let limit = window_limit(&rate, config.window_size);

            // Derived keys are valid RedisKeys by construction (see `compose_key`).
            let redis_key = match RedisKey::try_from(key.as_str()) {
                Ok(redis_key) => redis_key,
                Err(error) => {
                    debug_assert!(false, "derived key rejected: {error}");
                    return Err(actix_web::error::ErrorInternalServerError(
                        "invalid derived rate limiting key",
                    ));
                }
            };

            let started = Instant::now();
            let result = timed(
                config.backend_timeout,
                backend.backend_inc(config.strategy, &redis_key, &rate, cost),
            )
            .await;
            record_backend_latency(&config, started.elapsed());

            let (decision, fallback) = match result {
                Ok(decision) => (decision, None),
                Err(failure) => {
                    // The failure is counted on its own; every request also counts exactly one
                    // verdict below (admitted or rejected), whichever path decides it.
                    record_decision(&config, Decision::BackendError);
                    warn_backend_failure(&config.namespace, &failure);

                    // Shadow mode never rejects, whatever the policy says.
                    if config.permissive {
                        record_decision(&config, Decision::Admitted);
                        record_outcome(&config, &req, || LimitOutcome::BackendError);
                        return forward(&*service, req, None).await;
                    }

                    if let BackendErrorPolicy::Fallback(local) = &config.backend_error {
                        let decision = local.inc(config.strategy, key.as_str(), &rate, cost);
                        (decision, Some(local.clone()))
                    } else {
                        let action = match &config.backend_error {
                            BackendErrorPolicy::FailClosed => ErrorAction::Reject {
                                status: StatusCode::SERVICE_UNAVAILABLE,
                            },
                            BackendErrorPolicy::Custom(decide) => decide(&failure),
                            // FailOpen; Fallback was handled above.
                            _ => ErrorAction::Admit,
                        };

                        return match action {
                            ErrorAction::Admit => {
                                record_decision(&config, Decision::Admitted);
                                record_outcome(&config, &req, || LimitOutcome::BackendError);
                                forward(&*service, req, None).await
                            }
                            ErrorAction::Reject { status } => {
                                record_decision(&config, Decision::Rejected);
                                let response = HttpResponse::build(status)
                                    .content_type("text/plain; charset=utf-8")
                                    .body("rate limiter unavailable");
                                Ok(req.into_response(response).map_into_right_body())
                            }
                        };
                    }
                }
            };

            match map_decision(&config, key.as_str(), limit, decision) {
                Mapped::Admit { suppression } => {
                    let remaining =
                        read_remaining(&config, &backend, fallback.as_ref(), &redis_key, limit)
                            .await;
                    record_decision(&config, Decision::Admitted);
                    record_outcome(&config, &req, || {
                        admitted_outcome(limit, remaining, suppression)
                    });
                    let headers = AdmitHeaders::new(&config, limit, remaining, suppression);
                    forward(&*service, req, headers).await
                }
                Mapped::Reject(info) => {
                    record_decision(&config, Decision::Rejected);

                    if config.permissive {
                        let remaining =
                            read_remaining(&config, &backend, fallback.as_ref(), &redis_key, limit)
                                .await;
                        record_outcome(&config, &req, || permissive_outcome(&info));
                        let headers =
                            AdmitHeaders::new(&config, limit, remaining, info.suppression_factor);
                        return forward(&*service, req, headers).await;
                    }

                    let response = build_rejection(&config, &info);
                    Ok(req.into_response(response).map_into_right_body())
                }
            }
        })
    } // end method call
} // end impl

async fn forward<S, Bd>(
    service: &S,
    req: ServiceRequest,
    headers: Option<AdmitHeaders>,
) -> Result<ServiceResponse<EitherBody<Bd>>, Error>
where
    S: Service<ServiceRequest, Response = ServiceResponse<Bd>, Error = Error>,
    Bd: MessageBody,
{
    let mut response = service.call(req).await?;

    if let Some(headers) = headers {
        headers.apply(response.headers_mut());
    }

    Ok(response.map_into_left_body())
} // end fn forward

/// Run one backend call under the configured timeout.
pub(crate) async fn timed<T>(
    timeout: Duration,
    call: impl Future<Output = Result<T, TrypemaError>>,
) -> Result<T, BackendFailure> {
    match actix_web::rt::time::timeout(timeout, call).await {
        Ok(result) => result.map_err(BackendFailure::Error),
        Err(_elapsed) => Err(BackendFailure::Timeout(timeout)),
    }
}

async fn read_remaining<B: AsyncBackend>(
    config: &Config,
    backend: &B,
    fallback: Option<&Local>,
    key: &RedisKey,
    limit: u64,
) -> Option<u64> {
    if !config.remaining_header {
        return None;
    }

    if let Some(local) = fallback {
        let (total, declined) = local.usage(config.strategy, key.as_str());
        return Some(remaining(limit, total, declined));
    }

    let usage = timed(
        config.backend_timeout,
        backend.backend_usage(config.strategy, key),
    )
    .await;

    // A failed remaining read only omits the header; admission already happened.
    usage
        .ok()
        .map(|(total, declined)| remaining(limit, total, declined))
} // end fn read_remaining

/// When the process last logged a backend failure.
static LAST_BACKEND_WARN: Mutex<Option<Instant>> = Mutex::new(None);

/// Log at most one warning per second per process, with the namespace and failure kind only —
/// never the key or the error text, which can carry request data or connection details.
fn warn_backend_failure(namespace: &str, failure: &BackendFailure) {
    let now = Instant::now();

    {
        let mut last = LAST_BACKEND_WARN
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if last.is_some_and(|last| now.duration_since(last) < Duration::from_secs(1)) {
            return;
        }

        *last = Some(now);
    }

    let kind = match failure {
        BackendFailure::Timeout(_) => "timeout",
        BackendFailure::Error(TrypemaError::RedisError(_)) => "redis",
        BackendFailure::Error(_) => "other",
    };

    tracing::warn!(namespace, kind, "rate limiter backend failure");
} // end fn warn_backend_failure
