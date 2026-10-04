//! The request pipeline shared by every backend, and the local backend's service.

use std::{
    future::{Future, Ready, ready},
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll},
};

use actix_web::{
    Error,
    body::{EitherBody, MessageBody},
    dev::{Service, ServiceRequest, ServiceResponse, Transform, forward_ready},
};
use pin_project_lite::pin_project;
use trypema::{RateLimit, RateLimitDecision};

use crate::{
    KeyError, KeyErrorPolicy,
    backend::Local,
    builder::{Config, TrypemaLimiter, window_limit},
    extract::KeyBuf,
    key::compose_key,
    outcome::{self, LimitOutcome},
    response::{AdmitHeaders, RejectionInfo, build_rejection, nonzero_duration},
};

/// The middleware service produced by wrapping a [`TrypemaLimiter`].
pub struct TrypemaMiddleware<S, B> {
    pub(crate) service: Rc<S>,
    pub(crate) config: Arc<Config>,
    pub(crate) backend: B,
}

impl<S, B, Bd> Transform<S, ServiceRequest> for TrypemaLimiter<B>
where
    S: Service<ServiceRequest, Response = ServiceResponse<Bd>, Error = Error>,
    Bd: MessageBody,
    B: Clone,
    TrypemaMiddleware<S, B>:
        Service<ServiceRequest, Response = ServiceResponse<EitherBody<Bd>>, Error = Error>,
{
    type Response = ServiceResponse<EitherBody<Bd>>;
    type Error = Error;
    type Transform = TrypemaMiddleware<S, B>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(TrypemaMiddleware {
            service: Rc::new(service),
            config: Arc::clone(&self.config),
            backend: self.backend.clone(),
        }))
    } // end method new_transform
} // end impl

impl<S, Bd> Service<ServiceRequest> for TrypemaMiddleware<S, Local>
where
    S: Service<ServiceRequest, Response = ServiceResponse<Bd>, Error = Error>,
    Bd: MessageBody,
{
    type Response = ServiceResponse<EitherBody<Bd>>;
    type Error = Error;
    type Future = LimiterFuture<S::Future, Bd>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let config = &self.config;

        let (key, rate, cost) = match pre_decide(config, &req) {
            PreDecision::Bypass => {
                record_decision(config, Decision::Bypassed);
                record_outcome(config, &req, || LimitOutcome::Bypassed);
                return LimiterFuture::forward(self.service.call(req), None);
            }
            PreDecision::KeyRejected(error) => {
                record_decision(config, Decision::Rejected);
                return LimiterFuture::ready(Err(error.into()));
            }
            PreDecision::Proceed { key, rate, cost } => (key, rate, cost),
        };

        let limit = window_limit(&rate, config.window_size);
        let decision = self.backend.inc(config.strategy, key.as_str(), &rate, cost);

        finish(
            config,
            &*self.service,
            req,
            key.as_str(),
            limit,
            decision,
            || self.read_remaining(key.as_str(), limit),
        )
    } // end method call
} // end impl

impl<S> TrypemaMiddleware<S, Local> {
    fn read_remaining(&self, key: &str, limit: u64) -> Option<u64> {
        self.config.remaining_header.then(|| {
            let (total, declined) = self.backend.usage(self.config.strategy, key);
            remaining(limit, total, declined)
        })
    }
} // end impl

/// The response future of a decision that awaits Redis.
pub(crate) type BoxedResponse<Bd> =
    Pin<Box<dyn Future<Output = Result<ServiceResponse<EitherBody<Bd>>, Error>>>>;

pin_project! {
    /// The middleware's service future.
    ///
    /// Unboxed whenever the decision is made without awaiting: always for the local backend,
    /// and for most hybrid requests.
    pub struct LimiterFuture<F, Bd> {
        #[pin]
        state: LimiterFutureState<F, Bd>,
    }
}

pin_project! {
    #[project = LimiterFutureStateProj]
    enum LimiterFutureState<F, Bd> {
        Forward {
            #[pin]
            fut: F,
            headers: Option<AdmitHeaders>,
        },
        Ready {
            response: Option<Result<ServiceResponse<EitherBody<Bd>>, Error>>,
        },
        Boxed {
            fut: BoxedResponse<Bd>,
        },
    }
}

impl<F, Bd> LimiterFuture<F, Bd> {
    fn forward(fut: F, headers: Option<AdmitHeaders>) -> Self {
        Self {
            state: LimiterFutureState::Forward { fut, headers },
        }
    }

    fn ready(response: Result<ServiceResponse<EitherBody<Bd>>, Error>) -> Self {
        Self {
            state: LimiterFutureState::Ready {
                response: Some(response),
            },
        }
    }

    #[cfg_attr(
        not(feature = "redis"),
        expect(dead_code, reason = "only the async backends box")
    )]
    pub(crate) fn boxed(fut: BoxedResponse<Bd>) -> Self {
        Self {
            state: LimiterFutureState::Boxed { fut },
        }
    }
} // end impl

impl<F, Bd> Future for LimiterFuture<F, Bd>
where
    F: Future<Output = Result<ServiceResponse<Bd>, Error>>,
    Bd: MessageBody,
{
    type Output = Result<ServiceResponse<EitherBody<Bd>>, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().state.project() {
            LimiterFutureStateProj::Forward { fut, headers } => match fut.poll(cx) {
                Poll::Ready(Ok(mut response)) => {
                    if let Some(headers) = headers.take() {
                        headers.apply(response.headers_mut());
                    }

                    Poll::Ready(Ok(response.map_into_left_body()))
                }
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Pending => Poll::Pending,
            },
            // Futures are not polled again after completing, so the response is always present.
            LimiterFutureStateProj::Ready { response } => Poll::Ready(
                response
                    .take()
                    .expect("LimiterFuture polled after completion"),
            ),
            LimiterFutureStateProj::Boxed { fut } => fut.as_mut().poll(cx),
        }
    } // end method poll
} // end impl

#[expect(
    clippy::large_enum_variant,
    reason = "KeyBuf stays inline; boxing it would defeat the allocation-free admit path"
)]
pub(crate) enum PreDecision {
    Bypass,
    KeyRejected(KeyError),
    Proceed {
        key: KeyBuf,
        rate: RateLimit,
        cost: u64,
    },
}

/// The synchronous pipeline prefix: exclusion, key extraction, allow list, rate and cost.
///
/// The composed key is owned by the returned `KeyBuf`, so nothing borrows the request
/// afterwards.
pub(crate) fn pre_decide(config: &Config, req: &ServiceRequest) -> PreDecision {
    if let Some(exclude) = &config.exclude
        && exclude(req)
    {
        return PreDecision::Bypass;
    }

    let mut raw = KeyBuf::new();
    let extracted = match config.extractor.extract(req, &mut raw) {
        Ok(extracted) => extracted,
        Err(error) => {
            // Key errors carry configuration (header names), never request contents.
            tracing::debug!(
                namespace = &*config.namespace,
                %error,
                "rate limiting key extraction failed"
            );

            match config.key_error_policy {
                // Shadow mode never rejects, whatever the policy says.
                KeyErrorPolicy::Reject if config.permissive => return PreDecision::Bypass,
                KeyErrorPolicy::Reject => return PreDecision::KeyRejected(error),
                KeyErrorPolicy::Bypass => return PreDecision::Bypass,
                KeyErrorPolicy::UseGlobalKey => "",
            }
        }
    };

    if !config.allow_list.is_empty() && config.allow_list.contains(extracted) {
        return PreDecision::Bypass;
    }

    let rate = config.rate.resolve(req, extracted);
    let cost = config.cost.as_ref().map_or(1, |cost| cost(req));

    if cost == 0 {
        return PreDecision::Bypass;
    }

    let mut key = KeyBuf::new();
    compose_key(&mut key, &config.namespace, extracted);

    PreDecision::Proceed { key, rate, cost }
} // end fn pre_decide

/// Turn a backend decision into the response: forward (with headers) or reject.
///
/// `read_remaining` runs only when the remaining count is reported, on admission or in
/// permissive mode.
pub(crate) fn finish<S, Bd>(
    config: &Config,
    service: &S,
    req: ServiceRequest,
    key: &str,
    limit: u64,
    decision: RateLimitDecision,
    read_remaining: impl FnOnce() -> Option<u64>,
) -> LimiterFuture<S::Future, Bd>
where
    S: Service<ServiceRequest, Response = ServiceResponse<Bd>, Error = Error>,
{
    match map_decision(config, key, limit, decision) {
        Mapped::Admit { suppression } => {
            let remaining = read_remaining();
            record_decision(config, Decision::Admitted);
            record_outcome(config, &req, || {
                admitted_outcome(limit, remaining, suppression)
            });
            LimiterFuture::forward(
                service.call(req),
                AdmitHeaders::new(config, limit, remaining, suppression),
            )
        }
        Mapped::Reject(info) => {
            record_decision(config, Decision::Rejected);

            if config.permissive {
                let remaining = read_remaining();
                record_outcome(config, &req, || permissive_outcome(&info));
                return LimiterFuture::forward(
                    service.call(req),
                    AdmitHeaders::new(config, limit, remaining, info.suppression_factor),
                );
            }

            let response = build_rejection(config, &info);
            LimiterFuture::ready(Ok(req.into_response(response).map_into_right_body()))
        }
    }
} // end fn finish

pub(crate) enum Mapped {
    Admit { suppression: Option<f64> },
    Reject(RejectionInfo),
}

pub(crate) fn map_decision(
    config: &Config,
    key: &str,
    limit: u64,
    decision: RateLimitDecision,
) -> Mapped {
    let reject = |retry_after, remaining_after_waiting, suppression_factor| {
        Mapped::Reject(RejectionInfo {
            namespace: Arc::clone(&config.namespace),
            key: Arc::from(key),
            strategy: config.strategy,
            limit,
            retry_after,
            remaining_after_waiting,
            suppression_factor,
        })
    };

    match decision {
        RateLimitDecision::Allowed => Mapped::Admit { suppression: None },
        RateLimitDecision::Suppressed {
            is_allowed: true,
            suppression_factor,
            ..
        } => Mapped::Admit {
            suppression: Some(suppression_factor),
        },
        RateLimitDecision::Rejected {
            retry_after,
            remaining_after_waiting,
            ..
        } => reject(
            nonzero_duration(retry_after),
            Some(remaining_after_waiting),
            None,
        ),
        RateLimitDecision::Suppressed {
            is_allowed: false,
            suppression_factor,
            ..
        } => reject(config.suppressed_retry_hint, None, Some(suppression_factor)),
    }
} // end fn map_decision

pub(crate) fn admitted_outcome(
    limit: u64,
    remaining: Option<u64>,
    suppression: Option<f64>,
) -> LimitOutcome {
    match suppression {
        Some(factor) => LimitOutcome::Suppressed {
            factor,
            admitted: true,
        },
        None => LimitOutcome::Admitted { limit, remaining },
    }
}

pub(crate) fn permissive_outcome(info: &RejectionInfo) -> LimitOutcome {
    match info.suppression_factor {
        Some(factor) => LimitOutcome::Suppressed {
            factor,
            admitted: false,
        },
        None => LimitOutcome::Limited {
            retry_after: info.retry_after,
        },
    }
}

/// Record the outcome for handlers, when this instance records outcomes at all.
pub(crate) fn record_outcome(
    config: &Config,
    req: &ServiceRequest,
    outcome: impl FnOnce() -> LimitOutcome,
) {
    if config.records_outcome {
        outcome::record(req, &config.namespace, outcome());
    }
}

/// The saturating remaining-quota computation shared by every strategy.
pub(crate) fn remaining(limit: u64, total: u64, declined: u64) -> u64 {
    limit.saturating_sub(total.saturating_sub(declined))
}

/// A per-request decision, as counted by the `metrics` feature.
#[derive(Clone, Copy)]
pub(crate) enum Decision {
    Admitted,
    Rejected,
    Bypassed,
    #[cfg_attr(
        not(feature = "redis"),
        expect(dead_code, reason = "only the async backends fail")
    )]
    BackendError,
}

pub(crate) fn record_decision(config: &Config, decision: Decision) {
    #[cfg(feature = "metrics")]
    {
        let counter = match decision {
            Decision::Admitted => "ratelimit_admitted_total",
            Decision::Rejected => "ratelimit_rejected_total",
            Decision::Bypassed => "ratelimit_bypassed_total",
            Decision::BackendError => "ratelimit_backend_errors_total",
        };

        // Sharing the namespace Arc keeps labels free of per-request string copies.
        metrics::counter!(
            counter,
            "namespace" => metrics::SharedString::from(Arc::clone(&config.namespace)),
            "strategy" => config.strategy.as_str(),
        )
        .increment(1);
    }

    #[cfg(not(feature = "metrics"))]
    let _ = (config, decision);
} // end fn record_decision

#[cfg(feature = "redis")]
pub(crate) fn record_backend_latency(config: &Config, elapsed: std::time::Duration) {
    #[cfg(feature = "metrics")]
    metrics::histogram!(
        "ratelimit_backend_latency_seconds",
        "namespace" => metrics::SharedString::from(Arc::clone(&config.namespace)),
        "strategy" => config.strategy.as_str(),
    )
    .record(elapsed.as_secs_f64());

    #[cfg(not(feature = "metrics"))]
    let _ = (config, elapsed);
} // end fn record_backend_latency
