//! The middleware factory and its builder.

use std::{collections::HashSet, fmt, sync::Arc, time::Duration};

use actix_web::dev::ServiceRequest;
use trypema::{RateLimit, WindowSize};

use crate::{
    BackendErrorPolicy, ConfigError, KeyError, KeyErrorPolicy,
    backend::Backend,
    extract::{FnExtractor, KeyExtractor},
    key::validate_namespace,
    response::{DefaultResponder, RejectionResponder, nonzero_duration},
};

#[cfg(feature = "redis")]
use crate::backend::AsyncBackend;

const DEFAULT_BACKEND_TIMEOUT: Duration = Duration::from_millis(50);

pub(crate) type RateFn = Arc<dyn Fn(&ServiceRequest, &str) -> RateLimit + Send + Sync>;
pub(crate) type CostFn = Arc<dyn Fn(&ServiceRequest) -> u64 + Send + Sync>;
pub(crate) type ExcludeFn = Arc<dyn Fn(&ServiceRequest) -> bool + Send + Sync>;

/// Which trypema strategy admits requests.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Strategy {
    /// Sliding-window allow/reject admission. The default.
    #[default]
    Absolute,
    /// Probabilistic suppression for graceful degradation under overload.
    Suppressed,
}

impl Strategy {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Absolute => "absolute",
            Self::Suppressed => "suppressed",
        }
    }
} // end impl

/// Which rate limit headers responses carry. Bypassed requests never carry any.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum HeaderMode {
    /// No rate limit headers. Recommended for authentication endpoints, since limit headers
    /// aid reconnaissance.
    Off,
    /// `x-ratelimit-limit`, plus `x-ratelimit-remaining` with
    /// [`remaining_header`](TrypemaLimiterBuilder::remaining_header) and
    /// `x-ratelimit-suppression` on suppressed admits. The default.
    #[default]
    Legacy,
    /// `ratelimit-policy` and `ratelimit` structured fields per
    /// draft-ietf-httpapi-ratelimit-headers-09. Unstable while the draft moves.
    ///
    /// The draft makes the remaining count mandatory in `ratelimit`, so admitted responses carry
    /// it only with [`remaining_header`](TrypemaLimiterBuilder::remaining_header); rejections
    /// always carry it.
    Ietf,
}

pub(crate) enum RateSource {
    Fixed(RateLimit),
    Fn(RateFn),
}

impl RateSource {
    pub(crate) fn resolve(&self, req: &ServiceRequest, key: &str) -> RateLimit {
        match self {
            Self::Fixed(rate) => *rate,
            Self::Fn(resolver) => resolver(req, key),
        }
    }
} // end impl

pub(crate) struct Config {
    pub namespace: Arc<str>,
    pub extractor: Arc<dyn KeyExtractor>,
    pub rate: RateSource,
    pub cost: Option<CostFn>,
    pub strategy: Strategy,
    pub header_mode: HeaderMode,
    pub remaining_header: bool,
    pub suppressed_retry_hint: Option<Duration>,
    pub permissive: bool,
    pub records_outcome: bool,
    pub exclude: Option<ExcludeFn>,
    pub allow_list: HashSet<Box<str>>,
    pub key_error_policy: KeyErrorPolicy,
    pub responder: Arc<dyn RejectionResponder>,
    pub window_size: WindowSize,
    #[cfg_attr(
        not(feature = "redis"),
        expect(dead_code, reason = "read by the async backend path")
    )]
    pub backend_error: BackendErrorPolicy,
    #[cfg_attr(
        not(feature = "redis"),
        expect(dead_code, reason = "read by the async backend path")
    )]
    pub backend_timeout: Duration,
}

/// The advertised per-window capacity, computed exactly as trypema computes it.
pub(crate) fn window_limit(rate: &RateLimit, window_size: WindowSize) -> u64 {
    (rate.as_per_second() * window_size.as_seconds() as f64) as u64
}

/// The middleware factory: wrap it with `App::wrap` or `Scope::wrap`.
///
/// Cloning is cheap; clone it into the `HttpServer::new` closure. See the [crate docs](crate)
/// for a complete example.
#[derive(Clone)]
pub struct TrypemaLimiter<B> {
    pub(crate) config: Arc<Config>,
    pub(crate) backend: B,
}

impl<B> fmt::Debug for TrypemaLimiter<B> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TrypemaLimiter")
            .field("namespace", &self.config.namespace)
            .finish_non_exhaustive()
    }
} // end impl

impl<B: Backend> TrypemaLimiter<B> {
    /// Configure a limiter on a backend handle: [`Local`](crate::Local), `Redis`, or `Hybrid`.
    pub fn builder(backend: B) -> TrypemaLimiterBuilder<B> {
        TrypemaLimiterBuilder {
            backend,
            namespace: None,
            extractor: None,
            rate: None,
            cost: None,
            strategy: Strategy::default(),
            header_mode: HeaderMode::default(),
            remaining_header: false,
            suppressed_retry_hint: None,
            permissive: false,
            record_outcome: false,
            exclude: None,
            allow_list: HashSet::new(),
            key_error_policy: KeyErrorPolicy::default(),
            responder: Arc::new(DefaultResponder),
            backend_error: BackendErrorPolicy::default(),
            backend_timeout: DEFAULT_BACKEND_TIMEOUT,
        }
    } // end method builder
} // end impl

/// Configures one [`TrypemaLimiter`] instance.
///
/// `namespace`, `extractor`, and a rate are required; everything else has a default. `build()`
/// validates and never panics.
#[must_use = "call build() to obtain the middleware"]
pub struct TrypemaLimiterBuilder<B> {
    backend: B,
    namespace: Option<String>,
    extractor: Option<Arc<dyn KeyExtractor>>,
    rate: Option<RateSource>,
    cost: Option<CostFn>,
    strategy: Strategy,
    header_mode: HeaderMode,
    remaining_header: bool,
    suppressed_retry_hint: Option<Duration>,
    permissive: bool,
    record_outcome: bool,
    exclude: Option<ExcludeFn>,
    allow_list: HashSet<Box<str>>,
    key_error_policy: KeyErrorPolicy,
    responder: Arc<dyn RejectionResponder>,
    backend_error: BackendErrorPolicy,
    backend_timeout: Duration,
}

impl<B: Backend> TrypemaLimiterBuilder<B> {
    /// Name this instance: `[a-z0-9-]{1,32}`. Required.
    ///
    /// The namespace isolates keys from other instances sharing the backend, names the
    /// instance's [`LimitOutcome`](crate::LimitOutcome), and labels its metrics.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Choose what a request is keyed by. Required.
    pub fn extractor(mut self, extractor: impl KeyExtractor) -> Self {
        self.extractor = Some(Arc::new(extractor));
        self
    }

    /// Key requests with a closure instead of a [`KeyExtractor`].
    ///
    /// The closure returns an owned `String`, which allocates on every request; implement
    /// [`KeyExtractor`] and write into the [`KeyBuf`](crate::extract::KeyBuf) for hot paths.
    pub fn extractor_fn(
        mut self,
        extractor: impl Fn(&ServiceRequest) -> Result<String, KeyError> + Send + Sync + 'static,
    ) -> Self {
        self.extractor = Some(Arc::new(FnExtractor(extractor)));
        self
    }

    /// Use one fixed rate for every request.
    pub fn rate(mut self, rate: RateLimit) -> Self {
        self.rate = Some(RateSource::Fixed(rate));
        self
    }

    /// Resolve the rate per request, from the request and the extracted key.
    ///
    /// trypema rates are sticky per key: the first rate stored for a key wins. Return a rate
    /// that is a pure function of the key — for plan tiers, put the tier in the key — or change
    /// tiers explicitly with the provider's `set_rate_limit` and
    /// [`derived_key`](crate::derived_key). A rate below one request per window rejects every
    /// request for that key.
    pub fn rate_fn(
        mut self,
        rate: impl Fn(&ServiceRequest, &str) -> RateLimit + Send + Sync + 'static,
    ) -> Self {
        self.rate = Some(RateSource::Fn(Arc::new(rate)));
        self
    }

    /// Charge a per-request cost instead of 1. A cost of 0 bypasses the limiter.
    ///
    /// Keep costs at or below the window capacity. The local backend checks before adding, so a
    /// request costing `n` can overshoot the limit by up to `n - 1`; the Redis backends reject a
    /// request whose cost exceeds the remaining capacity, so a cost above the whole capacity is
    /// never admitted.
    pub fn cost_fn(
        mut self,
        cost: impl Fn(&ServiceRequest) -> u64 + Send + Sync + 'static,
    ) -> Self {
        self.cost = Some(Arc::new(cost));
        self
    }

    /// Choose the admission strategy. Defaults to [`Strategy::Absolute`].
    pub fn strategy(mut self, strategy: Strategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Choose the response headers. Defaults to [`HeaderMode::Legacy`].
    pub fn headers(mut self, mode: HeaderMode) -> Self {
        self.header_mode = mode;
        self
    }

    /// Also send the remaining quota on admitted responses.
    ///
    /// Costs one extra provider read per request: in-memory for Local, a Redis round trip for
    /// Redis, and a local estimate for Hybrid.
    pub fn remaining_header(mut self, enabled: bool) -> Self {
        self.remaining_header = enabled;
        self
    }

    /// Advertise this `retry-after` on suppressed rejections, which carry no hint of their own.
    /// A zero hint sends no `retry-after`.
    pub fn suppressed_retry_hint(mut self, hint: Duration) -> Self {
        self.suppressed_retry_hint = nonzero_duration(hint);
        self
    }

    /// Shadow mode: track usage and record outcomes, but admit every request.
    ///
    /// Requests whose key cannot be extracted bypass the limiter even under
    /// [`KeyErrorPolicy::Reject`], and backend failures are admitted whatever `on_backend_error`
    /// says. Implies [`record_outcome`](Self::record_outcome). With the Absolute strategy, rejected
    /// calls record nothing in the limiter, so shadow counters under-count overload; use
    /// [`Strategy::Suppressed`] for accurate shadow accounting.
    pub fn permissive(mut self, enabled: bool) -> Self {
        self.permissive = enabled;
        self
    }

    /// Record each request's [`LimitOutcome`](crate::LimitOutcome) for handlers to read through
    /// [`RateLimitInfo`](crate::RateLimitInfo).
    ///
    /// Off by default: recording costs a small allocation per request.
    /// [`permissive`](Self::permissive) mode always records.
    pub fn record_outcome(mut self, enabled: bool) -> Self {
        self.record_outcome = enabled;
        self
    }

    /// Bypass requests matching this predicate: health checks, or methods and paths that
    /// should not count. Each call adds a predicate; a request matching any of them is bypassed.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn example(builder: actix_trypema::TrypemaLimiterBuilder<actix_trypema::Local>) {
    /// use actix_web::http::Method;
    ///
    /// // Limit only POSTs, and never the health check:
    /// let builder = builder.exclude(|req| req.method() != Method::POST || req.path() == "/healthz");
    /// # let _ = builder;
    /// # }
    /// ```
    pub fn exclude(
        mut self,
        exclude: impl Fn(&ServiceRequest) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.exclude = Some(match self.exclude.take() {
            Some(previous) => Arc::new(move |req: &ServiceRequest| previous(req) || exclude(req)),
            None => Arc::new(exclude),
        });
        self
    }

    /// Never limit these extracted keys. Each call adds to the list.
    ///
    /// Entries match the extracted key exactly, before the namespace prefix is added: IPv4
    /// addresses as written, IPv6 clients in their masked form (`2001:db8:1:2::/64` by default),
    /// and [`Header::hashed`](crate::extract::Header::hashed) values as their 32-character hash
    /// — a raw API key never matches a hashed extractor.
    pub fn allow_keys<I>(mut self, keys: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<Box<str>>,
    {
        self.allow_list.extend(keys.into_iter().map(Into::into));
        self
    }

    /// Choose what happens when key extraction fails. Defaults to
    /// [`KeyErrorPolicy::Reject`].
    pub fn on_key_error(mut self, policy: KeyErrorPolicy) -> Self {
        self.key_error_policy = policy;
        self
    }

    /// Build rejection responses with a custom [`RejectionResponder`]; it can also change the
    /// status.
    pub fn responder(mut self, responder: impl RejectionResponder) -> Self {
        self.responder = Arc::new(responder);
        self
    }

    /// Validate the configuration and produce the middleware.
    ///
    /// # Errors
    ///
    /// Returns a [`ConfigError`] when the namespace, extractor, or rate is missing, the
    /// namespace is invalid, a fixed rate admits less than one request per window, the backend
    /// timeout is zero, or a [`Fallback`](BackendErrorPolicy::Fallback) backend uses a different
    /// window than this backend.
    pub fn build(self) -> Result<TrypemaLimiter<B>, ConfigError> {
        let namespace = self.namespace.ok_or(ConfigError::MissingNamespace)?;
        validate_namespace(&namespace)?;

        let extractor = self.extractor.ok_or(ConfigError::MissingExtractor)?;
        let rate = self.rate.ok_or(ConfigError::MissingRate)?;
        let window_size = self.backend.window_size();

        // trypema admits while usage is below the truncated capacity, so zero admits nothing.
        if let RateSource::Fixed(rate) = &rate
            && window_limit(rate, window_size) == 0
        {
            return Err(ConfigError::ZeroCapacity);
        }

        if self.backend_timeout.is_zero() {
            return Err(ConfigError::ZeroBackendTimeout);
        }

        // A fallback on another window would enforce a limit the headers do not advertise.
        if let BackendErrorPolicy::Fallback(fallback) = &self.backend_error
            && fallback.window_size() != window_size
        {
            return Err(ConfigError::FallbackWindowMismatch);
        }

        Ok(TrypemaLimiter {
            backend: self.backend,
            config: Arc::new(Config {
                namespace: namespace.into(),
                extractor,
                rate,
                cost: self.cost,
                strategy: self.strategy,
                header_mode: self.header_mode,
                remaining_header: self.remaining_header,
                suppressed_retry_hint: self.suppressed_retry_hint,
                permissive: self.permissive,
                records_outcome: self.record_outcome || self.permissive,
                exclude: self.exclude,
                allow_list: self.allow_list,
                key_error_policy: self.key_error_policy,
                responder: self.responder,
                window_size,
                backend_error: self.backend_error,
                backend_timeout: self.backend_timeout,
            }),
        })
    } // end method build
} // end impl

#[cfg(feature = "redis")]
impl<B: AsyncBackend> TrypemaLimiterBuilder<B> {
    /// Choose what happens when the backend errors or times out. Defaults to
    /// [`BackendErrorPolicy::FailOpen`]; use
    /// [`FailClosed`](BackendErrorPolicy::FailClosed) for login and other security-sensitive
    /// routes.
    pub fn on_backend_error(mut self, policy: BackendErrorPolicy) -> Self {
        self.backend_error = policy;
        self
    }

    /// Bound each backend call. Defaults to 50 ms; a timeout counts as a backend failure, and
    /// `build()` rejects zero.
    pub fn backend_timeout(mut self, timeout: Duration) -> Self {
        self.backend_timeout = timeout;
        self
    }
} // end impl
