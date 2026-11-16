//! Backends: trypema's own providers and limiters, shared through an `Arc`.
//!
//! A provider holds both strategies, so the builder chooses one with `strategy(..)`. A single
//! limiter holds one strategy, so its type sets the strategy.

use std::sync::Arc;

use trypema::{
    RateLimit, RateLimitDecision, WindowSize,
    local::{AbsoluteLocalRateLimiter, LocalRateLimiterProvider, SuppressedLocalRateLimiter},
};

use crate::builder::Strategy;

#[cfg(feature = "redis")]
use trypema::{
    TrypemaError,
    hybrid::{AbsoluteHybridRateLimiter, HybridRateLimiterProvider, SuppressedHybridRateLimiter},
    redis::{
        AbsoluteRedisRateLimiter, RedisKey, RedisKeyRef, RedisRateLimiterProvider,
        SuppressedRedisRateLimiter,
    },
};

pub(crate) mod sealed {
    pub trait Sealed {}

    /// The kind of a backend that decides synchronously: the local types.
    pub enum SyncKind {}

    /// The kind of a backend that awaits Redis: the Redis and hybrid types.
    #[cfg(feature = "redis")]
    pub enum AsyncKind {}
}

use sealed::{Sealed, SyncKind};

#[cfg(feature = "redis")]
use sealed::AsyncKind;

/// A trypema provider or limiter the middleware can drive, in an `Arc`.
///
/// Sealed. It is implemented for `Arc<T>`, where `T` is one of these trypema types:
///
/// | Feature | Providers (both strategies) | Single limiters (one strategy) |
/// |---|---|---|
/// | none | [`LocalRateLimiterProvider`] | [`AbsoluteLocalRateLimiter`], [`SuppressedLocalRateLimiter`] |
/// | `redis` | `RedisRateLimiterProvider`, `HybridRateLimiterProvider` | `AbsoluteRedisRateLimiter`, `SuppressedRedisRateLimiter`, `AbsoluteHybridRateLimiter`, `SuppressedHybridRateLimiter` |
///
/// Build the value with trypema's `RateLimiterBuilder`. The middleware reads the window from
/// the value, so the advertised limit always agrees with the enforced limit.
pub trait Backend: Sealed + Clone + 'static {
    /// Selects the synchronous or the asynchronous request path.
    #[doc(hidden)]
    type Kind;

    /// The strategy of a single limiter. `None` for a provider: the builder then uses the
    /// strategy given to [`strategy`](crate::TrypemaLimiterBuilder::strategy).
    const STRATEGY: Option<Strategy>;

    /// The window the provider or limiter was built with.
    fn window_size(&self) -> WindowSize;
}

/// A backend that holds both strategies: a trypema provider.
///
/// Only these backends accept [`strategy`](crate::TrypemaLimiterBuilder::strategy). Sealed.
pub trait ProviderBackend: Backend {}

macro_rules! backend {
    ($target:ty, $kind:ty, $strategy:expr) => {
        impl Sealed for Arc<$target> {}

        impl Backend for Arc<$target> {
            type Kind = $kind;

            const STRATEGY: Option<Strategy> = $strategy;

            fn window_size(&self) -> WindowSize {
                <$target>::window_size(self)
            }
        } // end impl
    };
}

backend!(LocalRateLimiterProvider, SyncKind, None);
backend!(AbsoluteLocalRateLimiter, SyncKind, Some(Strategy::Absolute));
backend!(
    SuppressedLocalRateLimiter,
    SyncKind,
    Some(Strategy::Suppressed)
);

#[cfg(feature = "redis")]
backend!(RedisRateLimiterProvider, AsyncKind, None);
#[cfg(feature = "redis")]
backend!(
    AbsoluteRedisRateLimiter,
    AsyncKind,
    Some(Strategy::Absolute)
);
#[cfg(feature = "redis")]
backend!(
    SuppressedRedisRateLimiter,
    AsyncKind,
    Some(Strategy::Suppressed)
);
#[cfg(feature = "redis")]
backend!(HybridRateLimiterProvider, AsyncKind, None);
#[cfg(feature = "redis")]
backend!(
    AbsoluteHybridRateLimiter,
    AsyncKind,
    Some(Strategy::Absolute)
);
#[cfg(feature = "redis")]
backend!(
    SuppressedHybridRateLimiter,
    AsyncKind,
    Some(Strategy::Suppressed)
);

impl ProviderBackend for Arc<LocalRateLimiterProvider> {}

#[cfg(feature = "redis")]
impl ProviderBackend for Arc<RedisRateLimiterProvider> {}

#[cfg(feature = "redis")]
impl ProviderBackend for Arc<HybridRateLimiterProvider> {}

/// The local backends: decided synchronously, in process.
///
/// A single limiter ignores `strategy`: it always runs its own strategy.
pub(crate) trait SyncBackend: Backend {
    fn backend_inc(
        &self,
        strategy: Strategy,
        key: &str,
        rate: &RateLimit,
        cost: u64,
    ) -> RateLimitDecision;

    /// Live `(total, declined)` usage for `key`.
    fn backend_usage(&self, strategy: Strategy, key: &str) -> (u64, u64);
}

impl SyncBackend for Arc<LocalRateLimiterProvider> {
    fn backend_inc(
        &self,
        strategy: Strategy,
        key: &str,
        rate: &RateLimit,
        cost: u64,
    ) -> RateLimitDecision {
        match strategy {
            Strategy::Absolute => self.absolute().inc(key, rate, cost),
            Strategy::Suppressed => self.suppressed().inc(key, rate, cost),
        }
    }

    fn backend_usage(&self, strategy: Strategy, key: &str) -> (u64, u64) {
        match strategy {
            Strategy::Absolute => (self.absolute().get(key), 0),
            Strategy::Suppressed => {
                let snapshot = self.suppressed().get(key);
                (snapshot.total, snapshot.total_declined)
            }
        }
    }
} // end impl

impl SyncBackend for Arc<AbsoluteLocalRateLimiter> {
    fn backend_inc(
        &self,
        _strategy: Strategy,
        key: &str,
        rate: &RateLimit,
        cost: u64,
    ) -> RateLimitDecision {
        AbsoluteLocalRateLimiter::inc(self, key, rate, cost)
    }

    fn backend_usage(&self, _strategy: Strategy, key: &str) -> (u64, u64) {
        (AbsoluteLocalRateLimiter::get(self, key), 0)
    }
} // end impl

impl SyncBackend for Arc<SuppressedLocalRateLimiter> {
    fn backend_inc(
        &self,
        _strategy: Strategy,
        key: &str,
        rate: &RateLimit,
        cost: u64,
    ) -> RateLimitDecision {
        SuppressedLocalRateLimiter::inc(self, key, rate, cost)
    }

    fn backend_usage(&self, _strategy: Strategy, key: &str) -> (u64, u64) {
        let snapshot = SuppressedLocalRateLimiter::get(self, key);
        (snapshot.total, snapshot.total_declined)
    }
} // end impl

/// The Redis and hybrid backends: decided through Redis, under a timeout. Sealed.
///
/// Only these backends accept
/// [`on_backend_error`](crate::TrypemaLimiterBuilder::on_backend_error) and
/// [`backend_timeout`](crate::TrypemaLimiterBuilder::backend_timeout).
#[cfg(feature = "redis")]
pub trait AsyncBackend: Backend {
    /// Count `cost` for `key`. A single limiter ignores `strategy`.
    #[doc(hidden)]
    fn backend_inc(
        &self,
        strategy: Strategy,
        key: &RedisKey,
        rate: &RateLimit,
        cost: u64,
    ) -> impl Future<Output = Result<RateLimitDecision, TrypemaError>>;

    /// Decide from local state without awaiting; `Ok(None)` means `backend_inc` is needed.
    #[doc(hidden)]
    fn try_backend_inc(
        &self,
        _strategy: Strategy,
        _key: RedisKeyRef<'_>,
        _cost: u64,
    ) -> Result<Option<RateLimitDecision>, TrypemaError> {
        Ok(None)
    }

    /// `(total, declined)` usage for `key`: read from Redis, or the local estimate of a hybrid
    /// backend.
    #[doc(hidden)]
    fn backend_usage(
        &self,
        strategy: Strategy,
        key: &RedisKey,
    ) -> impl Future<Output = Result<(u64, u64), TrypemaError>>;
}

#[cfg(feature = "redis")]
impl AsyncBackend for Arc<RedisRateLimiterProvider> {
    async fn backend_inc(
        &self,
        strategy: Strategy,
        key: &RedisKey,
        rate: &RateLimit,
        cost: u64,
    ) -> Result<RateLimitDecision, TrypemaError> {
        match strategy {
            Strategy::Absolute => self.absolute().inc(key, rate, cost).await,
            Strategy::Suppressed => self.suppressed().inc(key, rate, cost).await,
        }
    }

    async fn backend_usage(
        &self,
        strategy: Strategy,
        key: &RedisKey,
    ) -> Result<(u64, u64), TrypemaError> {
        match strategy {
            Strategy::Absolute => Ok((self.absolute().get(key).await?, 0)),
            Strategy::Suppressed => {
                let snapshot = self.suppressed().get(key).await?;
                Ok((snapshot.total, snapshot.total_declined))
            }
        }
    }
} // end impl

#[cfg(feature = "redis")]
impl AsyncBackend for Arc<AbsoluteRedisRateLimiter> {
    async fn backend_inc(
        &self,
        _strategy: Strategy,
        key: &RedisKey,
        rate: &RateLimit,
        cost: u64,
    ) -> Result<RateLimitDecision, TrypemaError> {
        AbsoluteRedisRateLimiter::inc(self, key, rate, cost).await
    }

    async fn backend_usage(
        &self,
        _strategy: Strategy,
        key: &RedisKey,
    ) -> Result<(u64, u64), TrypemaError> {
        Ok((AbsoluteRedisRateLimiter::get(self, key).await?, 0))
    }
} // end impl

#[cfg(feature = "redis")]
impl AsyncBackend for Arc<SuppressedRedisRateLimiter> {
    async fn backend_inc(
        &self,
        _strategy: Strategy,
        key: &RedisKey,
        rate: &RateLimit,
        cost: u64,
    ) -> Result<RateLimitDecision, TrypemaError> {
        SuppressedRedisRateLimiter::inc(self, key, rate, cost).await
    }

    async fn backend_usage(
        &self,
        _strategy: Strategy,
        key: &RedisKey,
    ) -> Result<(u64, u64), TrypemaError> {
        let snapshot = SuppressedRedisRateLimiter::get(self, key).await?;
        Ok((snapshot.total, snapshot.total_declined))
    }
} // end impl

// The hybrid usage reads use get_estimate, which keeps them off Redis in steady state.

#[cfg(feature = "redis")]
impl AsyncBackend for Arc<HybridRateLimiterProvider> {
    async fn backend_inc(
        &self,
        strategy: Strategy,
        key: &RedisKey,
        rate: &RateLimit,
        cost: u64,
    ) -> Result<RateLimitDecision, TrypemaError> {
        match strategy {
            Strategy::Absolute => self.absolute().inc(key, rate, cost).await,
            Strategy::Suppressed => self.suppressed().inc(key, rate, cost).await,
        }
    }

    fn try_backend_inc(
        &self,
        strategy: Strategy,
        key: RedisKeyRef<'_>,
        cost: u64,
    ) -> Result<Option<RateLimitDecision>, TrypemaError> {
        match strategy {
            Strategy::Absolute => self.absolute().try_inc(key, cost),
            Strategy::Suppressed => self.suppressed().try_inc(key, cost),
        }
    }

    async fn backend_usage(
        &self,
        strategy: Strategy,
        key: &RedisKey,
    ) -> Result<(u64, u64), TrypemaError> {
        match strategy {
            Strategy::Absolute => Ok((self.absolute().get_estimate(key).await?, 0)),
            Strategy::Suppressed => {
                let snapshot = self.suppressed().get_estimate(key).await?;
                Ok((snapshot.total, snapshot.total_declined))
            }
        }
    }
} // end impl

#[cfg(feature = "redis")]
impl AsyncBackend for Arc<AbsoluteHybridRateLimiter> {
    async fn backend_inc(
        &self,
        _strategy: Strategy,
        key: &RedisKey,
        rate: &RateLimit,
        cost: u64,
    ) -> Result<RateLimitDecision, TrypemaError> {
        AbsoluteHybridRateLimiter::inc(self, key, rate, cost).await
    }

    fn try_backend_inc(
        &self,
        _strategy: Strategy,
        key: RedisKeyRef<'_>,
        cost: u64,
    ) -> Result<Option<RateLimitDecision>, TrypemaError> {
        AbsoluteHybridRateLimiter::try_inc(self, key, cost)
    }

    async fn backend_usage(
        &self,
        _strategy: Strategy,
        key: &RedisKey,
    ) -> Result<(u64, u64), TrypemaError> {
        Ok((AbsoluteHybridRateLimiter::get_estimate(self, key).await?, 0))
    }
} // end impl

#[cfg(feature = "redis")]
impl AsyncBackend for Arc<SuppressedHybridRateLimiter> {
    async fn backend_inc(
        &self,
        _strategy: Strategy,
        key: &RedisKey,
        rate: &RateLimit,
        cost: u64,
    ) -> Result<RateLimitDecision, TrypemaError> {
        SuppressedHybridRateLimiter::inc(self, key, rate, cost).await
    }

    fn try_backend_inc(
        &self,
        _strategy: Strategy,
        key: RedisKeyRef<'_>,
        cost: u64,
    ) -> Result<Option<RateLimitDecision>, TrypemaError> {
        SuppressedHybridRateLimiter::try_inc(self, key, cost)
    }

    async fn backend_usage(
        &self,
        _strategy: Strategy,
        key: &RedisKey,
    ) -> Result<(u64, u64), TrypemaError> {
        let snapshot = SuppressedHybridRateLimiter::get_estimate(self, key).await?;
        Ok((snapshot.total, snapshot.total_declined))
    }
} // end impl
