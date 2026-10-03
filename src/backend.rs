//! Backend handles: a trypema provider plus the window it was built with.
//!
//! The middleware needs the provider's window to advertise limits, and trypema does not expose
//! it. The handles therefore build the provider themselves and apply the window last, so the
//! advertised limit can never disagree with what the provider enforces.

use std::{fmt, sync::Arc};

use trypema::{
    RateLimit, RateLimitDecision, RateLimiterBuilder, TrypemaError, WindowSize,
    local::{LocalRateLimiterBuilder, LocalRateLimiterProvider},
};

use crate::builder::Strategy;

#[cfg(feature = "redis")]
use trypema::{
    hybrid::{HybridRateLimiterBuilder, HybridRateLimiterProvider},
    redis::{ConnectionManager, RedisKey, RedisRateLimiterBuilder, RedisRateLimiterProvider},
};

mod sealed {
    pub trait Sealed {}
}

/// A trypema backend the middleware can drive: [`Local`], `Redis`, or `Hybrid`.
///
/// Sealed; construct one of the handles instead of implementing it.
pub trait Backend: sealed::Sealed + Clone + 'static {
    /// The window the provider was built with.
    fn window_size(&self) -> WindowSize;
}

/// The in-process backend.
///
/// Cloning is cheap and shares one provider, so build it once — outside `HttpServer::new`, or
/// every worker gets its own limiter — and clone it into each middleware instance.
///
/// # Examples
///
/// ```
/// use actix_trypema::Local;
/// use trypema::{BucketSize, RateLimiterBuilder, WindowSize};
///
/// let backend = Local::new(WindowSize::seconds_or_panic(60))?;
///
/// // Any other provider option goes through trypema's builder:
/// let tuned = Local::configured(WindowSize::seconds_or_panic(60), |builder| {
///     builder.bucket_size(BucketSize::milliseconds_or_panic(10))
/// })?;
/// # let _ = (backend, tuned);
/// # Ok::<(), trypema::TrypemaError>(())
/// ```
#[derive(Clone)]
pub struct Local {
    provider: Arc<LocalRateLimiterProvider>,
    window_size: WindowSize,
}

impl Local {
    /// Build an in-process provider with this window and trypema's defaults otherwise.
    ///
    /// # Errors
    ///
    /// Returns the [`TrypemaError`] trypema's builder reports for an invalid configuration.
    pub fn new(window_size: WindowSize) -> Result<Self, TrypemaError> {
        Self::configured(window_size, |builder| builder)
    }

    /// Build an in-process provider, customizing any option except the window.
    ///
    /// `configure` receives trypema's builder. The window is applied after it, so the provider
    /// always enforces the window the middleware advertises.
    ///
    /// # Errors
    ///
    /// Returns the [`TrypemaError`] trypema's builder reports for an invalid configuration, for
    /// example a bucket size larger than the window.
    pub fn configured(
        window_size: WindowSize,
        configure: impl FnOnce(LocalRateLimiterBuilder) -> LocalRateLimiterBuilder,
    ) -> Result<Self, TrypemaError> {
        let provider = configure(LocalRateLimiterProvider::builder())
            .window_size(window_size)
            .build()?;

        Ok(Self {
            provider,
            window_size,
        })
    } // end constructor

    /// The provider, for administration such as `delete` or `set_rate_limit` with
    /// [`derived_key`](crate::derived_key).
    pub fn provider(&self) -> &Arc<LocalRateLimiterProvider> {
        &self.provider
    }

    pub(crate) fn inc(
        &self,
        strategy: Strategy,
        key: &str,
        rate: &RateLimit,
        cost: u64,
    ) -> RateLimitDecision {
        match strategy {
            Strategy::Absolute => self.provider.absolute().inc(key, rate, cost),
            Strategy::Suppressed => self.provider.suppressed().inc(key, rate, cost),
        }
    }

    /// Live `(total, declined)` usage for `key`.
    pub(crate) fn usage(&self, strategy: Strategy, key: &str) -> (u64, u64) {
        match strategy {
            Strategy::Absolute => (self.provider.absolute().get(key), 0),
            Strategy::Suppressed => {
                let snapshot = self.provider.suppressed().get(key);
                (snapshot.total, snapshot.total_declined)
            }
        }
    }
} // end impl

impl sealed::Sealed for Local {}

impl Backend for Local {
    fn window_size(&self) -> WindowSize {
        self.window_size
    }
} // end impl

impl fmt::Debug for Local {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Local")
            .field("window_size", &self.window_size)
            .finish_non_exhaustive()
    }
} // end impl

/// Async backends: `Redis` and `Hybrid`. Sealed.
#[cfg(feature = "redis")]
pub trait AsyncBackend: Backend {
    #[doc(hidden)]
    fn backend_inc(
        &self,
        strategy: Strategy,
        key: &RedisKey,
        rate: &RateLimit,
        cost: u64,
    ) -> impl Future<Output = Result<RateLimitDecision, TrypemaError>>;

    /// Live `(total, declined)` usage for `key`.
    #[doc(hidden)]
    fn backend_usage(
        &self,
        strategy: Strategy,
        key: &RedisKey,
    ) -> impl Future<Output = Result<(u64, u64), TrypemaError>>;
}

/// The distributed Redis backend, shared across every app instance on the same prefix.
///
/// Cloning is cheap and shares one provider.
///
/// # Examples
///
/// ```no_run
/// # async fn example(connection: trypema::redis::ConnectionManager) -> Result<(), trypema::TrypemaError> {
/// use actix_trypema::Redis;
/// use trypema::{RateLimiterBuilder, WindowSize, redis::RedisKey};
///
/// let backend = Redis::configured(connection, WindowSize::seconds_or_panic(60), |builder| {
///     builder.prefix(RedisKey::try_from("my-service").unwrap())
/// })?;
/// # let _ = backend;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "redis")]
#[derive(Clone)]
pub struct Redis {
    provider: Arc<RedisRateLimiterProvider>,
    window_size: WindowSize,
}

#[cfg(feature = "redis")]
impl Redis {
    /// Build a Redis provider with this window and trypema's defaults otherwise.
    ///
    /// # Errors
    ///
    /// Returns the [`TrypemaError`] trypema's builder reports for an invalid configuration.
    pub fn new(
        connection: ConnectionManager,
        window_size: WindowSize,
    ) -> Result<Self, TrypemaError> {
        Self::configured(connection, window_size, |builder| builder)
    }

    /// Build a Redis provider, customizing any option except the window.
    ///
    /// # Errors
    ///
    /// Returns the [`TrypemaError`] trypema's builder reports for an invalid configuration.
    pub fn configured(
        connection: ConnectionManager,
        window_size: WindowSize,
        configure: impl FnOnce(RedisRateLimiterBuilder) -> RedisRateLimiterBuilder,
    ) -> Result<Self, TrypemaError> {
        let provider = configure(RedisRateLimiterProvider::builder(connection))
            .window_size(window_size)
            .build()?;

        Ok(Self {
            provider,
            window_size,
        })
    } // end constructor

    /// The provider, for administration with [`derived_key`](crate::derived_key).
    pub fn provider(&self) -> &Arc<RedisRateLimiterProvider> {
        &self.provider
    }
} // end impl

#[cfg(feature = "redis")]
impl sealed::Sealed for Redis {}

#[cfg(feature = "redis")]
impl Backend for Redis {
    fn window_size(&self) -> WindowSize {
        self.window_size
    }
} // end impl

#[cfg(feature = "redis")]
impl AsyncBackend for Redis {
    async fn backend_inc(
        &self,
        strategy: Strategy,
        key: &RedisKey,
        rate: &RateLimit,
        cost: u64,
    ) -> Result<RateLimitDecision, TrypemaError> {
        match strategy {
            Strategy::Absolute => self.provider.absolute().inc(key, rate, cost).await,
            Strategy::Suppressed => self.provider.suppressed().inc(key, rate, cost).await,
        }
    }

    async fn backend_usage(
        &self,
        strategy: Strategy,
        key: &RedisKey,
    ) -> Result<(u64, u64), TrypemaError> {
        match strategy {
            Strategy::Absolute => Ok((self.provider.absolute().get(key).await?, 0)),
            Strategy::Suppressed => {
                let snapshot = self.provider.suppressed().get(key).await?;
                Ok((snapshot.total, snapshot.total_declined))
            }
        }
    }
} // end impl

#[cfg(feature = "redis")]
impl fmt::Debug for Redis {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Redis")
            .field("window_size", &self.window_size)
            .finish_non_exhaustive()
    }
} // end impl

/// The hybrid backend: a local fast path with periodic Redis sync.
///
/// Admission lags Redis by the sync interval, and batches not yet flushed can be lost when the
/// provider is dropped at shutdown. Cloning is cheap and shares one provider.
#[cfg(feature = "redis")]
#[derive(Clone)]
pub struct Hybrid {
    provider: Arc<HybridRateLimiterProvider>,
    window_size: WindowSize,
}

#[cfg(feature = "redis")]
impl Hybrid {
    /// Build a hybrid provider with this window and trypema's defaults otherwise.
    ///
    /// # Errors
    ///
    /// Returns the [`TrypemaError`] trypema's builder reports for an invalid configuration.
    pub fn new(
        connection: ConnectionManager,
        window_size: WindowSize,
    ) -> Result<Self, TrypemaError> {
        Self::configured(connection, window_size, |builder| builder)
    }

    /// Build a hybrid provider, customizing any option (such as `sync_interval`) except the
    /// window.
    ///
    /// # Errors
    ///
    /// Returns the [`TrypemaError`] trypema's builder reports for an invalid configuration.
    pub fn configured(
        connection: ConnectionManager,
        window_size: WindowSize,
        configure: impl FnOnce(HybridRateLimiterBuilder) -> HybridRateLimiterBuilder,
    ) -> Result<Self, TrypemaError> {
        let provider = configure(HybridRateLimiterProvider::builder(connection))
            .window_size(window_size)
            .build()?;

        Ok(Self {
            provider,
            window_size,
        })
    } // end constructor

    /// The provider, for administration with [`derived_key`](crate::derived_key).
    pub fn provider(&self) -> &Arc<HybridRateLimiterProvider> {
        &self.provider
    }
} // end impl

#[cfg(feature = "redis")]
impl sealed::Sealed for Hybrid {}

#[cfg(feature = "redis")]
impl Backend for Hybrid {
    fn window_size(&self) -> WindowSize {
        self.window_size
    }
} // end impl

#[cfg(feature = "redis")]
impl AsyncBackend for Hybrid {
    async fn backend_inc(
        &self,
        strategy: Strategy,
        key: &RedisKey,
        rate: &RateLimit,
        cost: u64,
    ) -> Result<RateLimitDecision, TrypemaError> {
        match strategy {
            Strategy::Absolute => self.provider.absolute().inc(key, rate, cost).await,
            Strategy::Suppressed => self.provider.suppressed().inc(key, rate, cost).await,
        }
    }

    async fn backend_usage(
        &self,
        strategy: Strategy,
        key: &RedisKey,
    ) -> Result<(u64, u64), TrypemaError> {
        // get_estimate keeps this off the Redis hot path in steady state.
        match strategy {
            Strategy::Absolute => Ok((self.provider.absolute().get_estimate(key).await?, 0)),
            Strategy::Suppressed => {
                let snapshot = self.provider.suppressed().get_estimate(key).await?;
                Ok((snapshot.total, snapshot.total_declined))
            }
        }
    }
} // end impl

#[cfg(feature = "redis")]
impl fmt::Debug for Hybrid {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Hybrid")
            .field("window_size", &self.window_size)
            .finish_non_exhaustive()
    }
} // end impl
