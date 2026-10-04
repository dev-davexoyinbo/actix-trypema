# actix-trypema

actix-web middleware for the [trypema](https://crates.io/crates/trypema) sliding-window rate
limiter. One middleware covers every backend: in-process limits, exact limits shared through
Redis, and hybrid limits shared through a local fast path. Client IPs are resolved safely
behind load balancers and CDNs.

- **Full guide:** [trypema.davidoyinbo.com/actix-web](https://trypema.davidoyinbo.com/actix-web)
- **API docs:** [docs.rs/actix-trypema](https://docs.rs/actix-trypema)

## Install

```sh
cargo add actix-trypema trypema          # trypema provides RateLimit and WindowSize
cargo add actix-trypema --features redis # Redis and Hybrid backends
cargo add redis --features tokio-comp,connection-manager  # to connect to Redis
```

| Feature | Adds |
|---|---|
| `redis` | The `Redis` and `Hybrid` backends, on tokio (the runtime actix-web runs on). |
| `json` | `JsonResponder`, a JSON body for rejections. |
| `metrics` | Decision counters and a backend latency histogram through the `metrics` facade. |

MSRV 1.88.

## Quick start

Sixty requests per minute per client IP:

```rust
use actix_trypema::{Local, TrypemaLimiter, extract::PeerIp};
use actix_web::{App, HttpServer, web};
use trypema::{RateLimit, WindowSize};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Build the backend once, outside HttpServer::new: inside the closure every worker would
    // get its own counters, multiplying the effective limit by the worker count.
    let backend = Local::new(WindowSize::seconds_or_panic(60)).expect("valid backend");

    let limiter = TrypemaLimiter::builder(backend)
        .namespace("ip")
        .extractor(PeerIp::default())
        .rate(RateLimit::per_minute_or_panic(60.0))
        .build()
        .expect("valid configuration");

    HttpServer::new(move || {
        App::new()
            .wrap(limiter.clone())
            .route("/", web::get().to(|| async { "hello" }))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
```

The 61st request within a minute gets `429 Too Many Requests` with a `retry-after` header and
never reaches the handler.

Every limiter has a **backend** (where counts live), an **extractor** (who a request is), a
**rate**, and a **namespace** (`[a-z0-9-]{1,32}`). The namespace keeps limiters that share a
backend apart, and names the limiter in headers, outcomes, metrics and logs. `build()` checks
the configuration and returns a `ConfigError` instead of failing at request time.

## Client IPs

**Clients connect directly.** `PeerIp` keys by the TCP peer and never reads headers. IPv6
clients are grouped by their `/64` network, since one client usually controls a whole `/64`:

```rust
use actix_trypema::extract::PeerIp;

let per_ip = PeerIp::default();
let per_56 = PeerIp::default().ipv6_prefix_len(56)?; // group IPv6 clients by /56 instead
```

**Behind a load balancer.** Every request arrives from the load balancer, and the client's
address is in `X-Forwarded-For`. `RealIp::xff` reads that header **only** when the TCP peer is
one of your `TrustedProxies`:

```rust
use actix_trypema::{Local, TrypemaLimiter, extract::{RealIp, TrustedProxies}};
use trypema::{RateLimit, WindowSize};

// The load balancers in front of the app, and nothing else.
let trusted = TrustedProxies::new(["10.0.0.0/16"])?;

let limiter = TrypemaLimiter::builder(Local::new(WindowSize::seconds_or_panic(60))?)
    .namespace("ip")
    .extractor(RealIp::xff(trusted))
    .rate(RateLimit::per_minute_or_panic(60.0))
    .build()?;
```

The header is walked from the right, skipping trusted proxies; the first untrusted address is
the client. With `10.0.0.0/16` trusted:

| TCP peer | `X-Forwarded-For` | Key |
|---|---|---|
| `203.0.113.7` | anything | `203.0.113.7`: the peer is not trusted, so the header is ignored |
| `10.0.0.5` | `198.51.100.4` | `198.51.100.4` |
| `10.0.0.5` | `1.2.3.4, 198.51.100.4` | `198.51.100.4`: the client-written `1.2.3.4` is never read |
| `10.0.0.5` | `198.51.100.4, 10.0.0.9` | `198.51.100.4`: the trusted hop is skipped |
| `10.0.0.5` | absent | `10.0.0.5` |
| `10.0.0.5` | `garbage` | key error (`400` by default), never a silent fallback |

List only your own proxies: a client inside a trusted range can choose its own key. There is no
"private networks" preset, and `0.0.0.0/0` is rejected.

**`Forwarded` and CDN headers.** RFC 7239 `Forwarded` and single-value headers such as
Cloudflare's follow the same rules:

```rust
use actix_trypema::extract::{RealIp, TrustedProxies};

let forwarded = RealIp::forwarded(TrustedProxies::new(["10.0.0.0/16"])?);

// Trust Cloudflare's published ranges (two shown here; list them all).
let cloudflare = RealIp::header(
    "cf-connecting-ip",
    TrustedProxies::new(["173.245.48.0/20", "2400:cb00::/32"])?,
)?;
```

IPv4-mapped and NAT64 addresses are keyed as the IPv4 address they carry. `allow_keys([..])`
exempts addresses such as an office IP.

## API keys and custom extractors

```rust
use actix_trypema::extract::{Composite, Global, Header, PeerIp};

// An API key, hashed so the raw key never reaches limiter state or logs.
let api_key = Header::new("x-api-key")?.hashed();

// One bucket for every request, to protect a fixed-capacity downstream.
let everyone = Global;

// Two keys at once: a per-IP limit inside each tenant.
let per_tenant_ip = Composite::new(Header::new("x-tenant-id")?, PeerIp::default());
```

Key only on values an earlier middleware has **authenticated**: a raw header is chosen by the
client, and every new value gets a fresh bucket.

**Closures.** `extractor_fn` keys by anything on the request: extensions set by your
authentication middleware, path parameters, cookies, or a validated lookup.

```rust
use actix_trypema::{KeyError, Local, TrypemaLimiter};
use actix_web::HttpMessage;
use trypema::{RateLimit, WindowSize};

/// Inserted by your authentication middleware.
#[derive(Clone)]
struct UserId(u64);

let per_user = TrypemaLimiter::builder(Local::new(WindowSize::seconds_or_panic(60))?)
    .namespace("user")
    .extractor_fn(|req| {
        req.extensions()
            .get::<UserId>()
            .map(|user| user.0.to_string())
            .ok_or(KeyError::InvalidValue {
                reason: "request is not authenticated",
            })
    })
    .rate(RateLimit::per_minute_or_panic(300.0))
    .build()?;
```

Path parameters (`req.match_info()`) are filled in when the limiter wraps the scope or resource
that declares them, not the whole `App`.

**Implementing `KeyExtractor`.** A closure allocates a `String` per request. For hot paths,
implement the trait: borrow the key from the request, or write it into the provided `KeyBuf`, a
256-byte buffer that allocates nothing for short keys.

```rust
use actix_trypema::{KeyError, extract::{KeyBuf, KeyExtractor}};
use actix_web::dev::ServiceRequest;

/// Keys by the `x-tenant-id` header, accepting only short alphanumeric ids.
struct TenantId;

impl KeyExtractor for TenantId {
    fn extract<'a>(
        &self,
        req: &'a ServiceRequest,
        _buf: &'a mut KeyBuf,
    ) -> Result<&'a str, KeyError> {
        let tenant = req
            .headers()
            .get("x-tenant-id")
            .and_then(|value| value.to_str().ok())
            .ok_or(KeyError::InvalidValue { reason: "missing tenant id" })?;

        if tenant.is_empty() || tenant.len() > 32 || !tenant.bytes().all(|b| b.is_ascii_alphanumeric()) {
            return Err(KeyError::InvalidValue { reason: "invalid tenant id" });
        }

        Ok(tenant)
    }
}
```

When extraction fails, `on_key_error` decides: `Reject` (`400`, the default), `Bypass`, or
`UseGlobalKey` (one shared bucket).

## Limits

```rust
use std::time::Duration;

use actix_trypema::{KeyError, Local, Strategy, TrypemaLimiter, extract::PeerIp};
use actix_web::http::Method;
use trypema::{RateLimit, WindowSize};

let backend = Local::new(WindowSize::seconds_or_panic(60))?;

// Plan tiers: the plan is part of the key ("pro:account-2"), because trypema rates are sticky
// per key and an upgrade should land in a new bucket.
let tiers = TrypemaLimiter::builder(backend.clone())
    .namespace("apikey")
    .extractor_fn(|req| {
        req.headers()
            .get("x-plan-key")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
            .ok_or(KeyError::InvalidValue { reason: "missing plan key" })
    })
    .rate_fn(|_req, key| {
        if key.starts_with("pro:") {
            RateLimit::per_minute_or_panic(1_000.0)
        } else {
            RateLimit::per_minute_or_panic(60.0)
        }
    })
    .build()?;

// Costs, exclusions and gradual shedding.
let per_ip = TrypemaLimiter::builder(backend)
    .namespace("ip")
    .extractor(PeerIp::default())
    .rate(RateLimit::per_minute_or_panic(600.0))
    .cost_fn(|req| if req.path() == "/export" { 20 } else { 1 })
    .exclude(|req| req.path() == "/healthz" || req.method() == Method::OPTIONS)
    .strategy(Strategy::Suppressed) // shed a growing share past a soft limit, not a hard cut-off
    .suppressed_retry_hint(Duration::from_secs(5))
    .build()?;
```

Wrap several limiters for layered limits: a generous per-IP limit with `App::wrap`, and stricter
ones on `web::resource("/login").wrap(..)` or `web::scope("/api").wrap(..)`. actix runs the last
`.wrap` first. Put IP limiters outermost, before authentication, and identity limiters inside
it.

## Backends

| Backend | Feature | Shared across instances | Cost per request |
|---|---|---|---|
| `Local` | none | No | In memory. |
| `Redis` | `redis` | Yes, exactly | One Redis round trip; Redis 7.2+. |
| `Hybrid` | `redis` | Yes, eventually: counts sync every interval | In memory for most requests. |

```rust
use actix_trypema::{Hybrid, Local, Redis};
use trypema::{BucketSize, RateLimiterBuilder, WindowSize, hybrid::SyncInterval, redis::RedisKey};

let window = WindowSize::minutes_or_panic(1);
let connection = redis::Client::open("redis://127.0.0.1:6379/")?
    .get_connection_manager()
    .await?;

let local = Local::configured(window, |builder| {
    builder.bucket_size(BucketSize::milliseconds_or_panic(50))
})?;

let prefix = RedisKey::try_from("checkout-api")?; // services sharing one Redis
let redis = Redis::configured(connection.clone(), window, |builder| builder.prefix(prefix))?;

let hybrid = Hybrid::configured(connection, window, |builder| {
    builder.sync_interval(SyncInterval::milliseconds_or_panic(50))
})?;
```

`Hybrid` decides most requests from local state without awaiting; only requests that need Redis
take the async path. An instance's view of the others lags by up to one sync interval.

**When Redis fails.** Every Redis and hybrid call is bounded by `backend_timeout` (50 ms by
default), and failures follow `on_backend_error`:

| Policy | On failure |
|---|---|
| `FailOpen` (default) | Admit: a limiter outage should not become an API outage. |
| `FailClosed` | `503`. Use it on login, OTP and other abuse-prone routes. |
| `Fallback(local)` | Decide with a `Local` backend on the same window, per instance. |
| `Custom(fn)` | Decide per `BackendFailure` (timeout or error). |

## Responses

Rejections get `429`, `retry-after` in whole seconds, the rate limit headers, and a body from the
responder (`Too many requests, retry in 17s` by default).

| `headers(..)` | Admitted | Rejected |
|---|---|---|
| `Legacy` (default) | `x-ratelimit-limit: 60` | `x-ratelimit-limit: 60` |
| `Ietf` (draft 09) | `ratelimit-policy: "ip";q=60;w=60` | plus `ratelimit: "ip";r=0;t=17` |
| `Off` | none | none (`retry-after` is still sent) |

`remaining_header(true)` adds the remaining count. Turn headers off on login routes, where they
tell attackers how fast to go. For the body, use `JsonResponder` (`json` feature) or your own
`RejectionResponder`, which can also change the status:

```rust
use actix_trypema::{RejectionInfo, RejectionResponder};
use actix_web::{HttpResponse, HttpResponseBuilder, http::StatusCode};

/// Sheds load with 503 instead of 429.
struct ShedResponder;

impl RejectionResponder for ShedResponder {
    fn respond(&self, info: &RejectionInfo, mut builder: HttpResponseBuilder) -> HttpResponse {
        builder
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(format!("{} is busy, try again shortly", info.namespace))
    }
}
```

## Shadow mode, outcomes and metrics

`permissive(true)` tracks usage and records every decision but admits every request. Use it to
size a limit against real traffic. Handlers read decisions through `RateLimitInfo`, which needs
`permissive(true)` or `record_outcome(true)`:

```rust
use actix_trypema::{LimitOutcome, RateLimitInfo};
use actix_web::{HttpResponse, Responder};

async fn handler(info: RateLimitInfo) -> impl Responder {
    match info.outcome() {
        LimitOutcome::Limited { .. } | LimitOutcome::Suppressed { admitted: false, .. } => {
            HttpResponse::Ok().body("would have been limited")
        }
        _ => HttpResponse::Ok().body("within limits"),
    }
}
```

With stacked limiters, `info.for_namespace("ip")` reads one of them. Use `Strategy::Suppressed`
in shadow mode for accurate counts; absolute rejections record nothing in trypema.

With the `metrics` feature, every limiter reports `ratelimit_admitted_total`,
`ratelimit_rejected_total`, `ratelimit_bypassed_total`, `ratelimit_backend_errors_total` and
`ratelimit_backend_latency_seconds`, labelled by `namespace` and `strategy`.

## Managing keys

`derived_key(namespace, key)` returns the key a limiter stores. Pass it to the backend's provider
to inspect a client, lift a block, or change its rate:

```rust
use actix_trypema::{Local, derived_key};
use trypema::{RateLimit, WindowSize};

let backend = Local::new(WindowSize::seconds_or_panic(60))?;
let key = derived_key("ip", "203.0.113.7"); // "ip_r_203.0.113.7"
let limiter = backend.provider().absolute();

let used = limiter.get(&key);
limiter.delete(&key); // the client's next request starts fresh
limiter.set_rate_limit(&key, &RateLimit::per_minute_or_panic(600.0));
```

With `Redis` and `Hybrid`, the same calls are async and take `RedisKey::try_from(key)?`.

## Performance

Time per request through actix's test harness with a no-op handler on an Apple M2 Pro
(`cargo bench --bench overhead --features redis`, Redis in Docker on the same machine):

| Limiter | Distributed | Time per request | Added over no limiter |
|---|---|---|---|
| no limiter | — | 545 ns | — |
| actix-governor (GCRA) | **no**: per-process counters | 611 ns | +66 ns |
| actix-trypema `Local` | no | 673 ns | +128 ns |
| actix-trypema `Hybrid` | yes, eventually consistent | 658 ns | +113 ns |
| actix-trypema `Redis` | yes | 182 µs | one Redis round trip |

> **actix-governor is not a distributed rate limiter.** Each process keeps its own counters, so
> `n` instances admit `n` times the limit. Compare it with `Local`; `Hybrid` and `Redis` share one
> limit across instances.

- The total includes building the test request and running the handler (the "no limiter" row);
  the added column is the limiter's own cost.
- Each figure is the median of three runs; between runs the added cost varied by up to 15 ns.
- One client sends every request, the limit is never reached, and rate limit headers are off, so
  only the admit path is measured.
- With headers off and without `metrics`, the `Local` and `Hybrid` admit paths allocate nothing
  (tests enforce it).

There is no actix middleware for redis-cell. In trypema's own suite (100k keys, tokio), a
redis-cell `CL.THROTTLE` call takes 259 µs at p50 against 396 µs for trypema's Redis provider;
see [trypema's benchmarks](https://trypema.davidoyinbo.com/benchmarks/benchmark-results).

## Examples

- `local_ip`: per-IP limiting on the in-process backend.
- `api_key_tiers`: plan tiers keyed on a validated API key.
- `redis_login`: a distributed login limiter behind a load balancer, failing closed.
- `hybrid_shadow_mode`: shadow mode on the hybrid backend.

Run with `cargo run --example local_ip`. The Redis examples need `--features redis` and
`docker compose up -d`. Tests: `docker compose up -d && cargo test --features redis`.

## License

MIT
