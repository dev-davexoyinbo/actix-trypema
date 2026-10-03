# actix-trypema

actix-web middleware for the [trypema](https://crates.io/crates/trypema) sliding-window rate
limiter: in-process, Redis or hybrid backends behind one API, with safe client-IP resolution
behind proxies. API docs: [docs.rs/actix-trypema](https://docs.rs/actix-trypema).

## Install

```sh
cargo add actix-trypema trypema          # trypema provides RateLimit and WindowSize
cargo add actix-trypema --features redis # Redis and Hybrid backends
cargo add redis --features tokio-comp,connection-manager  # to build the ConnectionManager
```

## Quick start

```rust,no_run
use actix_trypema::{Local, TrypemaLimiter, extract::PeerIp};
use actix_web::{App, HttpServer, web};
use trypema::{RateLimit, WindowSize};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Build the backend ONCE, outside HttpServer::new: inside the closure every worker would
    // get its own limiter, multiplying the effective limit by the worker count.
    let backend = Local::new(WindowSize::seconds_or_panic(60)).expect("valid backend");

    let limiter = TrypemaLimiter::builder(backend)
        .namespace("ip")
        .extractor(PeerIp::default())
        .rate(RateLimit::per_minute_or_panic(60.0))
        .build()
        .expect("valid middleware configuration");

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

Excess requests get `429 Too Many Requests` with a rounded-up `retry-after`, without reaching
the handler.

## Backends

| Backend | Feature | Shared across instances | Notes |
|---|---|---|---|
| `Local` | — | no | In-process. |
| `Redis` | `redis` | yes | One Redis round trip per request; Redis 7.2+. |
| `Hybrid` | `redis` | yes, eventually | Local fast path, periodic Redis sync. Admission lags Redis by the sync interval; unflushed counts can be lost at shutdown. |

```rust,ignore
let local = Local::configured(window, |builder| builder.bucket_size(BucketSize::milliseconds_or_panic(10)))?;
let redis = Redis::configured(connection.clone(), window, |builder| {
    builder.prefix(RedisKey::try_from("api").unwrap())
})?;
let hybrid = Hybrid::new(connection, window)?;
```

A backend handle builds the trypema provider and applies the window last, so the advertised
limit always matches the enforced one. Other provider options go through trypema's builder;
`provider()` returns the provider for administration.

## Builder options

| Option | Default | Purpose |
|---|---|---|
| `namespace`, `extractor`, `rate` | required | Key isolation, who a request is, the limit. |
| `rate_fn(\|req, key\| ..)` | — | Per-request rate (plan tiers). Rates are sticky per key: see below. |
| `cost_fn(\|req\| ..)` | 1 | Charge more per request; 0 bypasses. |
| `strategy` | `Absolute` | `Suppressed` sheds load probabilistically instead of a hard cut-off. |
| `exclude(\|req\| ..)` | — | Bypass matching requests (health checks, methods). Repeated calls combine. |
| `allow_keys([..])` | — | Never limit these extracted keys (masked IPv6 text, hashed header values). |
| `on_key_error` | `Reject` (400) | Or `Bypass`, or `UseGlobalKey` (one shared bucket). |
| `headers` | `Legacy` | `x-ratelimit-*`, `Ietf` (draft 09, unstable), or `Off` (recommended on auth routes). |
| `remaining_header(true)` | off | Adds the remaining count; costs one extra provider read. |
| `suppressed_retry_hint` | none | `retry-after` for suppressed rejections, which carry none. |
| `responder` | text 429 | Custom `RejectionResponder` (or `JsonResponder` with `json`); may change status. |
| `permissive(true)` | off | Shadow mode: track and record everything, never reject. |
| `record_outcome(true)` | off | Let handlers read the verdict through `RateLimitInfo`. |
| `on_backend_error`, `backend_timeout` | `FailOpen`, 50 ms | Redis/Hybrid only; see below. |

## Keys

- `PeerIp` keys by the TCP peer and never reads headers. IPv6 clients are grouped by `/64`
  (`ipv6_prefix_len` changes it); IPv4-mapped and NAT64 forms canonicalize to IPv4.
- `RealIp::xff`, `RealIp::forwarded` (RFC 7239) and `RealIp::header` read the client from a
  forwarding header **only** when the TCP peer is in `TrustedProxies`. The chain is walked right
  to left; the first untrusted hop is the client and nothing left of it is parsed. A malformed
  trusted chain is a key error, never a silent fallback. There is no private-network preset and
  `/0` is rejected.
- `Header::new("x-api-key")?.hashed()` keys on a header, hashed so credentials stay out of limiter
  state. **Key only on values an earlier middleware authenticated**: every attacker-chosen value
  mints a fresh bucket.
- `Composite` joins two extractors, `Global` puts every request in one bucket, `extractor_fn`
  takes a closure, and `KeyExtractor` is the trait for your own.

trypema rates are **sticky per key**: the first rate stored for a key wins. Put the plan tier in
the key, or change it explicitly with `derived_key`, the stable encoding of every stored key:

```rust,ignore
let key = actix_trypema::derived_key("apikey", "pro:account-2");
local.provider().absolute().set_rate_limit(&key, &new_rate); // or delete(&key) to unban
```

## Shadow mode and outcomes

`permissive(true)` admits everything while tracking usage; handlers read what would have
happened:

```rust,ignore
async fn handler(info: RateLimitInfo) -> impl Responder {
    match info.outcome() {
        LimitOutcome::Limited { .. } => HttpResponse::Ok().body("would have been limited"),
        _ => HttpResponse::Ok().body("within limits"),
    }
}
```

Stacked instances are read with `info.for_namespace("apikey")`. Absolute rejections record
nothing in trypema, so use `Strategy::Suppressed` for accurate shadow counts.

## When Redis fails

Every backend call is bounded by `backend_timeout`. Failures follow `on_backend_error`:
`FailOpen` (default) admits; `FailClosed` returns 503 (use it on login and OTP routes);
`Fallback(local)` enforces per instance on a `Local` backend with the same window; `Custom(fn)`
decides per `BackendFailure`. Failures are logged at `warn` at most once per second per process,
with the namespace and failure kind only.

## Placement

- `App::wrap` runs in **reverse** registration order: the last `.wrap` sees the request first.
- Put IP limiters outermost, before auth; identity limiters inside the layer that authenticates
  the identity.
- Register `NormalizePath` to run first if `exclude` relies on normalized paths.

## Performance

Time per request through actix's test harness with a no-op handler, all in one run on an Apple M2
Pro (`cargo bench --bench overhead --features redis`, Redis in Docker on the same machine):

| Limiter | Distributed | Time per request | Added over no limiter |
|---|---|---|---|
| no limiter | — | 572 ns | — |
| actix-governor (GCRA) | **no**: per-process counters | 689 ns | +117 ns |
| actix-trypema `Local` | no | 721 ns | +150 ns |
| actix-trypema `Hybrid` | yes, eventually consistent | 1.10 µs | +524 ns |
| actix-trypema `Redis` | yes | 223 µs | one Redis round trip |

There is no actix middleware for redis-cell. In trypema's own suite (100k keys, tokio), a
redis-cell `CL.THROTTLE` call takes 259 µs at p50 against 396 µs for trypema's Redis provider;
see [trypema's benchmarks](https://trypema.davidoyinbo.com/benchmarks/benchmark-results) for the full tables.
With headers off and without `metrics`, the `Local` admit path allocates nothing (a test enforces
it).

## Examples

- `local_ip`: per-IP limiting on the in-process backend.
- `api_key_tiers`: plan tiers keyed on a validated API key.
- `redis_login`: distributed login limiter behind a load balancer, failing closed.
- `hybrid_shadow_mode`: shadow mode on the hybrid backend.

Run with `cargo run --example local_ip` (the Redis ones need `--features redis` and
`docker compose up -d`).

## Features

| Feature | Adds |
|---|---|
| `redis` | `Redis` and `Hybrid` backends (tokio, the runtime actix-web runs on). |
| `json` | `JsonResponder`. |
| `metrics` | Decision counters (each request counts one of admitted, rejected or bypassed; backend failures also count) and a backend-latency histogram. |

MSRV 1.88. Tests: `docker compose up -d && cargo test --features redis`.

## License

MIT
