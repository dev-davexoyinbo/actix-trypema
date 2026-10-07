# Changelog

This file records all notable changes to `actix-trypema`.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). This crate uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - 2026-10-05

### Changed

- The default `backend_timeout` is now 200 ms. Before, it was 50 ms.
- This change applies to the `Redis` and `Hybrid` backends. It does not apply to `Local`.
- To keep the old value, set `backend_timeout(Duration::from_millis(50))` on the builder.

## [0.1.0] - 2026-10-04

This is the first release.

### Added

#### Middleware

- `TrypemaLimiter`: an actix-web middleware that limits requests with the trypema sliding-window
  rate limiter.
- `TrypemaLimiter::builder(backend)`: set a namespace, a key extractor and a rate on a limiter.
- `build()` checks the configuration. If the configuration is not correct, `build()` returns a
  `ConfigError`. The middleware does not fail at request time.
- A namespace must match `[a-z0-9-]{1,32}`. The namespace keeps limiters on one backend apart.
- Two strategies:
  - `Strategy::Absolute` (default): the limiter rejects all requests above the limit.
  - `Strategy::Suppressed`: the limiter rejects a larger part of requests as the load increases.
- `rate_fn`: set a different rate for each request or key.
- `cost_fn`: set a cost for each request. The default cost is 1.
- `exclude`: send some requests, for example health checks, around the limiter.
- `allow_keys`: let some keys, for example an office IP address, go around the limiter.
- `on_key_error`: set the action when the extractor cannot find a key. The options are
  `Reject` (`400`, default), `Bypass` and `UseGlobalKey`.

#### Backends

- `Local`: keeps all counts in memory in one process.
- `Redis` (`redis` feature): shares exact counts between instances through Redis 7.2 or later.
- `Hybrid` (`redis` feature): decides most requests from local memory. It syncs counts with Redis
  at a set interval.
- `on_backend_error`: set the action when a `Redis` or `Hybrid` call fails. The options are:
  - `FailOpen` (default): admit the request.
  - `FailClosed`: reject the request with `503`.
  - `Fallback(Local)`: decide with a `Local` backend that uses the same window.
  - `Custom(fn)`: decide for each `BackendFailure`.
- `backend_timeout`: set the maximum time for each `Redis` or `Hybrid` call. The default is 50 ms.

#### Key extractors

- `PeerIp`: uses the TCP peer address. It does not read headers. It puts IPv6 clients into
  groups by `/64` network. You can change the prefix length.
- `RealIp::xff`, `RealIp::forwarded` and `RealIp::header`: read the client address from
  `X-Forwarded-For`, `Forwarded` (RFC 7239) or a CDN header.
- `RealIp` reads these headers only when the TCP peer is in `TrustedProxies`.
- `RealIp` reads `X-Forwarded-For` from the right and skips trusted proxies. A header value that
  is not correct causes a key error. `RealIp` does not use a different key silently.
- `TrustedProxies` rejects `0.0.0.0/0`.
- `PeerIp` and `RealIp` use the IPv4 address in IPv4-mapped and NAT64 addresses.
- `Header`: uses the value of a request header. `Header::hashed()` hashes the value, so the raw
  value does not go into limiter state or logs.
- `Global`: puts all requests into one bucket.
- `Composite`: makes one key from two extractors.
- `extractor_fn`: makes a key from a closure.
- `KeyExtractor` trait: makes a custom extractor. It can write the key into `KeyBuf`, a 256-byte
  buffer. `KeyBuf` does not allocate memory for short keys.

#### Responses

- A rejected request gets `429 Too Many Requests`, a `retry-after` header in whole seconds, and a
  text body.
- `headers(HeaderMode)`: select the rate limit headers:
  - `Legacy` (default): `x-ratelimit-limit`.
  - `Ietf`: `ratelimit-policy` and `ratelimit`, from draft-ietf-httpapi-ratelimit-headers-09.
  - `Off`: no rate limit headers. The response still has `retry-after`.
- `remaining_header(true)`: adds the remaining count to the headers.
- `suppressed_retry_hint`: sets the `retry-after` value for suppressed rejections.
- `RejectionResponder` trait: makes a custom rejection response. It can also change the status.
- `JsonResponder` (`json` feature): gives a JSON body for rejections.

#### Shadow mode, outcomes and metrics

- `permissive(true)`: the limiter counts usage and records each decision, but admits all requests.
- `record_outcome(true)`: the limiter records each decision, so handlers can read it.
- `RateLimitInfo`: an extractor that gives the `LimitOutcome` to handlers.
  `for_namespace` gives the outcome of one limiter when you use more than one limiter.
- `metrics` feature: each limiter reports these metrics, with `namespace` and `strategy` labels:
  - `ratelimit_admitted_total`
  - `ratelimit_rejected_total`
  - `ratelimit_bypassed_total`
  - `ratelimit_backend_errors_total`
  - `ratelimit_backend_latency_seconds`

#### Key management

- `derived_key(namespace, key)`: gives the key that a limiter stores. Use this key with the
  backend provider to read, delete or change the rate of a client.

#### Performance

- The `Local` and `Hybrid` admit paths do not allocate memory when headers are off and the
  `metrics` feature is off. Tests make sure of this.
- The `overhead` benchmark measures the time for each request on all backends.

#### Examples

- `local_ip`: limits each IP address with the `Local` backend.
- `api_key_tiers`: sets different limits for each plan, with a validated API key.
- `redis_login`: limits logins behind a load balancer with `Redis`. It fails closed.
- `hybrid_shadow_mode`: shows shadow mode on the `Hybrid` backend.

### Requirements

- Rust 1.88 or later.
- actix-web 4.15 or later.
- trypema 2.2 or later.

[0.1.1]: https://github.com/dev-davexoyinbo/actix-trypema/releases/tag/v0.1.1
[0.1.0]: https://crates.io/crates/actix-trypema/0.1.0
