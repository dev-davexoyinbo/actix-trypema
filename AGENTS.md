# actix-trypema Contributor and Agent Guide

This file applies to the full repository. It gives the behavior, security, test and workflow
rules that each change to actix-trypema must keep.

Read `CONVENTIONS.md` with this file. `AGENTS.md` owns the behavior contracts, the workflow and
the verification steps. `CONVENTIONS.md` owns the code layout, the names, the API shape, the
source style, the tests, the benchmark and the documentation.

The words **must** and **must not** are intentional. If the code seems to disagree with this
guide, read the recent commits and the tests first. Do not weaken a test or change a contract
only to make the code pass.

## 1. Ownership, Scope and Git

The owner often works in a dirty worktree and makes commits near your work. Before you edit:

```bash
git status --short
git log -8 --oneline --stat
git diff --check
```

Follow these rules:

- Treat recent owner commits as authoritative. Do not amend, reorder, squash, revert or rewrite
  them.
- Keep all uncommitted changes that you did not make. Do not reset, restore, stash or overwrite
  them. Do not switch branches.
- Keep your edits in the scope of the request. If a file has uncommitted edits, read its full
  diff before you change code near them.
- An agent never commits, pushes, tags or publishes unless the owner says so in that
  conversation. An earlier approval or a finished task is not permission. The owner publishes the
  crate.
- Do not create branches or pull requests unless the owner asks.
- Commit messages and pull request bodies have no `Co-Authored-By` or `Generated with` lines.
  They do not mention AI tools.
- Do not commit `target/`, Criterion reports or Redis data.
- When a behavior changes for one backend, examine all nine backend types and both request
  paths.

## 2. Repository Shape and Feature Model

| Path | Contents |
| --- | --- |
| `src/lib.rs` | Crate docs (the docs.rs page), the module list and the public re-exports |
| `src/backend.rs` | The sealed traits `Backend`, `ProviderBackend` and `AsyncBackend`, and the crate-private `SyncBackend` |
| `src/builder.rs` | `TrypemaLimiter`, `TrypemaLimiterBuilder`, `Strategy`, `HeaderMode`, the internal `Config` and `window_limit()` |
| `src/middleware.rs` | `TrypemaMiddleware`, the `Drive` trait, the shared steps `pre_decide` and `finish`, `LimiterFuture`, the sync path and the metrics calls |
| `src/async_middleware.rs` | The async path: the hybrid fast path, the timeout and the failure policy. Only with `redis`. |
| `src/key.rs` | Namespace validation, `derived_key` and the key encoding |
| `src/extract.rs` | `KeyExtractor`, `KeyBuf` and the built-in extractors |
| `src/client_ip.rs` | `TrustedProxies` and the forwarding-header parsers behind `RealIp` |
| `src/response.rs` | `RejectionResponder`, the built-in responders, `RejectionInfo` and the rate limit headers |
| `src/outcome.rs` | `LimitOutcome` and the `RateLimitInfo` extractor |
| `src/error.rs` | `ConfigError`, `KeyError`, the two policies, `BackendFailure` and `ErrorAction` |
| `src/tests/` | Unit tests of crate-private items, one `test_<module>.rs` file for each module |
| `tests/local/` | Integration tests on the local backends (test binary `local`) |
| `tests/redis/` | Integration tests on the async backends, against a real Redis (test binary `redis`) |
| `tests/common/` | Helpers that both test binaries include through `#[path]` |
| `tests/allocations.rs` | The allocation guard of the admit path (section 6) |
| `benches/overhead.rs` | The Criterion benchmark of the cost of one request |
| `examples/` | Four runnable servers |
| `README.md`, `CHANGELOG.md` | The crates.io and GitHub page, and the release record |

The Actix Web pages of the external guide are in the `trypema-docs` repository, in
`content/5.actix-web/`. The site is `https://trypema.davidoyinbo.com/actix-web`.

Feature rules:

- The crate has no default features.
- `redis` turns on `trypema/redis-tokio`. actix-web runs on tokio, so there is no smol feature.
- `json` adds `JsonResponder` through `serde_json`. `metrics` adds the counters and the latency
  histogram through the `metrics` facade.
- The local backends compile with no feature. Without `redis`, no `redis` crate is in the normal
  dependency tree. CI checks this.
- All 8 combinations of `redis`, `json` and `metrics` must compile with zero warnings in all
  targets. The targets are the library, the tests, the examples and the benchmark.
- The crate uses Rust 2024 and MSRV 1.88 (`rust-version` in `Cargo.toml`). Do not use a language
  or library feature that is newer than Rust 1.88.
- `src/lib.rs` has `#![deny(missing_docs)]` and `#![forbid(unsafe_code)]`. Each public item has
  Rustdoc. The only `unsafe` code is the counting allocator in `tests/allocations.rs`, which is a
  separate crate.
- CI and `compose.yaml` use Redis 7.2. trypema's Redis backends work with Redis 7.2 or later.

## 3. Backend Contracts

### 3.1 The sealed trait family

- `Backend` has the supertrait `sealed::Sealed`, so only this crate can implement it.
- The `backend!` macro implements `Sealed` and `Backend` for `Arc<T>` of nine trypema types:
  three providers and six single limiters.
- `Backend::Kind` is `SyncKind` for the local types and `AsyncKind` for the Redis and hybrid
  types. It selects the request path through `Drive` (section 4).
- `Backend::STRATEGY` is `None` for a provider. For a single limiter, it is the strategy of that
  limiter.
- `ProviderBackend` marks the three providers. `strategy(..)` is only in
  `impl<B: ProviderBackend> TrypemaLimiterBuilder<B>`. The `compile_fail,E0599` doctest on
  `strategy` proves that a single limiter has no `strategy(..)`. Keep it.
- `AsyncBackend` is public, but its methods are `#[doc(hidden)]`. `on_backend_error(..)` and
  `backend_timeout(..)` are only in `impl<B: AsyncBackend> TrypemaLimiterBuilder<B>`.
- `SyncBackend` is crate-private. Do not make it public.
- A single limiter ignores the `strategy` argument of `backend_inc`. `build()` stores
  `B::STRATEGY.unwrap_or(self.strategy)`.
- The usage reads of the hybrid types use `get_estimate`, so they do not read Redis in steady
  state. The Redis types use `get`.

When you add or remove a backend type, update all of these items in one change:

- The `backend!` list, and the `SyncBackend` or `AsyncBackend` impl.
- The `ProviderBackend` impl, for a provider.
- The backend tables in `src/backend.rs`, `src/lib.rs`, `README.md` and the guide.
- `tests/local/backends.rs` or `tests/redis/backends.rs`.
- `CHANGELOG.md`.

### 3.2 Window and limit

- `build()` reads the window from the backend with `window_size()`. So the advertised limit is
  always the enforced limit. Do not add a window option to the middleware.
- `window_limit()` computes the capacity as trypema does: the rate per second times the window
  in seconds, truncated to `u64`.
- For a fixed rate with a capacity of zero, `build()` returns `ConfigError::ZeroCapacity`.
  `build()` cannot check a `rate_fn`.
- The `Fallback` provider must use the window of the backend. If it does not, `build()` returns
  `ConfigError::FallbackWindowMismatch`.
- trypema rates are sticky for each key: the first stored rate stays. The Rustdoc of `rate_fn`
  must keep this warning.

## 4. Request Pipeline

`Drive` has two impls: `SyncKind` in `src/middleware.rs` and `AsyncKind` in
`src/async_middleware.rs`. Both paths share `pre_decide` and `finish`.

### 4.1 Order of steps

`pre_decide` runs these steps in this order:

1. If an `exclude` predicate matches, the request bypasses the limiter.
2. The extractor writes the extracted key into a `KeyBuf`. On a failure, `KeyErrorPolicy`
   decides. In permissive mode, `Reject` becomes a bypass.
3. If the extracted key is in the `allow_keys` list, the request bypasses the limiter.
4. `pre_decide` resolves the rate and the cost. A cost of 0 bypasses the limiter.
5. `compose_key` writes the derived key into a second `KeyBuf`.

Do all work that borrows the request before the first `.await`. A bypassed request gets no rate
limit headers.

### 4.2 Decisions and responses

- `map_decision` maps the trypema `RateLimitDecision`. `Allowed` and an admitted `Suppressed`
  forward the request. `Rejected` and a `Suppressed` with `is_allowed: false` reject it.
- `build_rejection` makes the `429 Too Many Requests` response. It puts the `RejectionInfo` in
  the response extensions.
- A rejection has `retry-after` only when the hint is not zero (`nonzero_duration`). A suppressed
  rejection uses `suppressed_retry_hint`. `retry-after` is in whole seconds, rounded up, with a
  minimum of 1.
- Permissive mode forwards each rejection and records the outcome.
- On an admitted response, rate limit headers never replace a header that the handler set
  (`insert_if_absent`). `HeaderMode::Off` sends no rate limit headers.
- The middleware reads the remaining count only with `remaining_header(true)`, and only on an
  admission or in permissive mode.
- The middleware never polls the body of a rejected or bypassed request.
  `tests/local/rejections.rs` checks this.

### 4.3 Counts and outcomes

- Each request adds 1 to exactly one of `ratelimit_admitted_total`, `ratelimit_rejected_total`
  and `ratelimit_bypassed_total`. A key-error rejection counts as rejected.
- A backend failure also adds 1 to `ratelimit_backend_errors_total`. The policy then decides if
  the request counts as admitted or rejected.
- `ratelimit_backend_latency_seconds` records only calls that await Redis.
- Each metric has the labels `namespace` and `strategy`. Keep the names and the labels stable,
  because user dashboards read them.
- The middleware records outcomes only with `record_outcome(true)` or `permissive(true)`.

### 4.4 The async path

- The hybrid fast path decides from local state with no `RedisKey`, no boxed future and no
  timeout. It runs when all of these conditions are true:
  - `pre_decide` returns `Proceed`.
  - `remaining_header` is off.
  - The derived key is a valid `RedisKeyRef`.
  - `try_backend_inc` returns `Ok(Some(decision))`.
- All other requests take the boxed path. There, each Redis call runs under `backend_timeout`
  through `timed`. A timeout or a trypema error is a `BackendFailure`.
- On a failure, permissive mode admits the request. If permissive mode is off,
  `BackendErrorPolicy` decides:
  - `FailOpen`, the default, admits the request.
  - `FailClosed` returns `503` with a fixed text body.
  - `Fallback` runs the same derived key, rate, cost and strategy on the local provider.
  - `Custom` returns an `ErrorAction`.
- If the remaining read fails, the response has no remaining count. The decision does not
  change.
- A derived key is always a valid `RedisKey`. If the conversion fails, `debug_assert!` fires and
  the response is a generic `500`.

## 5. Key Encoding

`src/key.rs` owns the derived key. It is a public and stable contract. Redis stores state under
it, and operators compute it with `derived_key`.

- A namespace matches `[a-z0-9-]{1,32}`. The charset has no `_` and no `:`. This keeps the
  encoding injective across namespaces.
- The raw form is `{namespace}_r_{key}`. Each `_` in the key becomes `__`, and each `:` becomes
  `_c`.
- If the raw form is longer than 255 bytes (`MAX_DERIVED_KEY_LEN`), the key is
  `{namespace}_h_{32 hex}`. The hex is the first 16 bytes of the SHA-256 of the extracted key.
- A derived key is never longer than 255 bytes and has no `:`. So it is always a valid trypema
  `RedisKey`.
- `Composite` writes a length prefix (`{len}.`) before the first part. So `("a", "bc")` and
  `("ab", "c")` give different keys.
- A change to the encoding makes all stored state unreachable and breaks operator code. It is a
  breaking change. Make it only when the owner asks, and record it in `CHANGELOG.md`.
- `src/tests/test_key.rs` pins exact keys and one SHA-256 value. Its `proptest` cases check that
  derived keys are valid and injective. Keep these tests.
- `derived_key` panics on an invalid namespace, because no middleware can have that namespace.
  Its `# Panics` section says so.

## 6. The Admit Path Has No Extra Work

The admit path is the path of an admitted request, with headers off and without `metrics`. Do
not add an allocation or other work for each request to this path.

These parts keep it free of allocations:

- `KeyBuf` holds 256 bytes inline (`KEY_BUF_INLINE_CAPACITY`). A `const` assert in `src/key.rs`
  makes sure that each derived key fits. Do not box or shrink `KeyBuf`.
- `PreDecision` holds the `KeyBuf` inline. Its `#[expect(clippy::large_enum_variant, ...)]`
  gives the reason. Do not remove it.
- The local backends and the hybrid fast path do not box `LimiterFuture`. Only a call that
  awaits Redis boxes its future.
- The metrics labels share the namespace `Arc`, with no string copy for each request.
- `write_ipv4` writes IPv4 text without the `fmt` machinery. The extractors parse header names
  once, at build time.
- Header values go in as `HeaderValue::from(n)` or as a `String`. The code never formats a
  header value and then parses it again.

`tests/allocations.rs` installs a counting global allocator. It asserts zero allocations in
steady state for these cases:

- `PeerIp` admissions and `allow_keys` bypasses.
- `RealIp::xff` behind a trusted proxy.
- `Header` keys, raw and hashed.
- The hybrid admit path, with `redis` and a Redis at `REDIS_URL`.

The file has `#![cfg(not(feature = "metrics"))]`, because the `metrics` facade allocates for
each recorded decision. So the CI test run with all features skips it. Run it without
`metrics` (section 10.3).

These options add work, and their Rustdoc must say so:

- `extractor_fn` allocates a `String` for each request.
- `record_outcome(true)` and `permissive(true)` allocate the outcome record.
- `remaining_header(true)` adds a usage read and stops the hybrid fast path.
- The `metrics` feature allocates for each recorded decision.

When a change touches the admit path, run the allocation tests and the benchmark. For a new
extractor or a new backend path, add a case to `tests/allocations.rs`.

## 7. Security and Privacy

### 7.1 Logs

Never log a key (extracted or derived), request contents, Redis error text or connection
details. The crate has four log sites. Keep their fields as they are:

| Site | Level | Fields |
| --- | --- | --- |
| `pre_decide`, key extraction failure | `debug` | The namespace and the `KeyError` |
| `build_rejection` | `debug` | The namespace, the strategy and `retry_after_secs` |
| `warn_backend_failure` | `warn` | The namespace and the failure kind: `timeout`, `redis` or `other` |
| `RateLimitInfo::from_request`, no recorded outcome | `warn`, once for each process | A fixed configuration hint |

- `warn_backend_failure` logs at most once a second for each process.
- Never log a `BackendFailure` or a `TrypemaError` through `Display` or `Debug`.
- The `KeyError` in the `debug` log is safe, because each variant carries only configuration
  (section 7.2).

### 7.2 Errors and response bodies

- Key errors do not echo request data. A `KeyError` carries a configured header name or a
  `&'static str` reason. Never put request bytes in a `KeyError`.
- The `400` body of a key error is the fixed text `invalid rate limiting key`.
- The failure bodies also use fixed text:
  - A backend-failure rejection (`FailClosed`, or `Custom` with `Reject`): `rate limiter
    unavailable`.
  - A `RateLimitInfo` with no recorded outcome (`500`): `rate limit outcome unavailable`.
  - An invalid derived key (`500`): `invalid derived rate limiting key`.
- A `ConfigError` can contain configuration values, such as a namespace or a proxy entry. It
  never contains request data.
- `DefaultResponder` and `JsonResponder` do not put the key in the body. `RejectionInfo::key`
  goes only to the responder and to the response extensions.
- The `Debug` output of `TrypemaLimiter` shows only the namespace. The `Debug` output of
  `BackendErrorPolicy` shows only the variant name.

### 7.3 Client addresses

- `RealIp` reads a forwarding header only when the TCP peer is in `TrustedProxies`. A unit test
  asserts that the header is not read for other peers.
- `RealIp` reads the header from the right. It skips trusted hops, and the first untrusted hop is
  the client. It does not parse the bytes to the left of that hop.
- A malformed hop, or more than 32 trusted hops (`MAX_FORWARDED_HOPS`), gives
  `KeyError::MalformedForwardedChain`.
- `TrustedProxies::new` rejects an empty list, a prefix length of 0 and host bits. It also
  rejects IPv4 networks in IPv4-mapped or NAT64 form. Do not add a preset for private networks.
- Trust checks use the full canonical address: IPv4-mapped and NAT64 addresses become IPv4.
  The `/64` mask of IPv6 clients (the default) applies only when `write_client_ip` writes the
  key text.
- `PeerIp` never reads a header.

### 7.4 Header keys and defaults

- `Header` rejects a raw value longer than `MAX_HEADER_VALUE_LEN` (1024 bytes).
  `Header::hashed()` cuts the value to that length, then hashes it.
- The docs tell users to use `hashed()` for credentials. They also tell users to key only on
  values that an earlier middleware validated. Keep these warnings.
- `BackendErrorPolicy::FailOpen` is the default. The docs recommend `FailClosed` for login and
  other sensitive routes, and `HeaderMode::Off` for authentication endpoints.

## 8. Public API Contracts

- `TrypemaLimiterBuilder::build()` returns `Result<TrypemaLimiter<B>, ConfigError>` and never
  panics. Add each new configuration check to `build()` as a `ConfigError` variant.
- Each validated constructor returns `Result<Self, ConfigError>`. Some also have an `_or_panic`
  twin with a `# Panics` section: `TrustedProxies::new_or_panic`, `Header::new_or_panic` and
  `PeerIp::ipv6_prefix_len_or_panic`.
- `Backend`, `ProviderBackend` and `AsyncBackend` are sealed traits. Users cannot implement
  them.
- `strategy(..)` exists only on provider backends. Keep the `compile_fail` doctest.
- `src/lib.rs` has `pub use trypema;`. Docs and examples import trypema types through
  `actix_trypema::trypema`.
- When the trypema dependency changes, update `Cargo.toml`, the crate docs, `README.md`,
  `CHANGELOG.md` and the guide in one change.
- Public enums and `RejectionInfo` are `#[non_exhaustive]`.
- Each breaking change gets an entry in `CHANGELOG.md`, in the Keep a Changelog format. Give a
  migration table from the old API to the new API. Follow Semantic Versioning.
- Keep `README.md`, the crate docs in `src/lib.rs` and the guide pages aligned. When public
  behavior changes, update all three, or tell the owner which pages do not agree.

## 9. Test Rules

Tests are contracts. They must fail when the code breaks a contract.

- Never delete, weaken or `#[ignore]` a test that checks real behavior. If a test fails, fix the
  code, or ask the owner.
- A test observes the real effect, not a proxy. Send requests through actix
  (`test::init_service` and `test::call_service`) or through a real `HttpServer`.
- Assert what users see: the status, the headers, the `RejectionInfo` extension, the stored
  usage or the metrics recorder. Read the stored usage with the backend `get` and `derived_key`.
- A regression test must fail under the bug that it is for. Prove this with a mutation:
  1. Put the bug back in the code.
  2. Run the test, and see it fail.
  3. Undo the change.
  4. Run `touch` on the changed file. A restore with `mv` or `cp` can keep an old mtime, and
     cargo then keeps the mutant build.
  5. Run the test, and see it pass.
- Do not undo a mutation with `git restore` or `git checkout` in a file with uncommitted owner
  changes.
- For synchronization, use a bounded poll with a deadline. Never use a fixed sleep. The polls in
  `tests/redis/backends.rs` and `tests/redis/failures.rs` show the pattern.
- Each Redis test uses its own key prefix from `support::unique_prefix()`.
- Inject backend failures with `FlakyProxy` in `tests/redis/flaky_proxy.rs`. Do not stop the
  shared Redis.
- Build Redis test backends with the helpers in `tests/redis/support.rs`. Their redis-rs
  timeouts (30 s) and their `backend_timeout` (10 s) prevent a false fail-open on a slow
  machine. A test of the timeout sets a short timeout itself.
- Metrics tests use `metrics::with_local_recorder` and `CountingRecorder`, so parallel tests do
  not share counts.
- A probabilistic assertion, as for the suppressed strategy, uses loose bounds. A comment gives
  the reason for the bounds.
- Test-only methods, types and trait impls do not go in production files. The only
  `#[cfg(test)]` item in a production file is the `mod tests;` declaration in `src/lib.rs`. Unit
  tests reach crate-private items through `pub(crate)`, such as `timed` and
  `MAX_FORWARDED_HOPS`. Keep this state.

## 10. Verification Matrix

Run checks in proportion to the change. For a change to a request path, a backend, a feature
gate or a dependency, run the full matrix.

### 10.1 CI checks

These commands come from `.github/workflows/ci.yml`, and they run without Redis. CI also runs
`cargo test --features redis,json,metrics` (section 10.3).

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets --features json -- -D warnings
cargo clippy --all-targets --features metrics -- -D warnings
cargo clippy --all-targets --features redis,json,metrics -- -D warnings
if cargo tree -e normal --prefix none | grep -E '^redis '; then
  echo "redis crates leaked into the no-feature tree" >&2
fi
cargo test
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --features redis,json,metrics
git diff --check
```

### 10.2 All feature sets

CI lints 4 of the 8 feature sets. When you change a `cfg`, a feature, an import or a lint
attribute, lint all 8:

```bash
for features in "" redis json metrics redis,json redis,metrics json,metrics redis,json,metrics; do
  cargo clippy --all-targets --features "$features" -- -D warnings || break
done
```

For a change to `json` or `metrics` code, also run these tests. They run without Redis:

```bash
cargo test --features json
cargo test --features metrics
```

### 10.3 Tests with Redis

```bash
docker compose up -d
cargo test --features redis,json,metrics
cargo test --features redis --test allocations
docker compose down
```

- The second `cargo test` runs the allocation tests with the hybrid case. The all-features run
  skips them (section 6).
- The tests use `redis://127.0.0.1:16379/` by default. Set `REDIS_URL` for a different Redis.
  `FlakyProxy` supports only plain `redis://host:port/` URLs.
- `REDIS_PORT` sets the host port in `compose.yaml`.
- Run one test binary or one test with `cargo test --test local <name>` or
  `cargo test --features redis --test redis <name>`.

### 10.4 MSRV

The CI `msrv` job runs these checks on Rust 1.88:

```bash
cargo +1.88 check --locked
cargo +1.88 check --locked --features redis,json,metrics
```

### 10.5 Benchmark

```bash
cargo bench --bench overhead --no-run
cargo bench --bench overhead
cargo bench --bench overhead --features redis
```

Start Redis (`docker compose up -d`) before the `redis` run. The table in the `README.md`
Performance section comes from `cargo bench --bench overhead --features redis`. Update that table
and the guide performance page only from full runs on a quiet machine, and name the machine.

## 11. Final Review Checklist

Before you give the work back, confirm all applicable items:

- [ ] You did not touch owner commits or uncommitted owner changes. You did not commit, push,
      tag or publish.
- [ ] All 8 feature sets compile with zero warnings in all targets. There is no `#[allow]`.
- [ ] Without `redis`, no `redis` crate is in the normal dependency tree.
- [ ] The admit path has no new allocation or work. The allocation tests pass without `metrics`.
- [ ] Logs contain no key, request contents, Redis error text or connection details.
- [ ] `KeyError` values and fixed response bodies contain no request data.
- [ ] `RealIp` reads forwarding headers only from trusted peers.
- [ ] The key encoding has no change, or `CHANGELOG.md` records the change as breaking.
- [ ] `build()` reports each new configuration mistake as a `ConfigError` and does not panic.
- [ ] `strategy(..)` stays on provider backends only, and the `compile_fail` doctest passes.
- [ ] Each request counts as exactly one of admitted, rejected and bypassed.
- [ ] New tests observe real effects, use bounded polls and use unique Redis prefixes.
- [ ] Each regression test failed under a mutation. You ran `touch` on the mutated file after
      the restore.
- [ ] Production files contain no test-only code.
- [ ] `README.md`, the crate docs, the guide and `CHANGELOG.md` agree with the behavior.
- [ ] The applicable parts of the verification matrix pass.

## 12. Common Failure Modes

- Do not log a `BackendFailure`, a `TrypemaError` or a key to add context.
- Do not put request bytes in a `KeyError` or a `ConfigError`.
- Do not box `KeyBuf` or the admit-path future to satisfy a lint.
- Do not use `#[allow]`. Use `#[expect(lint, reason = "...")]`.
- Do not add `strategy(..)` to single-limiter builders, or `on_backend_error(..)` to local
  builders.
- Do not read a forwarding header before the trust check of the peer.
- Do not change the key escapes, the 255-byte limit or the hash length.
- Do not trust the all-features CI run for the allocation tests. That run skips them.
- Do not read a Redis test failure as a code failure before you make sure that Redis runs.
- Do not wait for a hybrid flush or a reconnect with a fixed sleep.
- Do not use a fixed Redis prefix in a test.
- Do not restore a mutated file with `mv` or `cp` and then forget `touch`.
- Do not stop the shared Redis to inject a failure. Use `FlakyProxy`.
- Do not build the backend inside the `HttpServer::new` closure in docs or examples. Each worker
  then gets its own limiter, and the worker count multiplies the effective limit.
- Do not add a window option to the middleware. The backend owns the window.
- Do not weaken a correct test to match a bug.
