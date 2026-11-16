# actix-trypema Code and Documentation Conventions

This file is the style and design guide for actix-trypema. It tells you how to shape code, APIs,
tests, the benchmark and documentation, so that changes stay consistent.

`AGENTS.md` gives the behavior contracts, the workflow and the verification matrix. This file
gives the shape of the code. Read both before a large change. If they seem to conflict, keep the
behavior and correctness first. Then follow the more specific rule. Do not change a public
contract only to satisfy a style rule.

## 1. Main Principles

Apply these principles in this order:

1. Keep correctness, security and the documented behavior.
2. Keep the admit path free of extra work.
3. Prefer simplification. Delete knobs, and avoid ceremony.
4. Prefer established Rust and actix-web conventions to new patterns.
5. Let `rustfmt` decide the mechanical format.
6. Keep the docs current with the code.

## 2. Sources of Authority

When a rule here does not decide a question, use this order:

1. The public API contracts, the tests and `AGENTS.md`.
2. The vocabulary and the module structure of this repository.
3. `cargo fmt` with its default settings. The repository has no `rustfmt.toml`.
4. The Rust API Guidelines and the Rustdoc Book.
5. The code near the change.

This crate depends on the public behavior of trypema. trypema's own `AGENTS.md` and
`CONVENTIONS.md` do not apply to this repository.

## 3. Module Organization

`AGENTS.md` section 2 lists each file and its contents. Follow these rules:

- Put behavior beside the type that owns it.
- Keep an item private or `pub(crate)` unless users must reach it.
- Users reach public items through the re-exports in `src/lib.rs` or through the public
  `extract` module. Add each new public item to one of the two.
- Put shared request steps in `pre_decide` and `finish`. Keep the sync path in
  `src/middleware.rs` and the async path in `src/async_middleware.rs`.
- `async_middleware` is the only library module behind a feature gate. Gate other feature code
  at the smallest item.
- Do not add a `utils` module. Give each module the name of what it owns.

## 4. Source Layout

- Run `cargo fmt --all`. The result must pass `cargo fmt --all -- --check`.
- Put a blank line after each block statement, before the next statement. Block statements are
  `if`, `match`, `for`, `while`, `loop` and a bare `{ }` scope. Match arms and `use` lists do not
  get a blank line.
- Close each `impl` block with `// end impl`. An empty `impl` on one line has no marker.
- Close each function or method with more than a few statements with a marker:
  - `// end constructor` for `new`.
  - `// end method <name>` for a method.
  - `// end fn <name>` for a free function or a private helper.
- Short accessors, setters and test functions have no marker.
- Use the exact marker words. Do not write a variant such as `// end of impl`.
- Use early returns for guards and bypasses. Use `let else` when absence returns early. Use let
  chains (`if let ... && ...`) to join conditions.
- Keep a local near its first use.

```rust,ignore
impl Header {
    pub fn new(name: &str) -> Result<Self, ConfigError> {
        Ok(Self {
            name: parse_header_name(name)?,
            hashed: false,
        })
    } // end constructor
} // end impl
```

## 5. Names and Vocabulary

Use the standard Rust case conventions. Use each of these terms for one item only:

| Term | Item |
| --- | --- |
| namespace | The `[a-z0-9-]{1,32}` name of one middleware instance |
| extracted key | The text that a `KeyExtractor` returns, before the namespace prefix |
| derived key | The key that the backend stores, from `compose_key` or `derived_key` |
| backend | The `Arc` of a trypema provider or limiter that the middleware drives |
| provider | A backend with both strategies |
| single limiter | A backend with one strategy |
| local backends | The three local types (`SyncBackend`, `SyncKind`) |
| async backends | The six Redis and hybrid types (`AsyncBackend`, `AsyncKind`) |
| admit, reject, bypass | The three verdicts that `Decision` counts |
| outcome | A `LimitOutcome` that the middleware records for handlers |
| limit | The capacity of one window, from `window_limit()` |
| cost | The units that one request uses |

- Name a trypema `RateLimitDecision` value `decision`.
- Use `is_` and `has_` names only for booleans: `is_valid`, `is_quoted`, `has_host_bits`.
- Put the unit in the name of a raw number: `window_secs`, `retry_after_secs`, `prefix_len`. Use
  `Duration` for times in the public API.
- Constants use `SCREAMING_SNAKE_CASE`: `MAX_DERIVED_KEY_LEN`, `KEY_BUF_INLINE_CAPACITY`.
- Metric names start with `ratelimit_`. Counters end with `_total`. The latency histogram ends
  with `_seconds`.
- Use full words for locals and closure arguments: `formatter`, `position`, `address`, `byte`.
  `req` for a request is the one short name. Do not use one-letter names.

## 6. Simplification

- Delete a knob before you add one. Add a builder option only for a real use case, and give it a
  safe default.
- Avoid ceremony. Do not add a wrapper type, a trait or a layer without a real second use or a
  clear reason.
- A fixed value can be a named constant, also when it has one use, if the name makes the code
  clearer. `MAX_NAMESPACE_LEN`, `DEFAULT_BACKEND_TIMEOUT` and `HASHED_BYTES` are examples.
- Add a function for one call site only when it names a concept or holds an invariant.
  `parse_hop`, `read_remaining` and `warn_backend_failure` are examples.
- Prefer one straight sequence of steps to callbacks, flags and indirection.
- Validate configuration once, in `build()` or in a constructor. Do not validate it again for
  each request.

## 7. Public API Design

- Builder setters take `mut self` and return `Self`. `TrypemaLimiterBuilder` has `#[must_use]`.
- A validated constructor returns `Result<Self, ConfigError>`. A panic twin, when there is one,
  has the `_or_panic` suffix and calls the fallible form.
- A builder value that can be invalid goes into the builder unchanged. `build()` checks it, as
  for `backend_timeout` and `ConfigError::ZeroBackendTimeout`.
- Use a `bool` setter only for an on and off switch: `permissive`, `record_outcome`,
  `remaining_header`. Use an enum for a choice with names: `KeyErrorPolicy`,
  `BackendErrorPolicy`, `HeaderMode`, `Strategy`.
- Public enums are `#[non_exhaustive]`. They derive `Default` when they have a default.
- User closures are `Fn(..) + Send + Sync + 'static`. The builder keeps them in an `Arc`.
- Keep struct fields private. `RejectionInfo` is the exception: it has public fields and
  `#[non_exhaustive]`.
- When a type that holds closures or a backend implements `Debug`, write the impl by hand. Show
  no closure, key or backend state.
- Seal each trait that users must not implement, through the `sealed` module in
  `src/backend.rs`. Give the internal methods of a public sealed trait `#[doc(hidden)]`.
- Put a setter for some backends only in an `impl` block with a trait bound. Then a wrong use
  does not compile. Add a `compile_fail` doctest with the error code.

## 8. Errors, Panics and Logs

- Use `thiserror` for error enums.
- `ConfigError` messages are lowercase. They name the problem and, when possible, the fix:
  `missing namespace: call namespace(..) before build()`.
- `KeyError` reasons are `&'static str` values.
- In library code, use `unwrap` or `expect` only when a local invariant proves the value. The
  `expect` message states the invariant, as in `"KeyBuf holds whole UTF-8 pushes"`. The
  `_or_panic` twins are the other permitted use.
- For a state that cannot occur on the async path, write `debug_assert!(false, ...)` and return a
  generic error. Do not panic in a request.
- Each public function that can panic has a `# Panics` section. Each public function that can
  fail has an `# Errors` section.
- Log through `tracing`. Use `debug` for rejections and key errors, and `warn` for problems that
  an operator must fix. `AGENTS.md` section 7.1 lists the permitted fields.
- Recover a poisoned mutex with `unwrap_or_else(|poisoned| poisoned.into_inner())`.

## 9. Lints and Feature Gates

- Never use `#[allow]`. Use `#[expect(lint, reason = "...")]`. The reason tells why the lint does
  not apply.
- For an item that one feature set does not use, write
  `#[cfg_attr(not(feature = "redis"), expect(dead_code, reason = "..."))]`.
- When the body of a function is behind a feature, consume the arguments in the other branch:
  `#[cfg(not(feature = "metrics"))] let _ = (config, decision);`.
- Put each `cfg` on the smallest item: a field, a variant, a `use`, an `impl` or a module.
- Gate a test file or a test module with the feature that it uses. `tests/redis/main.rs` has
  `#![cfg(feature = "redis")]`, and the `telemetry` modules are behind `metrics`.
- An example that uses a feature has `required-features` in `Cargo.toml`.

## 10. Async and Request Code

- Borrow the request only before the first `.await`. Move owned values into the async block.
- All workers share one `Config` in an `Arc`. Do not add state for each worker.
- Bound each Redis call with `timed`. Do not add a second timeout.
- Do not take a lock on the admit path. The only `Mutex`, `LAST_BACKEND_WARN`, is on the failure
  path.

## 11. Tests

`AGENTS.md` section 9 gives the test contracts. This section gives the layout and the shape.

### 11.1 Layout

- Unit tests of crate-private items go in `src/tests/test_<module>.rs`. Each file starts with
  ``//! Unit tests for `crate::<module>`.``
- Behavior that users can reach goes in an integration test: `tests/local/` for the local
  backends and `tests/redis/` for the async backends.
- Put each topic in its own file, and declare it in the `main.rs` of that test binary.
- Shared helpers go in `tests/common/`, for both binaries, or in the `support.rs` of one binary.
- Do not put a `mod tests` block in a source file.

### 11.2 Shape

- A test name is a `snake_case` sentence that states the behavior:
  `rejected_and_bypassed_request_bodies_are_never_polled`.
- Put related cases in a table and loop over it. Give each case a label in the assertion message.
- Prefer exact assertions. Use loose bounds only for probabilistic behavior, and give the reason
  in a comment.
- `unwrap` is correct in tests when the setup must succeed.
- Use the constants in `tests/common/mod.rs`: `CLIENT`, `CLIENT_KEY` and `WINDOW_SECS`.
- Use documentation addresses for clients (`203.0.113.0/24`, `198.51.100.0/24`,
  `2001:db8::/32`) and `10.0.0.0/8` for proxies.
- Use `proptest` for properties of the key encoding.

## 12. Benchmark

- `benches/overhead.rs` measures the cost of one request through the actix test harness. It
  compares each limiter with `bare` (no limiter) and with actix-governor.
- Keep the rates far above the benchmark volume, so that only the admit path runs. Keep headers
  off.
- Time only the request loop, with `iter_custom`.
- The variants for the async backends compile only with `--features redis` and use `REDIS_URL`.
- Do not assert timings in tests. Do not commit Criterion output.

## 13. Documentation

### 13.1 Document ownership

| Document | Owns |
| --- | --- |
| Crate docs in `src/lib.rs` | The docs.rs page: quick start, backends, keys, limits, middleware placement and features |
| Item Rustdoc | The behavior, errors and panics of each public item |
| `README.md` | The crates.io and GitHub page |
| `trypema-docs/content/5.actix-web/` | The full guide, with an example for each option |
| `CHANGELOG.md` | The release record |
| `examples/*.rs` | Runnable servers. The `//!` header gives the run command and the expected output. |
| `AGENTS.md`, `CONVENTIONS.md` | The rules for contributors and agents |

Keep the crate docs, `README.md` and the guide aligned on behavior, defaults, feature names and
versions.

### 13.2 Language

Write all prose in ASD-STE100 Simplified Technical English:

- Use only simple tenses, the imperative and the active voice.
- Do not use "-ing" forms, except in technical nouns.
- Write at most 20 words in a procedure sentence and at most 25 words in a descriptive sentence.
- Write one instruction in each sentence. Do not use semicolons or Latin abbreviations.
- Write "make sure" for "ensure", "let" for "allow" and "necessary" for "need".
- Use one term for one item (section 5).

You can use code identifiers and API names as technical nouns. Docs describe the current state.
Do not write "previously", "now" or "changed from". `CHANGELOG.md` is the exception, because it
records changes.

### 13.3 Rustdoc

- Use this order: one summary sentence, the behavior, `# Examples`, `# Errors`, `# Panics`.
- Doctests must run. Use `?` with a hidden `fn main() -> Result<..>` or with
  `Ok::<(), E>(())`.
- An example that needs Redis is `no_run`. Put its body in a hidden async function with
  `# #[cfg(feature = "redis")]`, as the Redis and hybrid examples in `src/lib.rs` do. Then the
  example compiles on docs.rs and with `redis`, and the build without `redis` skips it.
- Use `compile_fail,E0599` (or the applicable error code) for a contract that must not compile.
- Intra-doc links must be valid with no features. Write a feature-gated item as plain code, as
  the backend table in `src/backend.rs` does.
- In examples, build the backend once, outside `HttpServer::new`. Clone the limiter into the
  closure.
- Import trypema types through `actix_trypema::trypema`.
- Both `cargo doc` commands of CI must pass with `-D warnings`.

### 13.4 Markdown

- Use one H1 and a short introduction before the first H2.
- Use ATX headings (`## Heading`) with one blank line before and after.
- Give each fenced code block a language, such as `rust` or `bash`.
- Use backticks for identifiers, commands, file names, feature names and literal values.
- Wrap prose near 100 columns. Tables, links and code can be longer.
- Do not put whitespace at the end of a line.

### 13.5 Comments

- Comment only the non-obvious why: an invariant, a race, a security reason or a cost.
- Do not write a comment that restates the code.
- Put the description of a field or an item in its Rustdoc, not in a comment.
- Do not keep commented-out code.
- Update a comment when its code changes.

## 14. Cargo Features and Dependencies

- Give each feature the name of its capability: `redis`, `json`, `metrics`.
- Connect each optional dependency through `dep:`, as in `json = ["dep:serde_json"]`.
- `actix-web` has `default-features = false` in `[dependencies]`. Do not turn on default features
  for users. Dev-dependencies can turn on more features.
- Add a dependency only when the standard library and the current dependencies cannot do the
  work.
- A type from a new public dependency becomes part of the API. Get the approval of the owner
  first.
- `[package.metadata.docs.rs]` lists all features. Add each new feature there.
- The `exclude` list in `Cargo.toml` keeps `.github` and `compose.yaml` out of the package.
- `Cargo.lock` is in git. The MSRV job uses it with `--locked`.

## 15. Change Discipline

- Give each diff one purpose.
- Do not rename, reorder or reformat code that the task does not touch.
- Pair each behavior change with tests. Pair each public change with docs and a `CHANGELOG.md`
  entry.
- When you apply a convention, apply it to all backends and both request paths.

Quick checklist:

- [ ] Names use the vocabulary of section 5.
- [ ] New lint exceptions use `#[expect(lint, reason = "...")]`.
- [ ] Each `cfg` is on the smallest item, and all 8 feature sets compile without warnings.
- [ ] `impl` blocks and long functions have end markers.
- [ ] A blank line follows each block statement before the next statement.
- [ ] Comments explain only the non-obvious why.
- [ ] Public items have Rustdoc with the applicable `# Errors` and `# Panics` sections.
- [ ] Docs use STE and describe the current state.
- [ ] `cargo fmt --all -- --check` and `git diff --check` pass.

## 16. External Basis

- [The Rust Style Guide](https://doc.rust-lang.org/style-guide/)
- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/checklist.html)
- [The Rustdoc Book](https://doc.rust-lang.org/rustdoc/how-to-write-documentation.html)
- [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
- [ASD-STE100 Simplified Technical English](https://www.asd-ste100.org/)

These sources are advisory. The contracts in `AGENTS.md` and this file govern this repository.
