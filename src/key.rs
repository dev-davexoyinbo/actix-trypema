//! Derived limiter keys: namespace validation and the injective key encoding.

use std::fmt::Write as _;

use sha2::{Digest, Sha256};

use crate::{
    ConfigError,
    extract::{KEY_BUF_INLINE_CAPACITY, KeyBuf},
};

const MAX_NAMESPACE_LEN: usize = 32;

/// Derived keys never exceed trypema's `RedisKey` limit.
const MAX_DERIVED_KEY_LEN: usize = 255;

const HASHED_BYTES: usize = 16;

// Every derived key fits the KeyBuf inline buffer, which is what keeps key composition on the
// admit path allocation-free.
const _: () = assert!(MAX_DERIVED_KEY_LEN < KEY_BUF_INLINE_CAPACITY);

/// Validate a middleware namespace: `[a-z0-9-]{1,32}`.
///
/// The restricted charset (no `_`, no `:`) is what keeps [`derived_key`] injective across
/// namespaces.
pub(crate) fn validate_namespace(namespace: &str) -> Result<(), ConfigError> {
    let is_valid = !namespace.is_empty()
        && namespace.len() <= MAX_NAMESPACE_LEN
        && namespace
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');

    if !is_valid {
        return Err(ConfigError::InvalidNamespace(format!(
            "`{namespace}` must match [a-z0-9-]{{1,32}}"
        )));
    }

    Ok(())
} // end fn validate_namespace

/// Compute the key the middleware stores state under, for operator access.
///
/// Pass the middleware's namespace and the extracted key (for client-IP extractors that is the
/// dotted IPv4 address or the IPv6 `{network}/{prefix}` text). The result is what the limiter —
/// local or Redis — was driven with, so it can be handed to the backend's provider directly, for
/// example to lift a block with `delete` or change a tier with `set_rate_limit`.
///
/// The encoding is stable and injective: `{ns}_r_{key}` with `_` escaped as `__` and `:` as
/// `_c`; keys whose raw form would exceed 255 bytes use `{ns}_h_{32 hex}` (SHA-256 prefix)
/// instead.
///
/// # Examples
///
/// ```
/// use actix_trypema::derived_key;
///
/// assert_eq!(derived_key("ip", "203.0.113.7"), "ip_r_203.0.113.7");
/// assert_eq!(derived_key("ip", "2001:db8::/64"), "ip_r_2001_cdb8_c_c/64");
/// ```
///
/// # Panics
///
/// Panics when `namespace` does not match `[a-z0-9-]{1,32}`: no middleware can have been built
/// with it, and an invalid namespace would break the encoding's injectivity.
pub fn derived_key(namespace: &str, key: &str) -> String {
    assert!(
        validate_namespace(namespace).is_ok(),
        "namespace must match [a-z0-9-]{{1,32}}"
    );

    let mut buf = KeyBuf::new();
    compose_key(&mut buf, namespace, key);
    buf.as_str().to_string()
} // end fn derived_key

/// Write the derived limiter key for `namespace` and an extracted `key` into `buf`.
pub(crate) fn compose_key(buf: &mut KeyBuf, namespace: &str, key: &str) {
    let escaped_len = key.len()
        + key
            .bytes()
            .filter(|byte| matches!(byte, b'_' | b':'))
            .count();

    if namespace.len() + 3 + escaped_len > MAX_DERIVED_KEY_LEN {
        buf.push_str(namespace);
        buf.push_str("_h_");
        write_hex_hash(buf, key.as_bytes());
        return;
    }

    buf.push_str(namespace);
    buf.push_str("_r_");

    let mut rest = key;

    while let Some(position) = rest.bytes().position(|byte| byte == b'_' || byte == b':') {
        let (before, escaped) = rest.split_at(position);
        buf.push_str(before);

        match escaped.as_bytes().first() {
            Some(b'_') => buf.push_str("__"),
            _ => buf.push_str("_c"),
        }

        rest = escaped.get(1..).unwrap_or("");
    }

    buf.push_str(rest);
} // end fn compose_key

/// Write 32 lowercase hex characters of the leading 16 bytes of `SHA-256(bytes)`.
pub(crate) fn write_hex_hash(buf: &mut KeyBuf, bytes: &[u8]) {
    let digest = Sha256::digest(bytes);

    for byte in digest.iter().take(HASHED_BYTES) {
        let _ = write!(buf, "{byte:02x}");
    }
} // end fn write_hex_hash
