//! Key extraction: who a request is rate-limited as.
//!
//! Built-in extractors cover the common identities — [`PeerIp`], [`RealIp`] behind trusted
//! proxies, an authenticated [`Header`] value, one [`Global`] bucket, and [`Composite`] pairs.
//! Implement [`KeyExtractor`] (or pass a closure to
//! [`extractor_fn`](crate::TrypemaLimiterBuilder::extractor_fn)) for anything else.

use std::{
    fmt,
    net::{IpAddr, Ipv4Addr},
};

use actix_web::{
    dev::ServiceRequest,
    http::header::{self, HeaderName},
};

use crate::{
    ConfigError, KeyError,
    client_ip::{IpSource, canonical, mask, parse_header_name},
    key::write_hex_hash,
};

pub use crate::client_ip::TrustedProxies;

pub(crate) const KEY_BUF_INLINE_CAPACITY: usize = 256;

/// Maximum header-value length [`Header`] accepts, in bytes.
pub const MAX_HEADER_VALUE_LEN: usize = 1024;

const DEFAULT_IPV6_PREFIX_LEN: u8 = 64;

/// A key buffer extractors write into, so short keys never touch the heap.
///
/// Holds 256 bytes inline and spills to a heap `String` only past that.
#[derive(Debug)]
pub struct KeyBuf {
    inline: [u8; KEY_BUF_INLINE_CAPACITY],
    inline_len: usize,
    spill: String,
}

impl Default for KeyBuf {
    fn default() -> Self {
        Self::new()
    }
} // end impl

impl KeyBuf {
    /// Create an empty buffer.
    pub fn new() -> Self {
        Self {
            inline: [0; KEY_BUF_INLINE_CAPACITY],
            inline_len: 0,
            spill: String::new(),
        }
    }

    /// Append a string.
    pub fn push_str(&mut self, value: &str) {
        if !self.spill.is_empty() {
            self.spill.push_str(value);
            return;
        }

        if self.inline_len + value.len() <= KEY_BUF_INLINE_CAPACITY {
            self.inline[self.inline_len..self.inline_len + value.len()]
                .copy_from_slice(value.as_bytes());
            self.inline_len += value.len();
            return;
        }

        // The spill is empty here, but keeps any capacity from before a `clear()`.
        self.spill.reserve(self.inline_len + value.len());
        self.spill
            .push_str(inline_text(&self.inline[..self.inline_len]));
        self.spill.push_str(value);
    } // end method push_str

    /// View the accumulated key.
    pub fn as_str(&self) -> &str {
        if self.spill.is_empty() {
            inline_text(&self.inline[..self.inline_len])
        } else {
            &self.spill
        }
    }

    /// Empty the buffer for reuse, keeping any spilled heap capacity.
    pub(crate) fn clear(&mut self) {
        self.inline_len = 0;
        self.spill.clear();
    }
} // end impl

fn inline_text(bytes: &[u8]) -> &str {
    // Only whole `&str` values are copied in, so the bytes are always valid UTF-8.
    str::from_utf8(bytes).expect("KeyBuf holds whole UTF-8 pushes")
}

impl fmt::Write for KeyBuf {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.push_str(value);
        Ok(())
    }
} // end impl

/// Extracts the rate-limiting key from a request.
///
/// Write the key into `buf` (or borrow it from the request) and return it. Errors are handled
/// according to the configured [`KeyErrorPolicy`](crate::KeyErrorPolicy).
pub trait KeyExtractor: Send + Sync + 'static {
    /// Extract the key for `req`.
    fn extract<'a>(
        &self,
        req: &'a ServiceRequest,
        buf: &'a mut KeyBuf,
    ) -> Result<&'a str, KeyError>;
}

/// Keys requests by the TCP peer address, ignoring every header.
///
/// Use this whenever clients connect directly. IPv6 peers are grouped by their `/64` network by
/// default; IPv4 is used as is.
///
/// # Examples
///
/// ```
/// use actix_trypema::extract::PeerIp;
///
/// let per_ip = PeerIp::default();
/// let coarse = PeerIp::default().ipv6_prefix_len(56)?;
///
/// // Invalid: a prefix must be 1..=128 bits.
/// assert!(PeerIp::default().ipv6_prefix_len(0).is_err());
/// # let _ = (per_ip, coarse);
/// # Ok::<(), actix_trypema::ConfigError>(())
/// ```
#[derive(Clone, Debug)]
pub struct PeerIp {
    ipv6_prefix_len: u8,
}

impl Default for PeerIp {
    fn default() -> Self {
        Self {
            ipv6_prefix_len: DEFAULT_IPV6_PREFIX_LEN,
        }
    }
} // end impl

impl PeerIp {
    /// Group IPv6 peers by this prefix length instead of the default `/64`.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidIpv6PrefixLen`] when `prefix_len` is outside `1..=128`.
    pub fn ipv6_prefix_len(mut self, prefix_len: u8) -> Result<Self, ConfigError> {
        self.ipv6_prefix_len = checked_prefix_len(prefix_len)?;
        Ok(self)
    }

    /// Group IPv6 peers by this prefix length instead of the default `/64`.
    ///
    /// # Panics
    ///
    /// Panics when [`PeerIp::ipv6_prefix_len`] would return an error.
    pub fn ipv6_prefix_len_or_panic(self, prefix_len: u8) -> Self {
        self.ipv6_prefix_len(prefix_len).unwrap()
    }
} // end impl

impl KeyExtractor for PeerIp {
    fn extract<'a>(
        &self,
        req: &'a ServiceRequest,
        buf: &'a mut KeyBuf,
    ) -> Result<&'a str, KeyError> {
        let peer = req.peer_addr().ok_or(KeyError::MissingPeerAddr)?.ip();
        write_client_ip(buf, peer, self.ipv6_prefix_len);
        Ok(buf.as_str())
    }
} // end impl

/// Keys requests by the client address behind trusted proxies.
///
/// The forwarding header is read only when the TCP peer is one of the
/// [`TrustedProxies`]; otherwise the peer itself is the client. The header is walked from the
/// right, skipping trusted hops, so hops the client wrote itself are never even parsed. A
/// malformed trusted chain is a [`KeyError`], handled by the configured
/// [`KeyErrorPolicy`](crate::KeyErrorPolicy).
///
/// IPv4-mapped and NAT64 addresses resolve to their embedded IPv4 address, and IPv6 clients are
/// grouped by their `/64` network by default.
///
/// # Examples
///
/// ```
/// use actix_trypema::extract::{RealIp, TrustedProxies};
///
/// let trusted = TrustedProxies::new(["10.0.0.0/8"])?;
///
/// // Load balancers append to X-Forwarded-For:
/// let xff = RealIp::xff(trusted.clone());
///
/// // Proxies write RFC 7239 `Forwarded`:
/// let forwarded = RealIp::forwarded(trusted.clone());
///
/// // Any other comma-list header:
/// let cloudflare = RealIp::header("cf-connecting-ip", trusted)?;
/// # let _ = (xff, forwarded, cloudflare);
/// # Ok::<(), actix_trypema::ConfigError>(())
/// ```
#[derive(Clone, Debug)]
pub struct RealIp {
    source: IpSource,
    ipv6_prefix_len: u8,
}

impl RealIp {
    /// Read the client from `x-forwarded-for` written by trusted proxies.
    pub fn xff(trusted_proxies: TrustedProxies) -> Self {
        Self::from_source(IpSource::Header {
            name: HeaderName::from_static("x-forwarded-for"),
            trusted_proxies,
        })
    }

    /// Read the client from a comma-list forwarding header written by trusted proxies.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidHeaderName`] when `name` is not a non-empty lowercase HTTP
    /// token, or is `forwarded`, which [`RealIp::forwarded`] handles.
    pub fn header(name: &str, trusted_proxies: TrustedProxies) -> Result<Self, ConfigError> {
        let name = parse_header_name(name)?;

        if name == header::FORWARDED {
            return Err(ConfigError::InvalidHeaderName(
                "use RealIp::forwarded for the forwarded header".to_string(),
            ));
        }

        Ok(Self::from_source(IpSource::Header {
            name,
            trusted_proxies,
        }))
    } // end method header

    /// Read the client from the `for` parameters of RFC 7239 `Forwarded`.
    pub fn forwarded(trusted_proxies: TrustedProxies) -> Self {
        Self::from_source(IpSource::Forwarded { trusted_proxies })
    }

    fn from_source(source: IpSource) -> Self {
        Self {
            source,
            ipv6_prefix_len: DEFAULT_IPV6_PREFIX_LEN,
        }
    }

    /// Group IPv6 clients by this prefix length instead of the default `/64`.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidIpv6PrefixLen`] when `prefix_len` is outside `1..=128`.
    pub fn ipv6_prefix_len(mut self, prefix_len: u8) -> Result<Self, ConfigError> {
        self.ipv6_prefix_len = checked_prefix_len(prefix_len)?;
        Ok(self)
    }
} // end impl

impl KeyExtractor for RealIp {
    fn extract<'a>(
        &self,
        req: &'a ServiceRequest,
        buf: &'a mut KeyBuf,
    ) -> Result<&'a str, KeyError> {
        let peer = req.peer_addr().ok_or(KeyError::MissingPeerAddr)?.ip();
        let client = self
            .source
            .resolve(peer, |name| {
                req.headers().get_all(name).map(|value| value.as_bytes())
            })
            .ok_or(KeyError::MalformedForwardedChain)?;
        write_client_ip(buf, client, self.ipv6_prefix_len);
        Ok(buf.as_str())
    } // end method extract
} // end impl

/// Keys requests by a request header value, such as an API key.
///
/// The header value is attacker chosen unless something upstream authenticated it: every new
/// value mints a fresh bucket, so key only on values an earlier middleware has validated. An
/// absent header is a [`KeyError`], handled by the configured
/// [`KeyErrorPolicy`](crate::KeyErrorPolicy); it never falls back to a shared bucket silently.
///
/// # Examples
///
/// ```
/// use actix_trypema::extract::Header;
///
/// // hashed(): credentials never appear in limiter keys or logs.
/// let api_key = Header::new("x-api-key")?.hashed();
/// # let _ = api_key;
/// # Ok::<(), actix_trypema::ConfigError>(())
/// ```
#[derive(Clone, Debug)]
pub struct Header {
    name: HeaderName,
    hashed: bool,
}

impl Header {
    /// Key on the first value of the named header.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidHeaderName`] when `name` is not a non-empty lowercase HTTP
    /// token.
    pub fn new(name: &str) -> Result<Self, ConfigError> {
        Ok(Self {
            name: parse_header_name(name)?,
            hashed: false,
        })
    } // end constructor

    /// Key on the first value of the named header.
    ///
    /// # Panics
    ///
    /// Panics when [`Header::new`] would return an error.
    pub fn new_or_panic(name: &str) -> Self {
        Self::new(name).unwrap()
    }

    /// Key on a hash of the value instead of the raw value.
    ///
    /// Use this for credentials: the raw value then never appears in limiter state or logs.
    /// Values are capped at [`MAX_HEADER_VALUE_LEN`] bytes before hashing.
    pub fn hashed(mut self) -> Self {
        self.hashed = true;
        self
    }
} // end impl

impl KeyExtractor for Header {
    fn extract<'a>(
        &self,
        req: &'a ServiceRequest,
        buf: &'a mut KeyBuf,
    ) -> Result<&'a str, KeyError> {
        let value = req
            .headers()
            .get(&self.name)
            .ok_or_else(|| KeyError::MissingHeader {
                name: self.name.as_str().into(),
            })?;

        if self.hashed {
            let bytes = value.as_bytes();
            let capped = bytes.get(..MAX_HEADER_VALUE_LEN).unwrap_or(bytes);
            write_hex_hash(buf, capped);
            return Ok(buf.as_str());
        }

        let value = value.to_str().map_err(|_| KeyError::InvalidValue {
            reason: "header value is not visible ASCII",
        })?;

        if value.len() > MAX_HEADER_VALUE_LEN {
            return Err(KeyError::InvalidValue {
                reason: "header value exceeds the length cap",
            });
        }

        Ok(value)
    } // end method extract
} // end impl

/// Keys every request into one namespace-wide bucket.
#[derive(Clone, Copy, Debug, Default)]
pub struct Global;

impl KeyExtractor for Global {
    fn extract<'a>(
        &self,
        _req: &'a ServiceRequest,
        buf: &'a mut KeyBuf,
    ) -> Result<&'a str, KeyError> {
        Ok(buf.as_str())
    }
} // end impl

/// Keys requests by two extractors joined injectively, for example tenant plus client IP.
///
/// # Examples
///
/// ```
/// use actix_trypema::extract::{Composite, Header, PeerIp};
///
/// let per_tenant_ip = Composite::new(Header::new("x-tenant-id")?, PeerIp::default());
/// # let _ = per_tenant_ip;
/// # Ok::<(), actix_trypema::ConfigError>(())
/// ```
#[derive(Clone, Debug)]
pub struct Composite<A, B> {
    first: A,
    second: B,
}

impl<A: KeyExtractor, B: KeyExtractor> Composite<A, B> {
    /// Combine two extractors.
    pub fn new(first: A, second: B) -> Self {
        Self { first, second }
    }
} // end impl

impl<A: KeyExtractor, B: KeyExtractor> KeyExtractor for Composite<A, B> {
    fn extract<'a>(
        &self,
        req: &'a ServiceRequest,
        buf: &'a mut KeyBuf,
    ) -> Result<&'a str, KeyError> {
        // Extractors may borrow from the request or write into their buffer, so each part goes
        // through one scratch buffer, reused for the second part.
        let mut scratch = KeyBuf::new();
        let first = self.first.extract(req, &mut scratch)?;

        // The length prefix keeps the joint key injective: ("a", "bc") and ("ab", "c") differ.
        let _ = fmt::Write::write_fmt(buf, format_args!("{}.", first.len()));
        buf.push_str(first);

        scratch.clear();
        let second = self.second.extract(req, &mut scratch)?;
        buf.push_str(second);

        Ok(buf.as_str())
    } // end method extract
} // end impl

/// Adapter for closure extractors registered through
/// [`extractor_fn`](crate::TrypemaLimiterBuilder::extractor_fn).
pub(crate) struct FnExtractor<F>(pub(crate) F);

impl<F> KeyExtractor for FnExtractor<F>
where
    F: Fn(&ServiceRequest) -> Result<String, KeyError> + Send + Sync + 'static,
{
    fn extract<'a>(
        &self,
        req: &'a ServiceRequest,
        buf: &'a mut KeyBuf,
    ) -> Result<&'a str, KeyError> {
        let key = (self.0)(req)?;
        buf.push_str(&key);
        Ok(buf.as_str())
    }
} // end impl

/// Write the canonical, masked key text of a client address: dotted IPv4, or
/// `{network}/{prefix}` for IPv6.
pub(crate) fn write_client_ip(buf: &mut KeyBuf, address: IpAddr, ipv6_prefix_len: u8) {
    match mask(canonical(address), ipv6_prefix_len) {
        IpAddr::V4(address) => write_ipv4(buf, address),
        IpAddr::V6(network) => {
            let _ = fmt::Write::write_fmt(buf, format_args!("{network}/{ipv6_prefix_len}"));
        }
    }
}

/// Dotted-decimal IPv4 without the `fmt` machinery, which costs more than the rest of key
/// extraction combined on this per-request path. Output matches `Ipv4Addr`'s `Display`.
pub(crate) fn write_ipv4(buf: &mut KeyBuf, address: Ipv4Addr) {
    let mut text = [0u8; 15];
    let mut len = 0;

    for (index, octet) in address.octets().into_iter().enumerate() {
        if index > 0 {
            text[len] = b'.';
            len += 1;
        }

        if octet >= 100 {
            text[len] = b'0' + octet / 100;
            len += 1;
        }

        if octet >= 10 {
            text[len] = b'0' + octet / 10 % 10;
            len += 1;
        }

        text[len] = b'0' + octet % 10;
        len += 1;
    }

    // Only ASCII digits and dots were written, and at most 15 of them.
    buf.push_str(str::from_utf8(&text[..len]).expect("dotted decimal is ASCII"));
} // end fn write_ipv4

fn checked_prefix_len(prefix_len: u8) -> Result<u8, ConfigError> {
    if !(1..=128).contains(&prefix_len) {
        return Err(ConfigError::InvalidIpv6PrefixLen(prefix_len));
    }

    Ok(prefix_len)
}
