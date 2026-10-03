//! Client-IP resolution: trusted proxies, forwarding-header parsing, canonicalization, masking.
//!
//! This is the security core behind [`RealIp`](crate::extract::RealIp). Headers are read only
//! when the TCP peer is a trusted proxy, and they are scanned lazily from the right, so bytes a
//! client wrote into its own portion of a header are never examined.

use std::{
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    str::{self, FromStr},
};

use actix_web::http::header::{self, HeaderName};

use crate::ConfigError;

/// Maximum number of trusted proxy hops walked before client-IP resolution fails.
pub(crate) const MAX_FORWARDED_HOPS: usize = 32;

/// The forwarding-header source a [`RealIp`](crate::extract::RealIp) extractor reads.
#[derive(Clone, Debug)]
pub(crate) enum IpSource {
    Header {
        name: HeaderName,
        trusted_proxies: TrustedProxies,
    },
    Forwarded {
        trusted_proxies: TrustedProxies,
    },
}

impl IpSource {
    /// Resolve the canonical, unmasked client address. Masking happens once, when the key text
    /// is written, so trust checks here always compare full addresses.
    ///
    /// `fields` returns the values of the named header in order of appearance. It is called only
    /// when the peer is a trusted proxy, with a name parsed once at construction, so looking the
    /// header up allocates nothing. Returns `None` when a walked hop is malformed or more than
    /// [`MAX_FORWARDED_HOPS`] trusted hops are walked.
    pub(crate) fn resolve<'a, I>(
        &self,
        peer: IpAddr,
        fields: impl FnOnce(&HeaderName) -> I,
    ) -> Option<IpAddr>
    where
        I: DoubleEndedIterator<Item = &'a [u8]>,
    {
        let peer = canonical(peer);

        match self {
            Self::Header {
                name,
                trusted_proxies,
            } => {
                if !trusted_proxies.contains(peer) {
                    return Some(peer);
                }

                let hops = fields(name).rev().flat_map(list_hops_rev);
                walk(peer, trusted_proxies, hops)
            }
            Self::Forwarded { trusted_proxies } => {
                if !trusted_proxies.contains(peer) {
                    return Some(peer);
                }

                let hops = fields(&header::FORWARDED)
                    .rev()
                    .flat_map(ForwardedHopsRev::new);
                walk(peer, trusted_proxies, hops)
            }
        }
    } // end fn resolve
} // end impl

/// Networks whose addresses are trusted to write forwarding headers.
///
/// Entries are CIDR blocks such as `10.0.0.0/8` and `2001:db8::/32`, or single addresses such as
/// `203.0.113.4`. List only the proxies and load balancers in front of the application: a client
/// whose address falls inside a trusted network can choose the address it is keyed by. There is
/// deliberately no preset for private networks — inside Kubernetes, VPC, or intranet ranges the
/// clients sit inside those ranges and could spoof their address.
///
/// # Examples
///
/// ```
/// use actix_trypema::extract::TrustedProxies;
///
/// let trusted_proxies = TrustedProxies::new(["10.0.0.0/8", "fd00::/8", "203.0.113.4"])?;
/// let load_balancers = TrustedProxies::new_or_panic(["192.0.2.0/24"]);
///
/// // Invalid: trusting every address makes forwarding headers spoofable.
/// assert!(TrustedProxies::new(["0.0.0.0/0"]).is_err());
/// # let _ = (trusted_proxies, load_balancers);
/// # Ok::<(), actix_trypema::ConfigError>(())
/// ```
#[derive(Clone, Debug)]
pub struct TrustedProxies {
    networks: Box<[Cidr]>,
}

impl TrustedProxies {
    /// Create trusted proxies from CIDR blocks and single addresses.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidTrustedProxy`] when `entries` is empty or an entry is not
    /// an IP address or CIDR block, uses a prefix length of zero or longer than the address, has
    /// host bits set, or writes an IPv4 network in IPv4-mapped or NAT64 IPv6 form.
    pub fn new<I>(entries: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        let networks = entries
            .into_iter()
            .map(|entry| Cidr::parse(entry.as_ref()))
            .collect::<Result<Box<[Cidr]>, ConfigError>>()?;

        if networks.is_empty() {
            return Err(ConfigError::InvalidTrustedProxy(
                "trusted proxies must not be empty".to_string(),
            ));
        }

        Ok(Self { networks })
    } // end constructor

    /// Create trusted proxies from CIDR blocks and single addresses.
    ///
    /// # Panics
    ///
    /// Panics when [`TrustedProxies::new`] would return an error.
    pub fn new_or_panic<I>(entries: I) -> Self
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        Self::new(entries).unwrap()
    }

    pub(crate) fn contains(&self, address: IpAddr) -> bool {
        self.networks
            .iter()
            .any(|network| network.contains(address))
    }
} // end impl

#[derive(Clone, Copy)]
enum Cidr {
    V4 { network: u32, prefix_len: u32 },
    V6 { network: u128, prefix_len: u32 },
}

impl Cidr {
    fn parse(entry: &str) -> Result<Self, ConfigError> {
        let invalid =
            |reason: &str| ConfigError::InvalidTrustedProxy(format!("`{entry}` {reason}"));

        let (address, prefix_len) = match entry.split_once('/') {
            Some((address, prefix_len)) => (address, Some(prefix_len)),
            None => (entry, None),
        };

        let address =
            IpAddr::from_str(address).map_err(|_| invalid("is not an IP address or CIDR block"))?;

        if canonical(address) != address {
            return Err(invalid("must use the IPv4 form"));
        }

        let max_prefix_len = match address {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };

        let prefix_len = match prefix_len {
            Some(prefix_len) => parse_decimal::<u32>(prefix_len, 3)
                .filter(|prefix_len| (1..=max_prefix_len).contains(prefix_len))
                .ok_or_else(|| {
                    invalid(&format!(
                        "must use a prefix length between 1 and {max_prefix_len}"
                    ))
                })?,
            None => max_prefix_len,
        };

        let has_host_bits = match address {
            IpAddr::V4(address) => u32::from(address) & !ipv4_mask(prefix_len) != 0,
            IpAddr::V6(address) => u128::from(address) & !ipv6_mask(prefix_len) != 0,
        };

        if has_host_bits {
            return Err(invalid("has host bits set"));
        }

        Ok(match address {
            IpAddr::V4(address) => Self::V4 {
                network: u32::from(address),
                prefix_len,
            },
            IpAddr::V6(address) => Self::V6 {
                network: u128::from(address),
                prefix_len,
            },
        })
    } // end fn parse

    fn contains(self, address: IpAddr) -> bool {
        match (self, address) {
            (
                Self::V4 {
                    network,
                    prefix_len,
                },
                IpAddr::V4(address),
            ) => u32::from(address) & ipv4_mask(prefix_len) == network,
            (
                Self::V6 {
                    network,
                    prefix_len,
                },
                IpAddr::V6(address),
            ) => u128::from(address) & ipv6_mask(prefix_len) == network,
            _ => false,
        }
    } // end fn contains
} // end impl

impl fmt::Debug for Cidr {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::V4 {
                network,
                prefix_len,
            } => write!(formatter, "{}/{prefix_len}", Ipv4Addr::from(network)),
            Self::V6 {
                network,
                prefix_len,
            } => write!(formatter, "{}/{prefix_len}", Ipv6Addr::from(network)),
        }
    }
} // end impl

/// Yields the `for` value of each element of one `Forwarded` field, right to left.
///
/// Elements are delimited while scanning from the right, so bytes left of the elements already
/// yielded are never examined. A malformed element yields `None` and ends the iteration.
struct ForwardedHopsRev<'a> {
    remaining: Option<&'a [u8]>,
}

impl<'a> ForwardedHopsRev<'a> {
    fn new(field: &'a [u8]) -> Self {
        Self {
            remaining: Some(field),
        }
    }
} // end impl

impl<'a> Iterator for ForwardedHopsRev<'a> {
    type Item = Option<&'a [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let remaining = self.remaining?;

            let Some((rest, element)) = split_last_element(remaining) else {
                self.remaining = None;
                return Some(None);
            };

            self.remaining = rest;
            let element = trim_ows(element);

            if element.is_empty() {
                continue;
            }

            let hop = forwarded_for_value(element);

            if hop.is_none() {
                self.remaining = None;
            }

            return Some(hop);
        }
    } // end method next
} // end impl

/// Walk hops right to left and return the first untrusted hop.
///
/// Returns the leftmost hop when every hop is trusted, and the peer when there are no hops.
fn walk<'a>(
    peer: IpAddr,
    trusted_proxies: &TrustedProxies,
    hops: impl Iterator<Item = Option<&'a [u8]>>,
) -> Option<IpAddr> {
    let mut leftmost_trusted_hop = None;
    let mut trusted_hop_count = 0;

    for hop in hops {
        let hop = canonical(parse_hop(hop?)?);

        if !trusted_proxies.contains(hop) {
            return Some(hop);
        }

        trusted_hop_count += 1;

        if trusted_hop_count > MAX_FORWARDED_HOPS {
            return None;
        }

        leftmost_trusted_hop = Some(hop);
    }

    Some(leftmost_trusted_hop.unwrap_or(peer))
} // end fn walk

/// Split one comma-separated header field into trimmed, non-empty hops, right to left.
fn list_hops_rev(field: &[u8]) -> impl Iterator<Item = Option<&[u8]>> {
    field
        .rsplit(|byte| *byte == b',')
        .map(trim_ows)
        .filter(|hop| !hop.is_empty())
        .map(Some)
}

/// Split the right-most element off a `Forwarded` field.
///
/// Returns the bytes left of the delimiting comma (`None` once the field start is reached) and
/// the element itself. Returns `None` when the element contains a backslash, a byte that is
/// neither visible ASCII nor whitespace, or an unbalanced quote.
fn split_last_element(field: &[u8]) -> Option<(Option<&[u8]>, &[u8])> {
    let mut is_quoted = false;

    for (position, byte) in field.iter().enumerate().rev() {
        match *byte {
            b'\\' => return None,
            b'"' => is_quoted = !is_quoted,
            b',' if !is_quoted => {
                let (rest, delimited) = field.split_at_checked(position)?;
                return Some((Some(rest), delimited.get(1..)?));
            }
            byte if !is_field_byte(byte) => return None,
            _ => {}
        }
    }

    if is_quoted {
        return None;
    }

    Some((None, field))
} // end fn split_last_element

/// Return the value of the single `for` parameter of one `Forwarded` element.
///
/// Parameter names compare case-insensitively. Quoted values must be exactly `"…"`; unquoted
/// values are taken verbatim. Elements with no `for` parameter, or more than one, return `None`.
fn forwarded_for_value(element: &[u8]) -> Option<&[u8]> {
    let mut is_quoted = false;
    let pairs = element.split(|byte| {
        if *byte == b'"' {
            is_quoted = !is_quoted;
        }

        *byte == b';' && !is_quoted
    });

    let mut for_value = None;

    for pair in pairs {
        let Some((name, value)) = split_once_byte(trim_ows(pair), b'=') else {
            continue;
        };

        if !name.eq_ignore_ascii_case(b"for") {
            continue;
        }

        if for_value.is_some() {
            return None;
        }

        for_value = Some(unquote(value)?);
    }

    for_value
} // end fn forwarded_for_value

fn unquote(value: &[u8]) -> Option<&[u8]> {
    let value = match value.strip_prefix(b"\"") {
        Some(quoted) => quoted.strip_suffix(b"\"")?,
        None => value,
    };

    (!value.contains(&b'"')).then_some(value)
}

fn split_once_byte(bytes: &[u8], delimiter: u8) -> Option<(&[u8], &[u8])> {
    let position = bytes.iter().position(|byte| *byte == delimiter)?;
    let (before, delimited) = bytes.split_at_checked(position)?;
    Some((before, delimited.get(1..)?))
}

/// Parse one hop: IPv4, `IPv4:port`, bare IPv6, `[IPv6]`, or `[IPv6]:port`.
fn parse_hop(hop: &[u8]) -> Option<IpAddr> {
    let hop = str::from_utf8(hop).ok()?;

    if let Some(bracketed) = hop.strip_prefix('[') {
        let (address, suffix) = bracketed.split_once(']')?;

        if !suffix.is_empty() {
            parse_decimal::<u16>(suffix.strip_prefix(':')?, 5)?;
        }

        return Ipv6Addr::from_str(address).ok().map(IpAddr::V6);
    }

    if let Ok(address) = IpAddr::from_str(hop) {
        return Some(address);
    }

    let (address, port) = hop.rsplit_once(':')?;
    parse_decimal::<u16>(port, 5)?;
    Ipv4Addr::from_str(address).ok().map(IpAddr::V4)
} // end fn parse_hop

/// Parse an unsigned decimal of at most `max_digits` ASCII digits, without sign or whitespace.
fn parse_decimal<T: FromStr>(digits: &str, max_digits: usize) -> Option<T> {
    if digits.is_empty()
        || digits.len() > max_digits
        || !digits.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }

    digits.parse().ok()
}

/// Canonicalize IPv4-mapped and NAT64 (`64:ff9b::/96`) IPv6 addresses to IPv4.
pub(crate) fn canonical(address: IpAddr) -> IpAddr {
    match address.to_canonical() {
        IpAddr::V6(address) => match address.segments() {
            [0x64, 0xff9b, 0, 0, 0, 0, high, low] => {
                IpAddr::V4(Ipv4Addr::from((u32::from(high) << 16) | u32::from(low)))
            }
            _ => IpAddr::V6(address),
        },
        address => address,
    }
}

/// Mask IPv6 addresses to their client network; IPv4 addresses are unchanged.
pub(crate) fn mask(address: IpAddr, ipv6_prefix_len: u8) -> IpAddr {
    match address {
        IpAddr::V4(_) => address,
        IpAddr::V6(address) => IpAddr::V6(Ipv6Addr::from(
            u128::from(address) & ipv6_mask(u32::from(ipv6_prefix_len)),
        )),
    }
}

/// Callers guarantee `1..=32`: `Cidr::parse` validates it.
fn ipv4_mask(prefix_len: u32) -> u32 {
    debug_assert!((1..=32).contains(&prefix_len));
    u32::MAX << (32 - prefix_len)
}

/// Callers guarantee `1..=128`: `Cidr::parse` and the extractors' prefix setters validate it.
fn ipv6_mask(prefix_len: u32) -> u128 {
    debug_assert!((1..=128).contains(&prefix_len));
    u128::MAX << (128 - prefix_len)
}

fn trim_ows(mut bytes: &[u8]) -> &[u8] {
    while let [b' ' | b'\t', rest @ ..] = bytes {
        bytes = rest;
    }

    while let [rest @ .., b' ' | b'\t'] = bytes {
        bytes = rest;
    }

    bytes
}

/// Parse a lowercase RFC 9110 token header name, once, at configuration time.
pub(crate) fn parse_header_name(name: &str) -> Result<HeaderName, ConfigError> {
    let invalid = || {
        ConfigError::InvalidHeaderName(format!("`{name}` must be a non-empty lowercase HTTP token"))
    };

    if name.is_empty() || !name.bytes().all(is_header_name_byte) {
        return Err(invalid());
    }

    HeaderName::from_str(name).map_err(|_| invalid())
} // end fn parse_header_name

/// RFC 9110 `tchar`, restricted to lowercase letters.
fn is_header_name_byte(byte: u8) -> bool {
    byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"!#$%&'*+-.^_`|~".contains(&byte)
}

/// Visible ASCII, space, or horizontal tab.
fn is_field_byte(byte: u8) -> bool {
    byte.is_ascii_graphic() || byte == b' ' || byte == b'\t'
}
