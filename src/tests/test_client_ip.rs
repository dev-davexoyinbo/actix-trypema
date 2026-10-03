//! Unit tests for `crate::client_ip`.

use std::{cell::Cell, iter, net::IpAddr, panic};

use actix_web::http::header::HeaderName;

use crate::ConfigError;
use crate::client_ip::{IpSource, MAX_FORWARDED_HOPS, TrustedProxies, mask, parse_header_name};

const PROXY: &str = "10.0.0.1";
const CLIENT: &str = "203.0.113.7";
const CLIENT_V6: &str = "2001:db8:1:2::7";

fn ip(address: &str) -> IpAddr {
    address.parse().unwrap()
}

fn trusted(entries: &[&str]) -> TrustedProxies {
    TrustedProxies::new(entries).unwrap()
}

fn xff() -> IpSource {
    IpSource::Header {
        name: HeaderName::from_static("x-forwarded-for"),
        trusted_proxies: trusted(&["10.0.0.0/8"]),
    }
}

fn forwarded() -> IpSource {
    IpSource::Forwarded {
        trusted_proxies: trusted(&["10.0.0.0/8"]),
    }
}

/// Resolve `source` for `peer`, serving `fields` as the header values.
fn resolve(source: &IpSource, peer: &str, fields: &[&str]) -> Option<IpAddr> {
    source.resolve(ip(peer), |_| fields.iter().map(|field| field.as_bytes()))
}

fn resolve_bytes(source: &IpSource, peer: &str, fields: &[&[u8]]) -> Option<IpAddr> {
    source.resolve(ip(peer), |_| fields.iter().copied())
}

/// Resolve `source` for `peer` and report whether the header closure was invoked.
fn resolve_tracking_reads(source: &IpSource, peer: &str) -> (Option<IpAddr>, bool) {
    let was_read = Cell::new(false);
    let client = source.resolve(ip(peer), |_| {
        was_read.set(true);
        iter::empty::<&[u8]>()
    });

    (client, was_read.get())
}

/// Comma-join `count` distinct addresses inside the trusted `10.0.0.0/8` network.
fn trusted_hops(count: usize) -> Vec<String> {
    (1..=count)
        .map(|hop| format!("10.0.{}.{}", hop / 256, hop % 256))
        .collect()
}

#[test]
fn untrusted_peer_never_reads_forwarding_headers() {
    let cases = [
        ("x-forwarded-for, untrusted peer", xff(), "203.0.113.9"),
        ("forwarded, untrusted peer", forwarded(), "203.0.113.9"),
    ];

    for (case, source, peer) in cases {
        let (client, was_read) = resolve_tracking_reads(&source, peer);
        assert_eq!(client, Some(ip(peer)), "{case}");
        assert!(!was_read, "{case}");
    }
}

#[test]
fn right_most_untrusted_hop_is_the_client() {
    let cases: [(&str, &[&str]); 4] = [
        ("single field", &["198.51.100.1, 203.0.113.7"]),
        (
            "trusted hops skipped",
            &["198.51.100.1, 203.0.113.7, 10.0.0.3, 10.0.0.2"],
        ),
        ("client in first field", &["203.0.113.7", "10.0.0.2"]),
        (
            "client in last field",
            &["198.51.100.1", "203.0.113.7, 10.0.0.2"],
        ),
    ];

    for (case, fields) in cases {
        assert_eq!(resolve(&xff(), PROXY, fields), Some(ip(CLIENT)), "{case}");
    }
}

#[test]
fn all_trusted_hops_resolve_to_the_leftmost_hop() {
    let cases: [(&str, &[&str]); 2] = [
        ("single field", &["10.0.0.3, 10.0.0.2"]),
        (
            "leftmost in earlier field",
            &["10.0.0.3", "10.0.0.4, 10.0.0.2"],
        ),
    ];

    for (case, fields) in cases {
        assert_eq!(
            resolve(&xff(), PROXY, fields),
            Some(ip("10.0.0.3")),
            "{case}"
        );
    }
}

#[test]
fn trusted_peer_without_hops_resolves_to_the_peer() {
    let cases: [(&str, &[&str]); 4] = [
        ("header absent", &[]),
        ("empty field", &[""]),
        ("only empty elements", &[" , ,\t"]),
        ("several empty fields", &["", ""]),
    ];

    for (case, fields) in cases {
        assert_eq!(resolve(&xff(), PROXY, fields), Some(ip(PROXY)), "{case}");
    }
}

#[test]
fn malformed_walked_hop_is_unavailable() {
    let cases: [(&str, &[&str]); 4] = [
        ("only hop", &["garbage"]),
        ("right-most hop", &["203.0.113.7, garbage"]),
        ("after a trusted hop", &["garbage, 10.0.0.2"]),
        ("after a trusted field", &["garbage", "10.0.0.2"]),
    ];

    for (case, fields) in cases {
        assert_eq!(resolve(&xff(), PROXY, fields), None, "{case}");
    }
}

#[test]
fn hops_left_of_the_client_are_never_parsed() {
    let list_cases: [(&str, &[&[u8]]); 4] = [
        ("garbage element", &[b"garbage, 203.0.113.7"]),
        ("garbage field", &[b"garbage", b"203.0.113.7"]),
        ("non-UTF-8 element", &[b"\xff\xfe, 203.0.113.7"]),
        ("non-UTF-8 field", &[b"\xff", b"203.0.113.7"]),
    ];

    for (case, fields) in list_cases {
        assert_eq!(
            resolve_bytes(&xff(), PROXY, fields),
            Some(ip(CLIENT)),
            "x-forwarded-for, {case}"
        );
    }

    let forwarded_cases: [(&str, &[&[u8]]); 6] = [
        (
            "unbalanced quote",
            &[b"for=\"unterminated, for=203.0.113.7"],
        ),
        ("backslash", &[b"for=\"a\\\"b\", for=203.0.113.7"]),
        ("control byte", &[b"for=\x01, for=203.0.113.7"]),
        ("unknown node", &[b"for=unknown, for=203.0.113.7"]),
        ("element without for", &[b"proto=https, for=203.0.113.7"]),
        ("garbage field", &[b"for=\"", b"for=203.0.113.7"]),
    ];

    for (case, fields) in forwarded_cases {
        assert_eq!(
            resolve_bytes(&forwarded(), PROXY, fields),
            Some(ip(CLIENT)),
            "forwarded, {case}"
        );
    }
}

#[test]
fn walking_more_than_the_hop_cap_of_trusted_hops_is_unavailable() {
    assert_eq!(MAX_FORWARDED_HOPS, 32);

    let at_cap = trusted_hops(MAX_FORWARDED_HOPS).join(", ");
    let client_before_cap = format!("{CLIENT}, {at_cap}");
    let over_cap = trusted_hops(MAX_FORWARDED_HOPS + 1).join(", ");
    let over_cap_hops = trusted_hops(MAX_FORWARDED_HOPS + 1);
    let (first_field_hops, second_field_hops) = over_cap_hops.split_at(20);
    let first_field = first_field_hops.join(", ");
    let second_field = second_field_hops.join(", ");
    let at_cap_with_empty_elements = trusted_hops(MAX_FORWARDED_HOPS).join(", , ");

    let cases: [(&str, Vec<&str>, Option<IpAddr>); 5] = [
        (
            "trusted hops at the cap",
            vec![at_cap.as_str()],
            Some(ip("10.0.0.1")),
        ),
        (
            "client after the cap of trusted hops",
            vec![client_before_cap.as_str()],
            Some(ip(CLIENT)),
        ),
        ("trusted hops over the cap", vec![over_cap.as_str()], None),
        (
            "trusted hops over the cap across fields",
            vec![first_field.as_str(), second_field.as_str()],
            None,
        ),
        (
            "empty elements do not count",
            vec![at_cap_with_empty_elements.as_str()],
            Some(ip("10.0.0.1")),
        ),
    ];

    for (case, fields, expected) in cases {
        assert_eq!(resolve(&xff(), PROXY, &fields), expected, "{case}");
    }
}

#[test]
fn accepted_hop_forms_resolve() {
    let cases = [
        ("ipv4 with port", "203.0.113.7:8443", CLIENT),
        ("bare ipv6", "2001:db8:1:2::7", CLIENT_V6),
        ("bracketed ipv6", "[2001:db8:1:2::7]", CLIENT_V6),
        (
            "bracketed ipv6 with port",
            "[2001:db8:1:2::7]:8443",
            CLIENT_V6,
        ),
        (
            "bracketed mapped trusted hop with port",
            "203.0.113.7, [::ffff:10.0.0.2]:80",
            CLIENT,
        ),
    ];

    for (case, field, expected) in cases {
        assert_eq!(
            resolve(&xff(), PROXY, &[field]),
            Some(ip(expected)),
            "{case}"
        );
    }
}

#[test]
fn rejected_hop_forms_are_unavailable() {
    let cases: [(&str, &[u8]); 12] = [
        ("zone id", b"fe80::1%eth0"),
        ("bracketed zone id", b"[fe80::1%25eth0]"),
        ("ipv4 shorthand", b"1.2.3"),
        ("octal-looking octet", b"01.2.3.4"),
        ("bracketed ipv4", b"[203.0.113.7]"),
        ("empty port", b"203.0.113.7:"),
        ("signed port", b"203.0.113.7:+80"),
        ("port out of range", b"203.0.113.7:65536"),
        ("bracketed ipv6 without port separator", b"[::1]80"),
        ("unterminated bracket", b"[::1"),
        ("unknown node", b"unknown"),
        ("non-UTF-8", b"\xff"),
    ];

    for (case, field) in cases {
        assert_eq!(resolve_bytes(&xff(), PROXY, &[field]), None, "{case}");
    }
}

#[test]
fn ipv4_mapped_hops_and_peers_match_ipv4_ranges() {
    assert_eq!(
        resolve(&xff(), PROXY, &["203.0.113.7, ::ffff:10.0.0.2"]),
        Some(ip(CLIENT)),
        "mapped hop"
    );
    assert_eq!(
        resolve(&xff(), "::ffff:10.0.0.1", &[CLIENT]),
        Some(ip(CLIENT)),
        "mapped peer"
    );
}

#[test]
fn ipv4_compatible_and_loopback_ipv6_stay_ipv6() {
    assert_eq!(
        resolve(&xff(), PROXY, &["203.0.113.7, ::10.0.0.2"]),
        Some(ip("::10.0.0.2")),
        "compatible hop is not trusted by an IPv4 range"
    );

    let source = IpSource::Header {
        name: HeaderName::from_static("x-forwarded-for"),
        trusted_proxies: trusted(&["0.0.0.1"]),
    };
    let (client, was_read) = resolve_tracking_reads(&source, "::1");
    assert_eq!(client, Some(ip("::1")), "loopback peer");
    assert!(!was_read, "loopback peer is not trusted as 0.0.0.1");
}

#[test]
fn nat64_addresses_resolve_to_their_embedded_ipv4() {
    assert_eq!(
        resolve(&xff(), "64:ff9b::a00:1", &[CLIENT]),
        Some(ip(CLIENT)),
        "NAT64 peer is trusted as 10.0.0.1"
    );
    assert_eq!(
        resolve(&xff(), PROXY, &["64:ff9b::198.51.100.7"]),
        Some(ip("198.51.100.7")),
        "NAT64 hop"
    );
}

#[test]
fn address_families_never_trust_each_other() {
    let (_, was_read) = resolve_tracking_reads(&xff(), "2001:db8::1");
    assert!(!was_read, "IPv4 range does not trust an IPv6 peer");

    let ipv6_trusted = IpSource::Header {
        name: HeaderName::from_static("x-forwarded-for"),
        trusted_proxies: trusted(&["2001:db8::/32"]),
    };
    let (_, was_read) = resolve_tracking_reads(&ipv6_trusted, PROXY);
    assert!(!was_read, "IPv6 range does not trust an IPv4 peer");

    assert_eq!(
        resolve(&ipv6_trusted, "2001:db8::1", &["203.0.113.7, 10.0.0.2"]),
        Some(ip("10.0.0.2")),
        "IPv6 range does not trust an IPv4 hop"
    );
}

#[test]
fn resolution_returns_full_addresses_and_mask_keeps_the_prefix() {
    let peer = "2001:db8:1:2:3:4:5:6";

    // Resolution never masks: the key writer masks once, after canonicalization.
    assert_eq!(resolve(&xff(), peer, &[]), Some(ip(peer)), "untrusted peer");
    assert_eq!(
        resolve(&xff(), PROXY, &[CLIENT_V6]),
        Some(ip(CLIENT_V6)),
        "hop"
    );

    assert_eq!(mask(ip(peer), 64), ip("2001:db8:1:2::"));
    assert_eq!(mask(ip(peer), 56), ip("2001:db8:1::"));
    assert_eq!(mask(ip(CLIENT), 64), ip(CLIENT), "IPv4 is never masked");
}

#[test]
fn trust_checks_use_unmasked_addresses() {
    let source = IpSource::Header {
        name: HeaderName::from_static("x-forwarded-for"),
        trusted_proxies: trusted(&["2001:db8::1"]),
    };

    let (_, was_read) = resolve_tracking_reads(&source, "2001:db8::2");
    assert!(!was_read, "same /64 as the trusted /128");

    let (_, was_read) = resolve_tracking_reads(&source, "2001:db8::1");
    assert!(was_read, "the trusted /128 itself");
}

#[test]
fn forwarded_parameters_resolve_the_client() {
    let cases: [(&str, &[&str], &str); 9] = [
        (
            "quoted delimiters in another parameter",
            &["for=203.0.113.7;by=\"a,b;c\""],
            CLIENT,
        ),
        (
            "quoted delimiters before for",
            &["by=\"x;y,z\";for=203.0.113.7"],
            CLIENT,
        ),
        ("capitalized name", &["For=203.0.113.7"], CLIENT),
        ("uppercase name", &["FOR=203.0.113.7"], CLIENT),
        (
            "whitespace and empty pairs",
            &[" proto=https ; for=203.0.113.7 ;; host=example.com "],
            CLIENT,
        ),
        ("quoted ipv4", &["for=\"203.0.113.7\""], CLIENT),
        (
            "quoted bracketed ipv6 with port",
            &["for=\"[2001:db8:1:2::7]:4711\""],
            CLIENT_V6,
        ),
        (
            "unquoted bracketed ipv6",
            &["for=[2001:db8:1:2::7]"],
            CLIENT_V6,
        ),
        (
            "trusted elements across fields",
            &["for=203.0.113.7 , for=10.0.0.3", "for=10.0.0.2"],
            CLIENT,
        ),
    ];

    for (case, fields, expected) in cases {
        assert_eq!(
            resolve(&forwarded(), PROXY, fields),
            Some(ip(expected)),
            "{case}"
        );
    }
}

#[test]
fn malformed_walked_forwarded_element_is_unavailable() {
    let cases: [(&str, &[u8]); 10] = [
        ("unknown node", b"for=unknown"),
        ("obfuscated node", b"for=_hidden"),
        ("escaped quote", b"for=\"203.0.113.7\\\"\""),
        ("duplicate for", b"for=203.0.113.7;FOR=198.51.100.1"),
        ("missing for", b"proto=https"),
        ("unterminated quote", b"for=\"203.0.113.7"),
        (
            "unterminated quote in another parameter",
            b"for=203.0.113.7;by=\"",
        ),
        ("stray quote", b"for=203.0.113.7\""),
        ("control byte", b"for=203.0.113.7\x01"),
        (
            "walked after a trusted element",
            b"for=unknown, for=10.0.0.2",
        ),
    ];

    for (case, field) in cases {
        assert_eq!(resolve_bytes(&forwarded(), PROXY, &[field]), None, "{case}");
    }
}

#[test]
fn header_closure_receives_the_configured_name() {
    let cases = [
        (
            IpSource::Header {
                name: HeaderName::from_static("cf-connecting-ip"),
                trusted_proxies: trusted(&["10.0.0.0/8"]),
            },
            "cf-connecting-ip",
        ),
        (forwarded(), "forwarded"),
    ];

    for (source, expected_name) in cases {
        let mut requested_names = Vec::new();
        source.resolve(ip(PROXY), |name| {
            requested_names.push(name.to_string());
            iter::empty::<&[u8]>()
        });

        assert_eq!(requested_names, [expected_name]);
    }
}

#[test]
fn trusted_proxies_reject_invalid_entries() {
    let cases = [
        ("zero IPv4 prefix", "0.0.0.0/0"),
        ("zero IPv6 prefix", "::/0"),
        ("IPv4 prefix too long", "10.0.0.0/33"),
        ("IPv6 prefix too long", "2001:db8::/129"),
        ("host bits set", "10.0.0.1/8"),
        ("empty prefix", "10.0.0.0/"),
        ("signed prefix", "10.0.0.0/+8"),
        ("surrounding whitespace", " 10.0.0.0/8"),
        ("IPv4-mapped form", "::ffff:10.0.0.0/104"),
        ("NAT64 form", "64:ff9b::/96"),
        ("zone id", "fe80::1%eth0"),
        ("brackets", "[::1]"),
        ("garbage", "garbage"),
    ];

    for (case, entry) in cases {
        assert!(
            matches!(
                TrustedProxies::new([entry]),
                Err(ConfigError::InvalidTrustedProxy(_))
            ),
            "{case}"
        );
    }

    assert!(matches!(
        TrustedProxies::new(Vec::<&str>::new()),
        Err(ConfigError::InvalidTrustedProxy(_))
    ));
    assert!(panic::catch_unwind(|| TrustedProxies::new_or_panic(["garbage"])).is_err());
}

#[test]
fn trusted_proxies_accept_single_addresses_as_host_routes() {
    let source = IpSource::Header {
        name: HeaderName::from_static("x-forwarded-for"),
        trusted_proxies: trusted(&["203.0.113.4"]),
    };

    let (_, was_read) = resolve_tracking_reads(&source, "203.0.113.4");
    assert!(was_read, "the listed address");

    let (_, was_read) = resolve_tracking_reads(&source, "203.0.113.5");
    assert!(!was_read, "a neighbouring address");

    assert_eq!(
        format!(
            "{:?}",
            trusted(&["10.0.0.0/8", "2001:db8::/32", "203.0.113.4"])
        ),
        "TrustedProxies { networks: [10.0.0.0/8, 2001:db8::/32, 203.0.113.4/32] }"
    );
}

#[test]
fn header_names_validate_as_lowercase_tokens() {
    let rejected = [
        ("uppercase", "X-Forwarded-For"),
        ("empty", ""),
        ("space", "x forwarded"),
        ("pseudo-header", ":authority"),
        ("non-ASCII", "x-fórwarded"),
    ];

    for (case, name) in rejected {
        assert!(
            matches!(
                parse_header_name(name),
                Err(ConfigError::InvalidHeaderName(_))
            ),
            "{case}"
        );
    }

    for name in ["x-forwarded-for", "x-real-ip", "cf-connecting-ip"] {
        assert!(parse_header_name(name).is_ok(), "{name}");
    }
}
