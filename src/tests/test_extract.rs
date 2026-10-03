//! Unit tests for `crate::extract`.

use std::net::SocketAddr;

use actix_web::test::TestRequest;

use crate::extract::{
    Composite, Global, Header, KeyBuf, KeyExtractor, PeerIp, RealIp, TrustedProxies,
};
use crate::{ConfigError, KeyError};

fn request_from(peer: &str) -> TestRequest {
    TestRequest::default().peer_addr(peer.parse::<SocketAddr>().unwrap())
}

fn extract(extractor: &impl KeyExtractor, req: TestRequest) -> Result<String, KeyError> {
    let req = req.to_srv_request();
    let mut buf = KeyBuf::new();
    extractor.extract(&req, &mut buf).map(|key| key.to_string())
}

#[test]
fn peer_ip_masks_and_canonicalizes() {
    let cases = [
        ("ipv4", "203.0.113.7:9000", "203.0.113.7"),
        (
            "ipv6 masked",
            "[2001:db8:1:2:3:4:5:6]:9000",
            "2001:db8:1:2::/64",
        ),
        ("mapped to ipv4", "[::ffff:203.0.113.7]:9000", "203.0.113.7"),
    ];

    for (case, peer, expected) in cases {
        assert_eq!(
            extract(&PeerIp::default(), request_from(peer)).as_deref(),
            Ok(expected),
            "{case}"
        );
    }

    assert_eq!(
        extract(
            &PeerIp::default().ipv6_prefix_len(56).unwrap(),
            request_from("[2001:db8:1:2::7]:9000"),
        )
        .as_deref(),
        Ok("2001:db8:1::/56")
    );
}

#[test]
fn real_ip_and_peer_ip_key_the_same_client_identically() {
    // Masking `64:ff9b::1:0:0:1` to /64 gives `64:ff9b::`, which has the NAT64 shape: a second
    // canonicalization after masking would turn it into 0.0.0.0.
    let client = "64:ff9b::1:0:0:1";
    let behind_proxy = request_from("10.0.0.1:9000").insert_header(("x-forwarded-for", client));
    let real_ip = RealIp::xff(TrustedProxies::new_or_panic(["10.0.0.0/8"]));

    assert_eq!(
        extract(&real_ip, behind_proxy).as_deref(),
        Ok("64:ff9b::/64")
    );
    assert_eq!(
        extract(
            &PeerIp::default(),
            request_from(&format!("[{client}]:9000"))
        )
        .as_deref(),
        Ok("64:ff9b::/64")
    );
}

#[test]
fn ipv4_key_text_matches_std_display() {
    for octets in [
        [0, 0, 0, 0],
        [9, 10, 99, 100],
        [203, 0, 113, 7],
        [255, 255, 255, 255],
    ] {
        let address = std::net::Ipv4Addr::from(octets);
        let mut buf = KeyBuf::new();
        crate::extract::write_ipv4(&mut buf, address);
        assert_eq!(buf.as_str(), address.to_string());
    }
}

#[test]
fn peer_ip_without_peer_errors() {
    assert_eq!(
        extract(&PeerIp::default(), TestRequest::default()),
        Err(KeyError::MissingPeerAddr)
    );
}

#[test]
fn prefix_len_outside_range_errors_or_panics() {
    for prefix_len in [0, 129] {
        assert!(
            matches!(
                PeerIp::default().ipv6_prefix_len(prefix_len),
                Err(ConfigError::InvalidIpv6PrefixLen(_))
            ),
            "{prefix_len}"
        );
        assert!(
            RealIp::xff(TrustedProxies::new_or_panic(["10.0.0.0/8"]))
                .ipv6_prefix_len(prefix_len)
                .is_err(),
            "{prefix_len}"
        );
        assert!(
            std::panic::catch_unwind(|| PeerIp::default().ipv6_prefix_len_or_panic(prefix_len))
                .is_err(),
            "{prefix_len}"
        );
    }

    assert!(PeerIp::default().ipv6_prefix_len(128).is_ok());
}

#[test]
fn header_uses_the_first_of_duplicate_values() {
    let header = Header::new("x-api-key").unwrap();
    let req = request_from("203.0.113.7:9000")
        .append_header(("x-api-key", "first"))
        .append_header(("x-api-key", "second"));
    assert_eq!(extract(&header, req).as_deref(), Ok("first"));
}

#[test]
fn real_ip_resolves_behind_trusted_proxies() {
    let real_ip = RealIp::xff(TrustedProxies::new_or_panic(["10.0.0.0/8"]));

    let spoofed =
        request_from("203.0.113.9:9000").insert_header(("x-forwarded-for", "198.51.100.1"));
    assert_eq!(
        extract(&real_ip, spoofed).as_deref(),
        Ok("203.0.113.9"),
        "untrusted peer keeps its own address"
    );

    let proxied =
        request_from("10.0.0.1:9000").insert_header(("x-forwarded-for", "203.0.113.7, 10.0.0.2"));
    assert_eq!(extract(&real_ip, proxied).as_deref(), Ok("203.0.113.7"));

    let malformed = request_from("10.0.0.1:9000").insert_header(("x-forwarded-for", "garbage"));
    assert_eq!(
        extract(&real_ip, malformed),
        Err(KeyError::MalformedForwardedChain)
    );

    assert_eq!(
        extract(&real_ip, TestRequest::default()),
        Err(KeyError::MissingPeerAddr)
    );
}

#[test]
fn real_ip_rejects_the_forwarded_name_and_invalid_names() {
    let trusted = TrustedProxies::new_or_panic(["10.0.0.0/8"]);
    assert!(RealIp::header("forwarded", trusted.clone()).is_err());
    assert!(RealIp::header("X-Forwarded-For", trusted.clone()).is_err());
}

#[test]
fn header_extracts_first_value_case_insensitively() {
    let header = Header::new("x-api-key").unwrap();
    let req = request_from("203.0.113.7:9000").insert_header(("X-Api-Key", "secret-1"));
    assert_eq!(extract(&header, req).as_deref(), Ok("secret-1"));
}

#[test]
fn header_missing_and_invalid_values_error() {
    let header = Header::new("x-api-key").unwrap();

    assert!(matches!(
        extract(&header, request_from("203.0.113.7:9000")),
        Err(KeyError::MissingHeader { .. })
    ));

    let non_ascii =
        request_from("203.0.113.7:9000").insert_header(("x-api-key", &b"caf\xc3\xa9"[..]));
    assert!(matches!(
        extract(&header, non_ascii),
        Err(KeyError::InvalidValue { .. })
    ));

    let long = request_from("203.0.113.7:9000").insert_header(("x-api-key", "k".repeat(1025)));
    assert!(matches!(
        extract(&header, long),
        Err(KeyError::InvalidValue { .. })
    ));

    assert!(Header::new("X-Api-Key").is_err());
    assert!(std::panic::catch_unwind(|| Header::new_or_panic("")).is_err());
}

#[test]
fn hashed_header_is_stable_hex_and_caps_input() {
    let hashed = Header::new("x-api-key").unwrap().hashed();

    let first = extract(
        &hashed,
        request_from("203.0.113.7:9000").insert_header(("x-api-key", "secret-1")),
    )
    .unwrap();
    let again = extract(
        &hashed,
        request_from("203.0.113.7:9000").insert_header(("x-api-key", "secret-1")),
    )
    .unwrap();
    let other = extract(
        &hashed,
        request_from("203.0.113.7:9000").insert_header(("x-api-key", "secret-2")),
    )
    .unwrap();

    assert_eq!(first, again);
    assert_ne!(first, other);
    assert_eq!(first.len(), 32);
    assert!(first.bytes().all(|byte| byte.is_ascii_hexdigit()));

    // Over-cap values hash their first KiB instead of erroring.
    let huge = extract(
        &hashed,
        request_from("203.0.113.7:9000").insert_header(("x-api-key", "k".repeat(4096))),
    )
    .unwrap();
    let same_prefix = extract(
        &hashed,
        request_from("203.0.113.7:9000").insert_header(("x-api-key", "k".repeat(2048))),
    )
    .unwrap();
    assert_eq!(huge, same_prefix);
}

#[test]
fn global_key_is_empty() {
    assert_eq!(
        extract(&Global, request_from("203.0.113.7:9000")).as_deref(),
        Ok("")
    );
}

#[test]
fn composite_keys_are_injective() {
    let composite = Composite::new(
        Header::new("x-tenant-id").unwrap(),
        Header::new("x-user-id").unwrap(),
    );

    let ab_c = extract(
        &composite,
        request_from("203.0.113.7:9000")
            .insert_header(("x-tenant-id", "ab"))
            .insert_header(("x-user-id", "c")),
    )
    .unwrap();
    let a_bc = extract(
        &composite,
        request_from("203.0.113.7:9000")
            .insert_header(("x-tenant-id", "a"))
            .insert_header(("x-user-id", "bc")),
    )
    .unwrap();

    assert_ne!(ab_c, a_bc);
}

#[test]
fn key_buf_spills_to_the_heap_past_inline_capacity() {
    let mut buf = KeyBuf::new();
    let quarter = "x".repeat(100);

    for _ in 0..3 {
        buf.push_str(&quarter);
    }

    assert_eq!(buf.as_str().len(), 300);
    assert!(buf.as_str().bytes().all(|byte| byte == b'x'));

    buf.push_str("y");
    assert!(buf.as_str().ends_with('y'));

    // A cleared buffer starts inline again, then spills again past capacity.
    buf.clear();
    assert_eq!(buf.as_str(), "");

    for _ in 0..3 {
        buf.push_str(&quarter);
    }

    assert_eq!(buf.as_str(), quarter.repeat(3));
}
