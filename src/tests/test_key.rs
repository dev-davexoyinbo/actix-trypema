//! Unit tests for `crate::key`.

use proptest::prelude::*;

use crate::key::{derived_key, validate_namespace};

fn assert_valid_derived_key(derived: &str) {
    assert!(!derived.is_empty());
    assert!(derived.len() <= 255, "{derived}");
    assert!(!derived.contains(':'), "{derived}");
}

#[test]
fn namespaces_validate_charset_and_length() {
    for namespace in ["ip", "api-key-2", "a", &"n".repeat(32)] {
        assert!(validate_namespace(namespace).is_ok(), "{namespace}");
    }

    for namespace in ["", "IP", "a_b", "a:b", "a b", &"n".repeat(33), "café"] {
        assert!(validate_namespace(namespace).is_err(), "{namespace}");
    }
}

#[test]
fn escaping_is_injective_for_colliding_raw_keys() {
    let pairs = [("a:b", "a_b"), ("a_cb", "a:b"), ("a__b", "a_b"), ("_", ":")];

    for (left, right) in pairs {
        assert_ne!(
            derived_key("ns", left),
            derived_key("ns", right),
            "{left} vs {right}"
        );
    }

    assert_eq!(derived_key("ns", "a:b"), "ns_r_a_cb");
    assert_eq!(derived_key("ns", "a_b"), "ns_r_a__b");
    assert_eq!(derived_key("ns", ""), "ns_r_");
}

#[test]
fn derived_key_panics_on_an_invalid_namespace() {
    for namespace in ["", "a_b", "A"] {
        assert!(
            std::panic::catch_unwind(|| derived_key(namespace, "k")).is_err(),
            "{namespace}"
        );
    }
}

#[test]
fn long_keys_use_the_hash_form() {
    let long = "k".repeat(300);
    let derived = derived_key("ns", &long);

    // The hash is part of the stable encoding: the leading 16 bytes of SHA-256, computed
    // independently (Python `hashlib.sha256(b"k" * 300).hexdigest()[:32]`).
    assert_eq!(derived, "ns_h_17b16d8ef494060fefa36a6a41567b8c");
    assert_valid_derived_key(&derived);

    // The boundary: the longest raw form still fits, one byte more hashes.
    let just_fits = "k".repeat(255 - "ns_r_".len());
    assert!(derived_key("ns", &just_fits).starts_with("ns_r_"));

    let one_over = "k".repeat(256 - "ns_r_".len());
    assert!(derived_key("ns", &one_over).starts_with("ns_h_"));
}

proptest! {
    #[test]
    fn derived_keys_are_always_valid(namespace in "[a-z0-9-]{1,32}", key in ".{0,400}") {
        let derived = derived_key(&namespace, &key);
        assert_valid_derived_key(&derived);

        #[cfg(feature = "redis")]
        trypema::redis::RedisKey::try_from(derived).unwrap();
    }

    #[test]
    fn derived_keys_are_injective_across_namespaces(
        first_namespace in "[a-z0-9-]{1,32}",
        first_key in ".{0,300}",
        second_namespace in "[a-z0-9-]{1,32}",
        second_key in ".{0,300}",
    ) {
        prop_assume!((&first_namespace, &first_key) != (&second_namespace, &second_key));
        prop_assert_ne!(
            derived_key(&first_namespace, &first_key),
            derived_key(&second_namespace, &second_key)
        );
    }
}
