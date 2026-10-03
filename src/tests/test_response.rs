//! Unit tests for `crate::response`.

use std::time::Duration;

use crate::response::{nonzero_duration, retry_after_secs};

#[test]
fn retry_after_rounds_up_with_minimum_one() {
    let cases = [
        (Duration::from_nanos(1), 1),
        (Duration::from_millis(999), 1),
        (Duration::from_millis(1000), 1),
        (Duration::from_millis(1001), 2),
        (Duration::from_secs(30), 30),
        (Duration::MAX, u64::MAX),
    ];

    for (duration, expected) in cases {
        assert_eq!(retry_after_secs(duration), expected, "{duration:?}");
    }

    assert_eq!(nonzero_duration(Duration::ZERO), None);
    assert_eq!(
        nonzero_duration(Duration::from_nanos(1)),
        Some(Duration::from_nanos(1))
    );
}
