//! Unit tests for `crate::middleware`.

use crate::middleware::remaining;

#[test]
fn remaining_saturates_on_torn_reads() {
    assert_eq!(remaining(10, 4, 0), 6);
    assert_eq!(remaining(10, 12, 4), 2);
    assert_eq!(remaining(10, 3, 5), 10);
    assert_eq!(remaining(10, 40, 0), 0);
}
