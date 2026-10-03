//! Configuration validated at `build()`.

use actix_trypema::{ConfigError, TrypemaLimiter, extract::PeerIp};
use trypema::RateLimit;

use crate::common::per_ip;
use crate::support::backend;

#[test]
fn build_rejects_incomplete_or_unusable_configurations() {
    let missing_namespace = TrypemaLimiter::builder(backend())
        .extractor(PeerIp::default())
        .rate(RateLimit::per_minute_or_panic(1.0))
        .build();
    assert!(matches!(
        missing_namespace,
        Err(ConfigError::MissingNamespace)
    ));

    let invalid_namespace = per_ip(&backend(), "Not_Valid", 1.0).build();
    assert!(matches!(
        invalid_namespace,
        Err(ConfigError::InvalidNamespace(_))
    ));

    let missing_extractor = TrypemaLimiter::builder(backend())
        .namespace("ip")
        .rate(RateLimit::per_minute_or_panic(1.0))
        .build();
    assert!(matches!(
        missing_extractor,
        Err(ConfigError::MissingExtractor)
    ));

    let missing_rate = TrypemaLimiter::builder(backend())
        .namespace("ip")
        .extractor(PeerIp::default())
        .build();
    assert!(matches!(missing_rate, Err(ConfigError::MissingRate)));

    // Half a request per 60-second window truncates to zero capacity, which admits nothing.
    let zero_capacity = per_ip(&backend(), "ip", 0.5).build();
    assert!(matches!(zero_capacity, Err(ConfigError::ZeroCapacity)));

    assert!(per_ip(&backend(), "ip", 1.0).build().is_ok());
}
