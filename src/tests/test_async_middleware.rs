//! Unit tests for `crate::async_middleware`.

use std::time::Duration;

use trypema::TrypemaError;

use crate::BackendFailure;
use crate::async_middleware::timed;

#[test]
fn a_hung_backend_call_is_a_timeout_failure() {
    let failure = actix_web::rt::System::new().block_on(timed(
        Duration::from_millis(10),
        std::future::pending::<Result<(), TrypemaError>>(),
    ));

    assert!(matches!(
        failure,
        Err(BackendFailure::Timeout(timeout)) if timeout == Duration::from_millis(10)
    ));
}

#[test]
fn a_backend_error_passes_through() {
    let failure = actix_web::rt::System::new().block_on(timed(
        Duration::from_secs(1),
        std::future::ready(Err::<(), _>(TrypemaError::CustomError("boom".to_string()))),
    ));

    assert!(matches!(
        failure,
        Err(BackendFailure::Error(TrypemaError::CustomError(_)))
    ));
}
