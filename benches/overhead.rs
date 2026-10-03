//! Per-request cost of each limiter behind a no-op handler.
//!
//! Every variant runs the same harness — build a test request, call the service — so the
//! difference against `bare` is the limiter's own cost. Limits are far above the benchmark volume,
//! so only the admit path is measured. With `--features redis`, the Redis and Hybrid variants run
//! against `REDIS_URL` (default `redis://127.0.0.1:16379/`, see `compose.yaml`).

use std::{net::SocketAddr, time::Instant};

use actix_governor::{Governor, GovernorConfigBuilder};
use actix_trypema::{Backend, HeaderMode, Local, TrypemaLimiter, extract::PeerIp};
use actix_web::{
    App,
    test::{self, TestRequest},
    web,
};
use criterion::{Criterion, criterion_group, criterion_main};
use trypema::{RateLimit, WindowSize};

const PEER: &str = "203.0.113.7:9000";

/// A per-IP limiter whose rate is never reached, with rate limit headers off (as in governor).
fn limiter<B: Backend>(backend: B, namespace: &str) -> TrypemaLimiter<B> {
    TrypemaLimiter::builder(backend)
        .namespace(namespace)
        .extractor(PeerIp::default())
        .rate(RateLimit::per_second_or_panic(1e9))
        .headers(HeaderMode::Off)
        .build()
        .unwrap()
}

fn overhead(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("overhead");
    group.sample_size(60);

    let runner = actix_web::rt::System::new();
    let peer: SocketAddr = PEER.parse().unwrap();
    let window = WindowSize::seconds_or_panic(3600);

    macro_rules! bench_app {
        ($name:expr, $app:expr) => {
            let service = runner.block_on(test::init_service($app));
            group.bench_function($name, |bencher| {
                bencher.iter_custom(|iterations| {
                    runner.block_on(async {
                        let started = Instant::now();

                        for _ in 0..iterations {
                            let request = TestRequest::default().peer_addr(peer).to_request();
                            let response = test::call_service(&service, request).await;
                            std::hint::black_box(response);
                        }

                        started.elapsed()
                    })
                });
            });
        };
    }

    let handler = || async { "ok" };

    bench_app!("bare", App::new().route("/", web::get().to(handler)));

    let governor_config = GovernorConfigBuilder::default()
        .requests_per_second(1_000_000_000)
        .burst_size(u32::MAX)
        .finish()
        .unwrap();
    bench_app!(
        "actix_governor",
        App::new()
            .wrap(Governor::new(&governor_config))
            .route("/", web::get().to(handler))
    );

    bench_app!(
        "trypema_local",
        App::new()
            .wrap(limiter(Local::new(window).unwrap(), "local"))
            .route("/", web::get().to(handler))
    );

    #[cfg(feature = "redis")]
    {
        use actix_trypema::{Hybrid, Redis};

        let url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:16379/".to_string());
        let connection = runner.block_on(async {
            redis::Client::open(url)
                .unwrap()
                .get_connection_manager()
                .await
                .unwrap()
        });

        // Construction spawns the providers' background tasks, so it runs on the actix runtime.
        let redis = runner.block_on(async { Redis::new(connection.clone(), window).unwrap() });
        let hybrid = runner.block_on(async { Hybrid::new(connection, window).unwrap() });

        bench_app!(
            "trypema_redis",
            App::new()
                .wrap(limiter(redis, "redis"))
                .route("/", web::get().to(handler))
        );
        bench_app!(
            "trypema_hybrid",
            App::new()
                .wrap(limiter(hybrid, "hybrid"))
                .route("/", web::get().to(handler))
        );
    }

    group.finish();
} // end fn overhead

criterion_group!(benches, overhead);
criterion_main!(benches);
