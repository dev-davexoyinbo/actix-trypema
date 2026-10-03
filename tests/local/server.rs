//! A real multi-worker server sharing one backend.

use std::{
    io::{Read as _, Write as _},
    net::{SocketAddr, TcpStream},
    time::Duration,
};

use actix_web::{App, web};

use crate::common::per_ip;
use crate::support::backend;

#[test]
fn workers_share_one_backend_and_one_limit() {
    let limiter = per_ip(&backend(), "workers", 4.0).build().unwrap();

    // The server runs on its own thread with its own System, so the workers are driven while
    // this thread issues blocking requests.
    let (sender, receiver) = std::sync::mpsc::channel();
    let server_thread = std::thread::spawn(move || {
        actix_web::rt::System::new().block_on(async move {
            let server = actix_web::HttpServer::new(move || {
                App::new()
                    .wrap(limiter.clone())
                    .route("/", web::get().to(|| async { "ok" }))
            })
            .workers(2)
            .bind(("127.0.0.1", 0))
            .unwrap();

            let addr = server.addrs()[0];
            let server = server.run();
            sender.send((addr, server.handle())).unwrap();
            server.await
        })
    });

    let (addr, handle) = receiver.recv_timeout(Duration::from_secs(10)).unwrap();
    let statuses: Vec<u16> = (0..6).map(|_| raw_get(addr)).collect();

    actix_web::rt::System::new().block_on(handle.stop(false));
    server_thread.join().unwrap().unwrap();

    let ok = statuses.iter().filter(|status| **status == 200).count();
    let rejected = statuses.iter().filter(|status| **status == 429).count();
    assert_eq!((ok, rejected), (4, 2), "{statuses:?}");
}

/// A minimal blocking HTTP/1.1 GET returning the status code.
fn raw_get(addr: SocketAddr) -> u16 {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();

    response
        .split_whitespace()
        .nth(1)
        .and_then(|status| status.parse().ok())
        .unwrap()
} // end fn raw_get
