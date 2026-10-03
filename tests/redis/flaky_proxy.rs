//! A cuttable TCP proxy in front of Redis, for failure injection.

use std::{
    net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs as _},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::support::redis_url;

fn upstream_addr() -> SocketAddr {
    redis_url()
        .trim_start_matches("redis://")
        .trim_end_matches('/')
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap()
}

/// A TCP proxy in front of the real Redis.
///
/// Backends are constructed while it forwards. `cut()` severs the live connections and
/// blackholes new ones, so the next backend call errors or hangs until the middleware timeout;
/// `restore()` forwards new connections again.
pub struct FlakyProxy {
    addr: SocketAddr,
    forwarding: Arc<AtomicBool>,
    live: Arc<Mutex<Vec<TcpStream>>>,
}

impl FlakyProxy {
    pub fn start() -> Self {
        let upstream = upstream_addr();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let forwarding = Arc::new(AtomicBool::new(true));
        let live: Arc<Mutex<Vec<TcpStream>>> = Arc::default();
        let accept_forwarding = Arc::clone(&forwarding);
        let accept_live = Arc::clone(&live);

        std::thread::spawn(move || {
            for inbound in listener.incoming() {
                let Ok(inbound) = inbound else { break };

                if !accept_forwarding.load(Ordering::Acquire) {
                    // Blackhole: keep the socket open and never answer.
                    accept_live.lock().unwrap().push(inbound);
                    continue;
                }

                let Ok(outbound) = TcpStream::connect(upstream) else {
                    break;
                };

                let mut client_reader = inbound.try_clone().unwrap();
                let mut upstream_writer = outbound.try_clone().unwrap();
                let mut upstream_reader = outbound.try_clone().unwrap();
                let mut client_writer = inbound.try_clone().unwrap();
                accept_live.lock().unwrap().extend([inbound, outbound]);

                std::thread::spawn(move || {
                    let _ = std::io::copy(&mut client_reader, &mut upstream_writer);
                });

                std::thread::spawn(move || {
                    let _ = std::io::copy(&mut upstream_reader, &mut client_writer);
                });
            }
        });

        Self {
            addr,
            forwarding,
            live,
        }
    } // end fn start

    pub fn url(&self) -> String {
        format!("redis://{}/", self.addr)
    }

    /// Sever every live connection and stop forwarding new ones.
    pub fn cut(&self) {
        self.forwarding.store(false, Ordering::Release);

        for stream in self.live.lock().unwrap().drain(..) {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
    } // end fn cut

    /// Sever the blackholed connections and forward new ones again.
    pub fn restore(&self) {
        self.forwarding.store(true, Ordering::Release);

        for stream in self.live.lock().unwrap().drain(..) {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
    } // end fn restore
} // end impl
