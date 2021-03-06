// -----------------------------------------------------------------------------
// A Rust implementation of the NICOS cache server.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the Free Software
// Foundation; either version 2 of the License, or (at your option) any later
// version.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
// details.
//
// You should have received a copy of the GNU General Public License along with
// this program; if not, write to the Free Software Foundation, Inc.,
// 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//
// Module authors:
//   Georg Brandl <g.brandl@fz-juelich.de>
//
// -----------------------------------------------------------------------------
//
//! This module contains the server instance itself.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream, TcpListener, UdpSocket, Shutdown};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use log::{info, warn};
use parking_lot::Mutex;
use crossbeam_channel::{unbounded, Sender, Receiver};
use mlzutil::fs::abspath;

use crate::handler::{Updater, Handler, UpdaterMsg};
use crate::database::{ThreadsafeDB, DB, Store};
use crate::store_flat::Store as FlatStore;
#[cfg(feature = "postgres")]
use crate::store_pgsql::Store as PgSqlStore;

pub const RECVBUF_LEN: usize = 4096;

pub type ClientAddr = SocketAddr;

/// Represents different ways to specify a store path.
pub enum StorePath {
    /// Specified as a normal filesystem path.  Uses the flat-file backend.
    Fs(PathBuf),
    /// Specified as an URI.  Currently only the postgresql:// scheme is supported.
    Uri(String),
}

impl StorePath {
    pub fn parse(path: &str) -> Result<StorePath, &'static str> {
        if path.contains("://") {
            if path.starts_with("postgresql://") {
                Ok(StorePath::Uri(path.to_string()))
            } else {
                Err("the given URI scheme is not supported")
            }
        } else {
            Ok(StorePath::Fs(abspath(&path)))
        }
    }
}

/// A trait abstracting our notion of a client -- could be TCP or UDP sockets in
/// the IP or Unix domain.
pub trait Client : Send {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize>;
    fn write(&self, _: &[u8]) -> io::Result<()>;
    fn try_clone(&self) -> io::Result<Box<dyn Client>>;
    fn close(&mut self);
    fn get_addr(&self) -> ClientAddr;
}

pub struct TcpClient(TcpStream, SocketAddr);
pub struct UdpClient(UdpSocket, SocketAddr, Option<Vec<u8>>);

impl Client for TcpClient {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
    fn write(&self, buf: &[u8]) -> io::Result<()> {
        (&self.0).write_all(buf)
    }
    fn try_clone(&self) -> io::Result<Box<dyn Client>> {
        self.0.try_clone().map(|s| Box::new(TcpClient(s, self.1)) as _)
    }
    fn close(&mut self) {
        let _ = self.0.shutdown(Shutdown::Both);
    }
    fn get_addr(&self) -> ClientAddr { self.1 }
}

impl Client for UdpClient {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // no further data received
        let mut n = 0;
        if let Some(v) = self.2.take() {
            n = v.len();
            for (loc, el) in buf.iter_mut().zip(v) {
                *loc = el;
            }
        }
        Ok(n)
    }
    fn write(&self, buf: &[u8]) -> io::Result<()> {
        let n = buf.len();
        let mut from = 0;
        while from < buf.len() {
            self.0.send_to(&buf[from..n.min(from+1496)], self.1)?;
            from += 1496;
        }
        Ok(())
    }
    fn try_clone(&self) -> io::Result<Box<dyn Client>> {
        self.0.try_clone().map(|s| Box::new(UdpClient(s, self.1, None)) as _)
    }
    fn close(&mut self) { }
    fn get_addr(&self) -> ClientAddr { self.1 }
}


/// Represents the main server object.
///
/// The Server creates the database object, starts a lot of threads and then
/// goes into a loop just waiting for a signal (SIGINT, SIGTERM) to stop.
///
/// The threads are:
/// - cleaner: goes through the database periodically, marks entries with TTL as
///   expired when needed
/// - updater: receives "update" messages from the database and handlers, and
///   sends key updates to clients who have subscribed to the key
/// - listeners: one listener for each server socket (UDP and TCP)
/// - handlers: each listener thread can spawn handler threads when a connection
///   comes in; each thread runs a Handler's main function
pub struct Server {
    db:    ThreadsafeDB,
    upd_q: Sender<UpdaterMsg>,
}

impl Server {
    pub fn new(storepath: StorePath, clear_db: bool) -> Result<Server, ()> {
        // create a channel to send updated keys to the updater thread
        let (w_updates, r_updates) = unbounded();

        // create the database object itself and wrap it into the mutex
        let store: Box<dyn Store> = match storepath {
            StorePath::Fs(path) => Box::new(FlatStore::new(path)),
            StorePath::Uri(ref uri) if uri.starts_with("postgresql://") => {
                Self::make_postgres_store(uri)?
            }
            StorePath::Uri(uri) => panic!("store URI {} not supported", uri)
        };
        let mut db = DB::new(store, w_updates.clone());
        if clear_db {
            info!("clearing stored database...");
            if let Err(e) = db.clear_db() {
                warn!("could not clear existing database: {}", e);
            }
        } else {
            info!("loading stored database...");
            if let Err(e) = db.load_db() {
                warn!("could not read existing database: {}", e);
            }
        }
        let db = Arc::new(Mutex::new(db));

        // start a thread that cleans the DB periodically of expired entries
        let db_clone = db.clone();
        thread::spawn(move || Server::cleaner(db_clone));

        // start a thread that sends out updates to connected clients
        thread::spawn(move || Server::updater(r_updates));

        Ok(Server { db, upd_q: w_updates })
    }

    #[cfg(feature = "postgres")]
    fn make_postgres_store(uri: &str) -> Result<Box<dyn Store>, ()> {
        match PgSqlStore::new(uri) {
            Ok(store) => Ok(Box::new(store)),
            Err(err) => {
                log::error!("could not connect to Postgres: {}", err);
                Err(())
            }
        }
    }

    #[cfg(not(feature = "postgres"))]
    fn make_postgres_store(_: &str) -> Result<Box<dyn Store>, ()> {
        panic!("not compiled with postgres support")
    }

    /// Periodically call the database's "clean" function, which searches for
    /// expired keys and updates clients about the expiration.
    fn cleaner(db: ThreadsafeDB) {
        info!("cleaner started");
        loop {
            thread::sleep(Duration::from_millis(250));
            {
                let mut db = db.lock();
                db.clean();
            }
        }
    }

    /// Receive key updates from the database, and distribute them to all
    /// connected clients.
    fn updater(chan: Receiver<UpdaterMsg>) {
        info!("updater started");
        let mut updaters: Vec<Updater> = Vec::with_capacity(8);
        for item in chan {
            match item {
                UpdaterMsg::Update(mut entry, source) => {
                    for upd in &updaters {
                        match source {
                            // if the update came from a certain client, do not send it
                            // back to this client
                            Some(a) if a == upd.addr => continue,
                            _ => upd.update(&mut entry),
                        }
                    }
                },
                UpdaterMsg::NewUpdater(updater) => {
                    updaters.push(*updater);
                },
                UpdaterMsg::Subscription(addr, key, with_ts) => {
                    if let Some(upd) = updaters.iter_mut().find(|u| u.addr == addr) {
                        upd.add_subscription(key, with_ts);
                    }
                },
                UpdaterMsg::CancelSubscription(addr, key, with_ts) => {
                    if let Some(upd) = updaters.iter_mut().find(|u| u.addr == addr) {
                        upd.remove_subscription(key, with_ts);
                    }
                },
                UpdaterMsg::RemoveUpdater(addr) => {
                    updaters.retain(|upd| upd.addr != addr);
                }
            }
        }
    }

    /// Listen for data on the UDP socket and spawn handlers for it.
    fn udp_listener(sock: UdpSocket, db: ThreadsafeDB) {
        info!("udp listener started");
        let mut recvbuf = [0u8; RECVBUF_LEN];
        loop {
            if let Ok((len, addr)) = sock.recv_from(&mut recvbuf) {
                info!("[{}] new UDP client connected", addr);
                let sock_clone = sock.try_clone().expect("could not clone socket");
                let client = UdpClient(sock_clone, addr,
                                       Some(recvbuf[..len].to_vec()));
                let db_clone = db.clone();
                let (w_tmp, _r_tmp) = unbounded();
                thread::spawn(move || {
                    Handler::new(Box::new(client), w_tmp, db_clone).handle();
                });
            }
        }
    }

    /// Listen for connections on the TCP socket and spawn handlers for it.
    fn tcp_listener(self, tcp_sock: TcpListener) {
        info!("tcp listener started");
        while let Ok((stream, addr)) = tcp_sock.accept() {
            let client = TcpClient(stream, addr);
            info!("[{}] new client connected", addr);
            // create the updater object and insert it into the mapping
            let upd_client = client.try_clone().expect("could not clone socket");
            let updater = Updater::new(upd_client, addr);
            let _ = self.upd_q.send(UpdaterMsg::NewUpdater(Box::new(updater)));

            // create the handler and start its main thread
            let notifier = self.upd_q.clone();
            let db_clone = self.db.clone();
            thread::spawn(move || Handler::new(Box::new(client), notifier, db_clone).handle());
        }
    }

    /// Main server function; start threads to accept clients on the listening
    /// socket and spawn handlers to handle them.
    pub fn start(self, addr: &str) -> io::Result<()> {
        // create the UDP socket and start its handler thread
        let udp_sock = UdpSocket::bind(addr)?;
        let db_clone = self.db.clone();
        thread::spawn(move || Server::udp_listener(udp_sock, db_clone));

        // create the TCP socket and start its handler thread
        let tcp_sock = TcpListener::bind(addr)?;
        thread::spawn(move || Server::tcp_listener(self, tcp_sock));
        Ok(())
    }
}
