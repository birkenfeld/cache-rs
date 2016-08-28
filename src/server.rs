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
//   Georg Brandl <georg.brandl@frm2.tum.de>
//
// -----------------------------------------------------------------------------
//
//! This module contains the server instance itself.

use std::cmp::min;
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream, TcpListener, UdpSocket, Shutdown};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use handler::{Updater, Handler, UpdaterMsg};
use database::{DB, Store};
use store_flat::Store as FlatStore;
use store_pgsql::Store as PgSqlStore;
use util::{Threadsafe, threadsafe, lock_mutex};

pub const RECVBUF_LEN: usize = 4096;

pub type ClientAddr = SocketAddr;


/// A trait abstracting our notion of a client -- could be TCP or UDP sockets in
/// the IP or Unix domain.
pub trait Client : Send {
    fn read(&mut self, &mut [u8]) -> io::Result<usize>;
    fn write(&mut self, &[u8]) -> io::Result<usize>;
    fn try_clone(&self) -> io::Result<Box<Client>>;
    fn close(&mut self);
    fn get_addr(&self) -> ClientAddr;
}

pub struct TcpClient(TcpStream, SocketAddr);
pub struct UdpClient(UdpSocket, SocketAddr, Option<Vec<u8>>);

impl Client for TcpClient {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }
    fn try_clone(&self) -> io::Result<Box<Client>> {
        self.0.try_clone().map(|s| (Box::new(TcpClient(s, self.1)) as Box<Client>))
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
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = buf.len();
        let mut from = 0;
        while from < buf.len() {
            try!(self.0.send_to(&buf[from..min(n, from+1496)], self.1));
            from += 1496;
        }
        Ok(n)
    }
    fn try_clone(&self) -> io::Result<Box<Client>> {
        self.0.try_clone().map(|s| (Box::new(UdpClient(s, self.1, None)) as Box<Client>))
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
    db:       Threadsafe<DB>,
    upd_map:  Threadsafe<HashMap<ClientAddr, Updater>>,
    upd_q:    mpsc::Sender<UpdaterMsg>,
}

impl Server {
    pub fn new(storepath: &str, clear_db: bool) -> Server {
        // create a channel to send updated keys to the updater thread
        let (w_updates, r_updates) = mpsc::channel();

        // create the database object itself and wrap it into the mutex
        let store: Box<Store> = if storepath.starts_with("postgresql://") {
            Box::new(PgSqlStore::new(storepath))
        } else {
            Box::new(FlatStore::new(storepath))
        };
        let mut db = DB::new(store, w_updates.clone());
        info!("loading stored database...");
        if clear_db {
            if let Err(e) = db.clear_db() {
                warn!("could not clear existing database: {}", e);
            }
        } else if let Err(e) = db.load_db() {
            warn!("could not read existing database: {}", e);
        }
        let db = threadsafe(db);

        // start a thread that cleans the DB periodically of expired entries
        let db_clone = db.clone();
        thread::spawn(move || Server::cleaner(db_clone));

        // create the updaters map
        let upd_map = threadsafe(HashMap::new());

        // start a thread that sends out updates to connected clients
        let upd_map_clone = upd_map.clone();
        thread::spawn(move || Server::updater(r_updates, upd_map_clone));

        Server { db: db, upd_q: w_updates, upd_map: upd_map }
    }

    /// Periodically call the database's "clean" function, which searches for
    /// expired keys and updates clients about the expiration.
    fn cleaner(db: Threadsafe<DB>) {
        info!("cleaner started");
        loop {
            thread::sleep(Duration::from_millis(250));
            {
                let mut db = lock_mutex(&db);
                db.clean();
            }
        }
    }

    /// Receive key updates from the database, and distribute them to all
    /// connected clients.
    fn updater(chan: mpsc::Receiver<UpdaterMsg>,
               upd_map: Threadsafe<HashMap<ClientAddr, Updater>>) {
        info!("updater started");
        // whenever the update to the client fails, we drop it from the
        // mapping of connected clients
        let mut to_drop = Vec::new();
        for item in chan.iter() {
            let mut map = lock_mutex(&upd_map);
            match item {
                UpdaterMsg::Update(ref key, ref entry, ref source) => {
                    for (addr, updater) in map.iter_mut() {
                        match *source {
                            // if the update came from a certain client, do not send it
                            // back to this client
                            Some(ref a) if a == addr => { },
                            _ => {
                                if !updater.update(key, entry) {
                                    to_drop.push(*addr);
                                }
                            }
                        }
                    }
                    for addr in to_drop.drain(..) {
                        map.remove(&addr);
                    }
                },
                UpdaterMsg::Subscription(ref addr, ref key, with_ts) => {
                    if let Some(u) = map.get_mut(addr) {
                        u.add_subscription(key, with_ts);
                    }
                },
                UpdaterMsg::CancelSubscription(ref addr, ref key, with_ts) => {
                    if let Some(u) = map.get_mut(addr) {
                        u.remove_subscription(key, with_ts);
                    }
                },
            }
        }
    }

    /// Listen for data on the UDP socket and spawn handlers for it.
    fn udp_listener(sock: UdpSocket, db: Threadsafe<DB>) {
        info!("udp listener started");
        let mut recvbuf = [0u8; RECVBUF_LEN];
        loop {
            if let Ok((len, addr)) = sock.recv_from(&mut recvbuf) {
                info!("[{}] new UDP client connected", addr);
                let sock_clone = sock.try_clone().expect("could not clone socket");
                let client = UdpClient(sock_clone, addr,
                                       Some(recvbuf[..len].to_vec()));
                let db_clone = db.clone();
                let (w_tmp, _r_tmp) = mpsc::channel();
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
            let mut upd_map = lock_mutex(&self.upd_map);
            let upd_client = client.try_clone().expect("could not clone socket");
            let updater = Updater::new(upd_client, addr.to_string());
            upd_map.insert(addr, updater);

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
        let udp_sock = try!(UdpSocket::bind(addr));
        let db_clone = self.db.clone();
        thread::spawn(move || Server::udp_listener(udp_sock, db_clone));

        // create the TCP socket and start its handler thread
        let tcp_sock = try!(TcpListener::bind(addr));
        thread::spawn(move || Server::tcp_listener(self, tcp_sock));
        Ok(())
    }
}
