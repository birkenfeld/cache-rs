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
//! This module contains the handler for a single network connection.

use std::thread;
use aho_corasick::{Automaton, AcAutomaton};
use crossbeam_channel::{unbounded, Sender, Receiver};

use entry::UpdaterEntry;
use database::ThreadsafeDB;
use message::CacheMsg;
use message::CacheMsg::*;
use util::localtime;
use server::{ClientAddr, Client, RECVBUF_LEN};


/// Provides functionality to send key updates to the the connected client.
///
/// This is a separate object since it is shared between the update thread and
/// the handler threads.
pub struct Updater {
    pub addr: ClientAddr,
    client:   Box<Client>,
    subs:     Vec<String>,
    with_ts:  bool,
    searcher: AcAutomaton<String>,
}

/// These objects are sent to the updater thread from the DB and handlers.
pub enum UpdaterMsg {
    NewUpdater(Updater),
    Update(UpdaterEntry, Option<ClientAddr>),
    Subscription(ClientAddr, String, bool),
    CancelSubscription(ClientAddr, String, bool),
    RemoveUpdater(ClientAddr),
}

/// Handles incoming queries on a connected client and executes the corresponding
/// database calls.
pub struct Handler {
    name:   String,
    client: Box<Client>,
    addr:   ClientAddr,
    db:     ThreadsafeDB,
    upd_q:  Sender<UpdaterMsg>,
    send_q: Sender<String>,
}

impl Updater {
    pub fn new(client: Box<Client>, addr: ClientAddr) -> Updater {
        Updater { addr, client, subs: vec![], with_ts: false,
                  searcher: AcAutomaton::new(vec![]) }
    }

    /// Add a new subscription for this client.
    pub fn add_subscription(&mut self, key: String, with_ts: bool) {
        self.subs.push(key);
        self.with_ts |= with_ts;
        self.searcher = AcAutomaton::new(self.subs.clone());
    }

    /// Remove a subscription for this client.
    pub fn remove_subscription(&mut self, key: String, _with_ts: bool) {
        self.subs.retain(|substr| substr != &key);
        self.with_ts = !self.subs.is_empty();
        self.searcher = AcAutomaton::new(self.subs.clone());
    }

    /// Update this client, if the key is matched by one of the subscriptions.
    pub fn update(&self, entry: &mut UpdaterEntry) {
        if !self.subs.is_empty() && self.searcher.find(entry.key()).next().is_some() {
            debug!("[{}] update: {:?} | {:?}", self.addr, entry, self.subs);
            let _ = self.client.write(entry.get_msg(self.with_ts).as_bytes());
        }
    }
}

impl Handler {
    pub fn new(client: Box<Client>, upd_q: Sender<UpdaterMsg>, db: ThreadsafeDB) -> Handler {
        // spawn a thread that handles sending back replies to the socket
        let (w_msgs, r_msgs) = unbounded();
        let send_client = client.try_clone().expect("could not clone socket");
        let thread_name = client.get_addr().to_string();
        thread::spawn(move || Handler::sender(&thread_name, send_client, r_msgs));
        Handler {
            name:   client.get_addr().to_string(),
            addr:   client.get_addr(),
            send_q: w_msgs,
            client,
            db,
            upd_q,
        }
    }

    /// Thread that sends back replies (but not updates) to the client.
    fn sender(name: &str, client: Box<Client>, r_msgs: Receiver<String>) {
        for to_send in r_msgs.iter() {
            if let Err(err) = client.write(to_send.as_bytes()) {
                warn!("[{}] write error in sender: {}", name, err);
                break;
            }
        }
        info!("[{}] sender quit", name);
    }

    /// Handle a single cache message.
    fn handle_msg(&self, msg: CacheMsg) {
        // get a handle to the DB (since all but one of the message types require DB
        // access, we do it here once)
        let mut db = self.db.lock();
        match msg {
            // key updates
            Tell { key, val, no_store } =>
                if let Err(err) = db.tell(key, val, localtime(), 0., no_store, self.addr) {
                    warn!("could not write key {} to db: {}", key, err);
                },
            TellTS { time, ttl, key, val, no_store } =>
                if let Err(err) = db.tell(key, val, time, ttl, no_store, self.addr) {
                    warn!("could not write key {} to db: {}", key, err);
                },
            // key inquiries
            Ask { key, with_ts } =>
                db.ask(key, with_ts, &self.send_q),
            AskWild { key, with_ts } =>
                db.ask_wc(key, with_ts, &self.send_q),
            AskHist { key, from, delta } =>
                db.ask_hist(key, from, delta, &self.send_q),
            // locking
            Lock { key, client, time, ttl } =>
                db.lock(true, key, client, time, ttl, &self.send_q),
            Unlock { key, client } =>
                db.lock(false, key, client, 0., 0., &self.send_q),
            // meta messages
            Rewrite { new_prefix, old_prefix } =>
                db.rewrite(new_prefix, old_prefix),
            Subscribe { key, with_ts } => {
                let _ = self.upd_q.send(
                    UpdaterMsg::Subscription(self.addr, key.into(), with_ts));
            },
            Unsub { key, with_ts } => {
                let _ = self.upd_q.send(
                    UpdaterMsg::CancelSubscription(self.addr, key.into(), with_ts));
            },
            // we ignore TellOlds
            _ => (),
        }
    }

    /// Process a single line (message).
    fn process(&self, line: &str) -> bool {
        match CacheMsg::parse(line) {
            Some(Quit) => {
                // an empty line closes the connection
                false
            }
            Some(msg) => {
                debug!("[{}] processing {:?} => {:?}", self.name, line, msg);
                self.handle_msg(msg);
                true
            }
            None => {
                // not a valid cache protocol line => ignore it
                warn!("[{}] strange line: {:?}", self.name, line);
                true
            }
        }
    }

    /// Handle incoming stream of messages.
    pub fn handle(&mut self) {
        let mut buf = Vec::with_capacity(RECVBUF_LEN);
        let mut recvbuf = [0u8; RECVBUF_LEN];

        'outer: loop {
            // read a chunk of incoming data
            let got = match self.client.read(&mut recvbuf) {
                Err(err) => {
                    warn!("[{}] error in recv(): {}", self.name,  err);
                    break;
                },
                Ok(0)    => break,  // no data from blocking read...
                Ok(got)  => got,
            };
            // convert to string and add to our buffer
            buf.extend_from_slice(&recvbuf[0..got]);
            // process all whole lines we got
            let mut from = 0;
            while let Some(to) = buf[from..].iter().position(|b| *b == b'\n') {
                let line_str = String::from_utf8_lossy(&buf[from..from+to+1]);
                if !self.process(&line_str) {
                    // false return value means "quit"
                    break 'outer;
                }
                from += to + 1;
            }
            buf.drain(0..from);
        }
        let _ = self.upd_q.send(UpdaterMsg::RemoveUpdater(self.addr));
        info!("[{}] handler is finished", self.name);
        self.client.close();
    }
}
