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
//! This module contains the definition for the in-memory and on-disk database.

use std::io;
use std::collections::HashSet;
use std::collections::hash_map::Entry as HEntry;
use std::sync::Arc;
use fnv::FnvHashMap as HashMap;
use parking_lot::Mutex;
use crossbeam_channel::Sender;

use entry::{Entry, BATCHSIZE, split_key, construct_key};
use handler::UpdaterMsg;
use util::localtime;
use server::ClientAddr;
use message::CacheMsg::{TellTS, LockRes};

pub type EntryMap = HashMap<String, HashMap<String, Entry>>;

/// Represents the database of key-value entries.
///
/// The database object is split into the part that deals with in-memory store
/// of the current key-value set and the part that deals with storing the history
/// and querying past values.  The latter part (`Store`) is factored out into
/// a trait and pluggable.
pub struct DB {
    /// Store backend (dynamically dispatched).
    store:        Box<Store>,
    /// Map of keys, first by categories (key prefixes) then by subkey.
    entry_map:    EntryMap,
    /// Map of lock entries.
    locks:        HashMap<String, Entry>,
    /// Map of rewrite entries (from X to (Y1, Y2, ...)).
    rewrites:     HashMap<String, HashSet<String>>,
    /// Inverse map of rewrite entries (from Y1 to X).
    inv_rewrites: HashMap<String, String>,
    /// Queue to send updates back to the updater thread.
    upd_q:        Sender<UpdaterMsg>,
}

pub type ThreadsafeDB = Arc<Mutex<DB>>;

pub trait Store : Send {
    /// Clear all stored data.  Used for --clear invocation.
    fn clear(&mut self) -> io::Result<()>;
    /// Load latest key-value set from stored data.
    fn load_latest(&mut self, entry_map: &mut EntryMap) -> io::Result<()>;
    /// Called when a new key is set.  Used to roll over stores or similar.
    fn tell_hook(&mut self, entry: &Entry, entry_map: &mut EntryMap) -> io::Result<()>;
    /// Save a new entry to the store.
    fn save(&mut self, catname: &str, subkey: &str, entry: &Entry) -> io::Result<()>;
    /// Query history of entries for a specified key to given client.
    fn query_history(&mut self, key: &str, from: f64, to: f64, send: &mut FnMut(f64, &str));
}

impl DB {
    /// Create a new empty database.
    pub fn new(store: Box<Store>, upd_q: Sender<UpdaterMsg>) -> DB {
        DB {
            store,
            upd_q,
            entry_map: HashMap::default(),
            locks: HashMap::default(),
            rewrites: HashMap::default(),
            inv_rewrites: HashMap::default(),
        }
    }

    /// Clear all DB store files.
    pub fn clear_db(&mut self) -> io::Result<()> {
        self.store.clear()
    }

    /// Load the DB entries from the store path.
    pub fn load_db(&mut self) -> io::Result<()> {
        self.store.load_latest(&mut self.entry_map)
    }

    /// Clean up expired keys.
    pub fn clean(&mut self) {
        for (catname, submap) in &mut self.entry_map {
            let now = localtime();
            for (subkey, entry) in submap.iter_mut() {
                if entry.expired {
                    continue;
                }
                if entry.ttl != 0. && (entry.time + entry.ttl < now) {
                    debug!("cleaner: {}/{} expired", catname, subkey);
                    entry.expired = true;
                    let fullkey = construct_key(catname, subkey);
                    let _ = self.upd_q.send(
                        UpdaterMsg::Update(fullkey, entry.clone(), None));
                    let _ = self.store.save(catname, subkey, entry);
                }
            }
        }
    }

    /// Set or delete a prefix rewrite entry.
    pub fn rewrite(&mut self, new: &str, old: &str) {
        // rewrite goes old -> new
        let old = old.to_lowercase();
        // remove any existing rewrite to the "new" prefix
        if let Some(previous) = self.inv_rewrites.remove(new) {
            if let HEntry::Occupied(mut entry) = self.rewrites.entry(previous) {
                entry.get_mut().remove(new);
                if entry.get().is_empty() {
                    entry.remove();
                }
            }
        }
        // then, if old is not empty, insert a new rewrite
        if old != "" {
            self.inv_rewrites.insert(new.into(), old.clone());
            self.rewrites.entry(old).or_insert_with(HashSet::new).insert(new.into());
        }
        info!("rewrites={:?} inv_rewrites={:?}",
              self.rewrites, self.inv_rewrites);
    }

    /// Insert or update a key-value entry.
    pub fn tell(&mut self, key: &str, val: &str, time: f64, ttl: f64, no_store: bool,
                from: ClientAddr) -> io::Result<()> {
        let (catname, subkey) = split_key(key);
        let mut newcats = vec![catname];
        // process rewrites for this key's prefix (= category)
        if let Some(rewrite_cats) = self.rewrites.get(catname) {
            newcats.extend(rewrite_cats.iter().map(String::as_str));
        }
        let entry = Entry::new(time, ttl, val);
        self.store.tell_hook(&entry, &mut self.entry_map)?;
        for catname in newcats {
            let mut need_update = true;
            // write to in-memory map
            if let Some(catmap) = self.entry_map.get_mut(catname) {
                if let Some(existing_entry) = catmap.get_mut(subkey) {
                    if existing_entry.value == val && !existing_entry.expired {
                        // if we already have the same value, only adapt time
                        // and ttl info
                        need_update = false;
                        existing_entry.time = time;
                        existing_entry.ttl = ttl;
                    } else {
                        if val == "" && existing_entry.expired {
                            // if the value is deleted, but the entry was already
                            // expired, no need to record the deletion
                            need_update = false;
                        }
                        *existing_entry = entry.clone();
                    }
                } else {
                    catmap.insert(subkey.into(), entry.clone());
                }
            } else {
                let mut catmap = HashMap::default();
                catmap.insert(subkey.into(), entry.clone());
                self.entry_map.insert(catname.into(), catmap);
            }
            // write to on-disk file
            if need_update && !no_store {
                self.store.save(catname, subkey, &entry)?;
            }
            // notify about update (nostore keys are always propagated)
            if need_update || no_store {
                let fullkey = construct_key(catname, subkey);
                self.upd_q.send(
                    UpdaterMsg::Update(fullkey, entry.clone(), Some(from)))
                          .expect("could not send to updates queue");
            }
        }
        Ok(())
    }

    /// Ask for a single value.
    pub fn ask(&self, key: &str, with_ts: bool, send_q: &Sender<String>) {
        let (catname, subkey) = split_key(key);
        let msg = match self.entry_map.get(catname).and_then(|m| m.get(subkey)) {
            None => Entry::no_msg(key, with_ts),
            Some(entry) => entry.to_msg(key, with_ts),
        };
        let _ = send_q.send(msg.to_string());
    }

    /// Ask for many values matching a key wildcard.
    pub fn ask_wc(&self, wc: &str, with_ts: bool, send_q: &Sender<String>) {
        let mut res = Vec::with_capacity(BATCHSIZE);
        for (catname, catmap) in &self.entry_map {
            for (subkey, entry) in catmap.iter() {
                let fullkey = construct_key(catname, subkey);
                if fullkey.find(wc).is_some() {
                    res.push(entry.to_msg(&fullkey, with_ts).to_string());
                    if res.len() >= BATCHSIZE {
                        let _ = send_q.send(res.join(""));
                        res.clear();
                    }
                }
            }
        }
        let _ = send_q.send(res.join(""));
    }

    /// Ask for the history of a single key.
    pub fn ask_hist(&mut self, key: &str, from: f64, delta: f64, send_q: &Sender<String>) {
        if delta < 0. {
            return;
        }
        let mut res = Vec::with_capacity(BATCHSIZE);
        self.store.query_history(key, from, from + delta, &mut |time, val| {
            res.push(TellTS { key, val, time, ttl: 0., no_store: false }.to_string());
            if res.len() >= BATCHSIZE {
                let _ = send_q.send(res.join(""));
                res.clear();
            }
        });
        let _ = send_q.send(res.join(""));
    }

    /// Lock or unlock a key for multi-process synchronization.
    pub fn lock(&mut self, lock: bool, key: &str, client: &str, time: f64, ttl: f64,
                send_q: &Sender<String>) {
        // find existing lock entry (these are in a different namespace from normal keys)
        let entry = self.locks.entry(key.into());
        let msg = if lock {
            match entry {
                HEntry::Occupied(mut entry) => {
                    let lock_denied = {
                        let e = entry.get();
                        e.value != client && (e.ttl == 0. || e.time + e.ttl < localtime())
                    };
                    if lock_denied {
                        info!("lock {}: denied to {} (locked by {})", key, client, entry.get().value);
                        LockRes { key, client: &entry.get().value }.to_string()
                    } else {
                        entry.insert(Entry::new(time, ttl, client));
                        debug!("lock {}: granted to {} (same client or lock expired)", key, client);
                        LockRes { key, client: "" }.to_string()
                    }
                },
                HEntry::Vacant(entry) => {
                    entry.insert(Entry::new(time, ttl, client));
                    info!("lock {}: granted to {} (no lock)", key, client);
                    LockRes { key, client: "" }.to_string()
                }
            }
        } else {
            match entry {
                HEntry::Occupied(ref entry) if entry.get().value != client => {
                    info!("unlock {}: denied to {} (locked by {})", key, client, entry.get().value);
                    LockRes { key, client: &entry.get().value }.to_string()
                },
                HEntry::Occupied(entry) => {
                    info!("unlock {}: granted to {} (unlocked)", key, client);
                    entry.remove();
                    LockRes { key, client: "" }.to_string()
                },
                HEntry::Vacant(..) => {
                    info!("unlock {}: granted to {} (unnecessary)", key, client);
                    LockRes { key, client: "" }.to_string()
                }
            }
        };
        let _ = send_q.send(msg);
    }
}
