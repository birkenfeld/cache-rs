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

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry as HEntry;
use std::fs::{File, read_dir, remove_file, hard_link, remove_dir_all};
use std::io::{self, BufRead, BufReader, Seek, SeekFrom, Write};
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::sync::mpsc;

use time::{now, Tm, Duration};

use handler::UpdaterMsg;
use message::CacheMsg;
use message::CacheMsg::{Tell, TellOld, TellTS, TellOldTS, LockRes};
use util::{localtime, ensure_dir, to_timefloat, day_path, all_days, open_file};
use server::ClientAddr;

/// Number of entries to send back in one batch.
const BATCHSIZE: usize = 100;

/// Represents an entry (without key) in the database.
///
/// The four pieces of data are:
/// - time: timestamp, as a float, of the last change of this value.
/// - ttl: time to live of this value; no TTL if zero.
/// - value: value, or empty string if deleted.
/// - expired: flag whether this value has expired or is deleted.
///   This flag is almost redundant, since either time + ttl < localtime
///   or an empty value give the same.  However it is much quicker to test
///   this flag, and on keys read from store files it is the only
///   indication that the value is expired.
#[derive(Clone, Debug)]
pub struct Entry {
    time: f64,
    ttl: f64,
    expired: bool,
    value: String,
}

impl Entry {
    pub fn new(time: f64, ttl: f64, value: &str) -> Entry {
        Entry { time: time, ttl: ttl, expired: value == "", value: String::from(value) }
    }

    /// Mark the Entry as expired.
    pub fn expired(mut self) -> Entry {
        self.expired = true;
        self
    }

    /// Convert the Entry into a Tell-type CacheMsg.
    pub fn to_msg<'a, T: Into<Cow<'a, str>>>(&'a self, key: T, with_ts: bool) -> CacheMsg {
        if with_ts {
            if self.expired {
                TellOldTS { key: key.into(), val: self.value.clone().into(),
                            time: self.time, ttl: self.ttl }
            } else {
                TellTS { key: key.into(), val: self.value.clone().into(),
                         time: self.time, ttl: self.ttl, no_store: false }
            }
        } else if self.expired {
            TellOld { key: key.into(), val: self.value.clone().into() }
        } else {
            Tell { key: key.into(), val: self.value.clone().into(), no_store: false }
        }
    }

    /// Return a Tell-type CacheMsg that represents a missing entry.
    pub fn no_msg(key: &str, with_ts: bool) -> CacheMsg {
        if with_ts {
            TellOldTS { key: key.into(), val: "".into(), time: 0., ttl: 0. }
        } else {
            TellOld { key: key.into(), val: "".into() }
        }
    }

    /// Write the Entry to a store file.
    pub fn to_file(&self, subkey: &str, fp: &mut File) -> io::Result<()> {
        let ttlsign = if self.ttl > 0. || self.expired { "-" } else { "+" };
        writeln!(fp, "{}\t{}\t{}\t{}",
                 subkey, self.time, ttlsign,
                 if self.expired { "-" } else { &self.value[..] })
    }
}

#[inline]
/// Helper function to split a full key into (catname, subkey), with the correct
/// handling of empty categories.
fn split_key(key: &str) -> (&str, &str) {
    if let Some(i) = key.rfind('/') {
        return (&key[..i], &key[i+1..]);
    } else {
        return ("nocat", key);
    }
}

#[inline]
/// Helper function to construct a full key from catname and subkey.
fn construct_key(catname: &str, subkey: &str) -> String {
    format!("{}{}{}",
            if catname == "nocat" { "" } else { &catname[..] },
            if catname == "nocat" { "" } else { "/" },
            subkey)
}


/// Represents the database of key-value entries.
pub struct DB {
    /// Root path for cache file storage.
    storepath:    PathBuf,
    /// YYYY/MM-DD path component.
    ymd_path:     String,
    /// Map of keys, first by categories (key prefixes) then by subkey.
    entry_cats:   HashMap<String, HashMap<String, Entry>>,
    /// Map of store files, by categories.
    files:        HashMap<String, File>,
    /// Map of lock entries.
    locks:        HashMap<String, Entry>,
    /// Map of rewrite entries (from X to (Y1, Y2, ...)).
    rewrites:     HashMap<String, HashSet<String>>,
    /// Inverse map of rewrite entries (from Y1 to X).
    inv_rewrites: HashMap<String, String>,
    /// Queue to send updates back to the updater thread.
    upd_q:        mpsc::Sender<UpdaterMsg>,
    /// Last and next midnight as floating timestamps.
    midnights:    (f64, f64),
}

impl DB {
    /// Create a new empty database.
    pub fn new(storepath: &str, upd_q: mpsc::Sender<UpdaterMsg>) -> DB {
        let thisday = Tm { tm_hour: 0, tm_min: 0, tm_sec: 0, tm_nsec: 0, ..now() };
        DB { storepath: Path::new(storepath).to_path_buf(),
             entry_cats: HashMap::new(),
             files: HashMap::new(),
             locks: HashMap::new(),
             rewrites: HashMap::new(),
             inv_rewrites: HashMap::new(),
             upd_q: upd_q,
             midnights: (to_timefloat(thisday),
                         to_timefloat(thisday + Duration::days(1))),
             ymd_path: day_path(thisday),
        }
    }

    /// Clear all DB store files.
    pub fn clear_db(&mut self) -> io::Result<()> {
        if self.storepath.is_dir() {
            try!(remove_dir_all(&self.storepath));
            try!(ensure_dir(&self.storepath));
            self.set_lastday();
        }
        Ok(())
    }

    /// Load the DB entries from the store path.
    pub fn load_db(&mut self) -> io::Result<()> {
        try!(ensure_dir(&self.storepath));
        let mut nentries = 0;
        let mut nfiles = 0;
        let mut need_rollover = false;

        // determine directory to read
        let mut p = self.storepath.join(&self.ymd_path);
        if !p.is_dir() {
            p = self.storepath.join("lastday");
            need_rollover = true;
        }
        if !p.is_dir() {
            info!("no previous values found, setting \"lastday\" link");
            self.set_lastday();
            return Ok(());
        }

        if let Ok(dentry_iter) = read_dir(p) {
            for dentry in dentry_iter {
                if let Ok(dentry) = dentry {
                    if !dentry.metadata().map(|m| m.is_file()).unwrap_or(false) {
                        continue;
                    }
                    let path = dentry.path();
                    let catname = path.file_name().unwrap();
                    let catname = catname.to_string_lossy().replace("-", "/");
                    match self.load_one_file(catname, dentry.path()) {
                        Ok(n) => {
                            nentries += n;
                            nfiles += 1;
                        },
                        Err(err) => {
                            warn!("could not read data from store file {:?}: {}",
                                  dentry.path(), err);
                        }
                    }
                }
            }
        }
        info!("db: read {} entries from {} storefiles", nentries, nfiles);
        if need_rollover {
            self.rollover()
        } else {
            Ok(())
        }
    }

    /// Load keys from a single file for category "catname".
    fn load_one_file(&mut self, catname: String, filename: PathBuf) -> io::Result<i32> {
        let fp = try!(open_file(filename, "ra"));
        let mut reader = BufReader::new(fp);
        let mut line = String::new();
        let mut nentries = 0;
        let mut map = HashMap::new();
        while let Ok(n) = reader.read_line(&mut line) {
            if n == 0 {
                break;
            } else {
                let parts = line.trim().split('\t').collect::<Vec<_>>();
                if parts.len() == 4 {
                    let subkey = parts[0].into();
                    if parts[2] == "+" {
                        // value is non-expiring: we can take it as valid
                        if let Ok(v) = parts[1].parse::<f64>() {
                            map.insert(subkey, Entry::new(v, 0., parts[3]));
                        }
                    } else if parts[3] != "-" {
                        // value was expiring but is not empty: take it as expired
                        if let Ok(v) = parts[1].parse::<f64>() {
                            map.insert(subkey, Entry::new(v, 0., parts[3]).expired());
                        }
                    } else if self.entry_cats.contains_key(&subkey) {
                        // value is empty: be sure to mark any current value as expired
                        map.get_mut(&subkey).unwrap().expired = true;
                    }
                    nentries += 1;
                }
            }
            line.clear();
        }
        let mut file = reader.into_inner();
        try!(file.seek(SeekFrom::End(0)));
        self.files.insert(catname.clone(), file);
        self.entry_cats.insert(catname, map);
        Ok(nentries)
    }

    /// Set the "lastday" symlink to the latest yyyy/mm-dd directory.
    fn set_lastday(&self) {
        let path = self.storepath.join("lastday");
        let _ign = remove_file(&path);
        if let Err(e) = symlink(&self.ymd_path, &path) {
            warn!("could not set \"lastday\" symlink: {}", e);
        }
    }

    /// Roll over all store files after midnight has passed.
    fn rollover(&mut self) -> io::Result<()> {
        info!("midnight passed, rolling over data files...");
        let thisday = Tm { tm_hour: 0, tm_min: 0, tm_sec: 0, tm_nsec: 0, ..now() };
        self.midnights = (to_timefloat(thisday),
                          to_timefloat(thisday + Duration::days(1)));
        self.ymd_path = day_path(thisday);
        let mut new_files = HashMap::new();
        let old_files = {
            self.files.drain().collect::<Vec<_>>()
        };
        for (catname, fp) in old_files {
            drop(fp);
            let submap = self.entry_cats.get(&catname).unwrap();
            let mut new_fp = try!(self.create_fd(&catname));
            for (subkey, entry) in submap {
                if !entry.expired {
                    try!(entry.to_file(subkey, &mut new_fp));
                }
            }
            new_files.insert(catname, new_fp);
        }
        self.files = new_files;
        self.set_lastday();
        Ok(())
    }

    /// Create a new file for a category.
    fn create_fd(&self, catname: &str) -> io::Result<File> {
        let safe_catname = catname.replace("/", "-");
        let subpath = self.storepath.join(&self.ymd_path);
        let linkfile = self.storepath.join(&safe_catname).join(&self.ymd_path);
        try!(ensure_dir(&subpath));
        let file = subpath.join(safe_catname);
        let mut fp = try!(open_file(&file, "wa"));
        if try!(fp.seek(SeekFrom::Current(0))) == 0 {
            try!(fp.write(b"# NICOS cache store file v2\n"));
        }
        try!(ensure_dir(linkfile.parent().unwrap()));
        if !linkfile.is_file() {
            try!(hard_link(file, linkfile));
        }
        Ok(fp)
    }

    /// Read history for a given subkey from a file.
    fn read_history(&self, path: &str, catname: &str, subkey: &str)
                    -> io::Result<Vec<(f64, String)>> {
        let catname = catname.replace("/", "-");
        let path = self.storepath.join(path).join(catname);
        let mut res = Vec::new();
        if !path.is_file() {
            return Ok(res)
        }
        let fp = try!(File::open(path));
        let reader = BufReader::new(fp);
        for line in reader.lines() {
            if let Ok(line) = line {
                let parts = line.trim().split('\t').collect::<Vec<_>>();
                if parts.len() == 4 && parts[0] == subkey {
                    let val = if parts[3] == "-" { "" } else { parts[3] };
                    res.push((parts[1].parse().unwrap_or(0.), String::from(val)));
                }
            }
        }
        Ok(res)
    }

    /// Clean up expired keys.
    pub fn clean(&mut self) {
        for (catname, submap) in &mut self.entry_cats {
            let now = localtime();
            for (subkey, entry) in submap.iter_mut() {
                if entry.expired {
                    continue;
                }
                if entry.ttl != 0. && (entry.time + entry.ttl < now) {
                    debug!("cleaner: {}/{} expired", catname, subkey);
                    entry.expired = true;
                    let fullkey = construct_key(catname, subkey);
                    let _ign = self.upd_q.send(
                        UpdaterMsg::Update(fullkey, entry.clone(), None));
                    let mut fp = self.files.get_mut(catname).unwrap();
                    let _ign = entry.to_file(subkey, &mut fp);
                }
            }
        }
    }

    // Public API used by the Handler

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
        if time >= self.midnights.1 {
            try!(self.rollover());
        }
        let (catname, subkey) = split_key(key);
        let mut newcats = vec![catname];
        // process rewrites for this key's prefix (= category)
        if let Some(rew_cats) = self.rewrites.get(catname) {
            newcats.extend(rew_cats.iter().map(|s| &s[..]));
        }
        let entry = Entry::new(time, ttl, val);
        for cat in newcats {
            let mut need_update = true;
            // write to in-memory map
            {
                let submap = self.entry_cats.entry(cat.into()).or_insert_with(HashMap::new);
                if let Some(ref mut entry) = submap.get_mut(subkey) {
                    // if we already have the same value, only adapt time and ttl info
                    if entry.value == val && !entry.expired {
                        need_update = false;
                        entry.time = time;
                        entry.ttl = ttl;
                    } else if val == "" && entry.expired {
                        // if the value is deleted, but the entry is already expired,
                        // no need to record the deletion
                        need_update = false;
                    }
                }
                submap.insert(subkey.into(), entry.clone());
            }
            // write to on-disk file
            if need_update && !no_store {
                if !self.files.contains_key(cat) {
                    let fp = try!(self.create_fd(cat));
                    self.files.insert(cat.into(), fp);
                }
                let fp = self.files.get_mut(cat).unwrap();
                try!(entry.to_file(subkey, fp));
            }
            // notify about update
            if need_update {
                let fullkey = construct_key(cat, subkey);
                self.upd_q.send(
                    UpdaterMsg::Update(fullkey, entry.clone(), Some(from)))
                          .expect("could not send to updates queue");
            }
        }
        Ok(())
    }

    /// Ask for a single value.
    pub fn ask(&self, key: &str, with_ts: bool, send_q: &mpsc::Sender<String>) {
        let (catname, subkey) = split_key(key);
        let msg = match self.entry_cats.get(catname).and_then(|m| m.get(subkey)) {
            None => Entry::no_msg(key, with_ts),
            Some(entry) => entry.to_msg(key, with_ts),
        };
        let _ = send_q.send(msg.to_string());
    }

    /// Ask for many values matching a key wildcard.
    pub fn ask_wc(&self, wc: &str, with_ts: bool, send_q: &mpsc::Sender<String>) {
        let mut res = Vec::with_capacity(BATCHSIZE);
        for (catname, submap) in &self.entry_cats {
            for (subkey, entry) in submap.iter() {
                let fullkey = construct_key(catname, subkey);
                if fullkey.find(wc).is_some() {
                    res.push(entry.to_msg(fullkey, with_ts).to_string());
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
    pub fn ask_hist(&self, key: &str, from: f64, delta: f64, send_q: &mpsc::Sender<String>) {
        let (catname, subkey) = split_key(key);
        if delta < 0. {
            return;
        }
        let to = from + delta;
        let paths = if from >= self.midnights.0 {
            vec![self.ymd_path.clone()]
        } else {
            all_days(from, to)
        };
        let mut res = Vec::with_capacity(BATCHSIZE);
        for path in paths {
            match self.read_history(&path, catname, subkey) {
                Err(e)   => warn!("could not read histfile for {}/{}: {}", path, catname, e),
                Ok(msgs) => {
                    for (time, val) in msgs {
                    if from <= time && time <= to {
                        res.push(TellTS { key: key.into(), val: val.into(), time: time,
                                          ttl: 0., no_store: false }.to_string());
                        if res.len() >= BATCHSIZE {
                            let _ = send_q.send(res.join(""));
                            res.clear();
                        }
                    }
                }}
            }
        }
        let _ = send_q.send(res.join(""));
    }

    /// Lock or unlock a key for multi-process synchronization.
    pub fn lock(&mut self, lock: bool, key: &str, client: &str, time: f64, ttl: f64,
                send_q: &mpsc::Sender<String>) {
        // find existing lock entry (these are in a different namespace from normal keys)
        let entry = self.locks.entry(String::from(key));
        let msg = if lock {
            match entry {
                HEntry::Occupied(mut entry) => {
                    let lock_denied = {
                        let e = entry.get();
                        e.value != client && (e.ttl == 0. || e.time + e.ttl < localtime())
                    };
                    if lock_denied {
                        info!("lock {}: denied to {} (locked by {})", key, client, entry.get().value);
                        LockRes { key: key.into(), client: entry.get().value.clone().into() }
                    } else {
                        entry.insert(Entry::new(time, ttl, client));
                        debug!("lock {}: granted to {} (same client or lock expired)", key, client);
                        LockRes { key: key.into(), client: "".into() }
                    }
                },
                HEntry::Vacant(entry) => {
                    entry.insert(Entry::new(time, ttl, client));
                    info!("lock {}: granted to {} (no lock)", key, client);
                    LockRes { key: key.into(), client: "".into() }
                }
            }
        } else {
            match entry {
                HEntry::Occupied(ref entry) if entry.get().value != client => {
                    info!("unlock {}: denied to {} (locked by {})", key, client, entry.get().value);
                    LockRes { key: key.into(), client: entry.get().value.clone().into() }
                },
                HEntry::Occupied(entry) => {
                    info!("unlock {}: granted to {} (unlocked)", key, client);
                    entry.remove();
                    LockRes { key: key.into(), client: "".into() }
                },
                HEntry::Vacant(..) => {
                    info!("unlock {}: granted to {} (unnecessary)", key, client);
                    LockRes { key: key.into(), client: "".into() }
                }
            }
        };
        let _ = send_q.send(msg.to_string());
    }
}
