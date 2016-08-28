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

// use std::mem;
use std::borrow::Cow;
// use std::collections::{HashMap, HashSet};
// use std::collections::hash_map::Entry as HEntry;
// use std::fs::{File, read_dir, remove_file, hard_link, remove_dir_all};
// use std::io::{self, BufRead, BufReader, Seek, SeekFrom, Write};
// use std::os::unix::fs::symlink;
// use std::path::{Path, PathBuf};
// use std::sync::mpsc;

// use time::{now, Tm, Duration};

// use handler::UpdaterMsg;
use message::CacheMsg;
use message::CacheMsg::{Tell, TellOld, TellTS, TellOldTS};
// use util::{localtime, ensure_dir, to_timefloat, day_path, all_days, open_file};
// use server::ClientAddr;

/// Number of entries to send back in one batch.
pub const BATCHSIZE: usize = 100;

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
    pub time: f64,
    pub ttl: f64,
    pub expired: bool,
    pub value: String,
}

impl Entry {
    pub fn new(time: f64, ttl: f64, value: &str) -> Entry {
        Entry { time: time, ttl: ttl, expired: value == "", value: value.into() }
    }

    pub fn new_owned(time: f64, ttl: f64, value: String) -> Entry {
        Entry { time: time, ttl: ttl, expired: value == "", value: value }
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
}

#[inline]
/// Helper function to split a full key into (catname, subkey), with the correct
/// handling of empty categories.
pub fn split_key(key: &str) -> (&str, &str) {
    if let Some(i) = key.rfind('/') {
        return (&key[..i], &key[i+1..]);
    } else {
        return ("nocat", key);
    }
}

#[inline]
/// Helper function to construct a full key from catname and subkey.
pub fn construct_key(catname: &str, subkey: &str) -> String {
    format!("{}{}{}",
            if catname == "nocat" { "" } else { catname },
            if catname == "nocat" { "" } else { "/" },
            subkey)
}
