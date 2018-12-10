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

use std::fmt;

use crate::message::CacheMsg;
use crate::message::CacheMsg::{Tell, TellOld, TellTS, TellOldTS};

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
        Entry { time, ttl, expired: value == "", value: value.into() }
    }

    #[allow(dead_code)]
    pub fn new_owned(time: f64, ttl: f64, value: String) -> Entry {
        Entry { time, ttl, expired: value == "", value }
    }

    /// Mark the Entry as expired.
    pub fn expired(mut self) -> Entry {
        self.expired = true;
        self
    }

    /// Convert the Entry into a Tell-type CacheMsg.
    pub fn to_msg<'a>(&'a self, key: &'a str, with_ts: bool) -> CacheMsg<'a> {
        if with_ts {
            if self.expired {
                TellOldTS { key, val: &self.value, time: self.time, ttl: self.ttl }
            } else {
                TellTS { key, val: &self.value,
                         time: self.time, ttl: self.ttl, no_store: false }
            }
        } else if self.expired {
            TellOld { key, val: &self.value }
        } else {
            Tell { key, val: &self.value, no_store: false }
        }
    }

    /// Return a Tell-type CacheMsg that represents a missing entry.
    pub fn no_msg(key: &str, with_ts: bool) -> CacheMsg {
        if with_ts {
            TellOldTS { key, val: "", time: 0., ttl: 0. }
        } else {
            TellOld { key, val: "" }
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

/// Entry associated with a key and a cache for interpolated protocol messages.
///
/// This is used by updaters that have to send the same update string to
/// potentially a lot of clients.
pub struct UpdaterEntry {
    key: String,
    val: Entry,
    cache: (Option<String>, Option<String>),
}

impl UpdaterEntry {
    pub fn new(key: String, val: &Entry) -> UpdaterEntry {
        UpdaterEntry { key, val: val.clone(), cache: (None, None) }
    }

    /// Check if the entry matches a subscription substring.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Get the interpolated message, use the cache if possible.
    pub fn get_msg(&mut self, with_ts: bool) -> &str {
        let cached = if with_ts { &mut self.cache.0 } else { &mut self.cache.1 };
        if cached.is_none() {
            *cached = Some(self.val.to_msg(&self.key, with_ts).to_string());
        }
        cached.as_ref().unwrap()
    }
}

impl fmt::Debug for UpdaterEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}={:?}", self.key, self.val)
    }
}
