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
//! This module contains the definition of a protocol message, along with tools
//! to parse and string-format it.

use regex::Regex;

use util::localtime;


lazy_static! {
    static ref MSG_RE: Regex = Regex::new(r#"(?x)
    ^ (?:
      \s* (?P<time>\d+\.?\d*)?                 # timestamp
      \s* (?P<ttlop>[+-]?)                     # ttl operator
      \s* (?P<ttl>\d+\.?\d*(?:[eE][+-]?\d+)?)? # ttl
      \s* (?P<tsop>@)                          # timestamp mark
    )?
    \s* (?P<key>[^=!?:*$]*?)                   # key
    \s* (?P<op>[=!?:|*$~])                     # operator
    \s* (?P<value>[^\r\n]*?)                   # value
    \s* $
    "#).unwrap();
}

/// An algebraic data type that represents any message (line) that can be sent
/// over the network in the cache protocol.
///
/// String entries here are borrowed because we just reuse slices from the
/// original string coming from the network.
#[derive(Debug)]
pub enum CacheMsg<'a> {
    /// quit a.k.a. empty line
    Quit,
    /// value update without timestamp
    Tell      { key: &'a str, val: &'a str, no_store: bool },
    /// value update with timestamp
    TellTS    { key: &'a str, val: &'a str, time: f64, ttl: f64, no_store: bool },
    /// expired value update without timestamp
    TellOld   { key: &'a str, val: &'a str },
    /// expired value update with timestamp
    TellOldTS { key: &'a str, val: &'a str, time: f64, ttl: f64 },
    /// query for a single key
    Ask       { key: &'a str, with_ts: bool },
    /// query for multiple keys with a wildcard
    AskWild   { key: &'a str, with_ts: bool },
    /// query for history of a single key
    AskHist   { key: &'a str, from: f64, delta: f64 },
    /// subscription to a key substring
    Subscribe { key: &'a str, with_ts: bool },
    /// unsubscription
    Unsub     { key: &'a str, with_ts: bool },
    /// lock request
    Lock      { key: &'a str, client: &'a str, time: f64, ttl: f64 },
    /// unlock request
    Unlock    { key: &'a str, client: &'a str },
    /// result of a lock or unlock request
    LockRes   { key: &'a str, client: &'a str },
    /// set or delete of a prefix rewrite
    Rewrite   { new_prefix: &'a str, old_prefix: &'a str },
}

use self::CacheMsg::*;

impl<'a> CacheMsg<'a> {
    /// Parse a String containing a cache message.
    ///
    /// This matches a regular expression, and then creates a `CacheMsg` if successful.
    pub fn parse(line: &str) -> Option<CacheMsg> {
        if let Some(captures) = MSG_RE.captures(line) {
            let t1;
            let mut dt = 0.;
            let has_tsop = captures.name("tsop").is_some();
            if has_tsop {
                t1 = captures.name("time").and_then(|m| m.as_str().parse().ok()).unwrap_or_else(localtime);
                dt = captures.name("ttl").and_then(|m| m.as_str().parse().ok()).unwrap_or(0.);
                if captures.name("ttlop").map_or("", |m| m.as_str()) == "-" {
                    dt -= t1;
                }
            } else {
                t1 = localtime();
            }
            let key = captures.name("key").expect("no key in match?!").as_str();
            let val = captures.name("value").map_or("", |m| m.as_str());
            match captures.name("op").expect("no op in match?!").as_str() {
                "=" => {
                    // handle the "no store" flag, a "#" after the key name
                    let no_store = key.ends_with('#');
                    let key = if no_store { &key[0..key.len() - 1] } else { key };
                    if has_tsop {
                        Some(TellTS { key, val, time: t1, ttl: dt, no_store })
                    } else {
                        Some(Tell { key, val, no_store })
                    }},
                "!" =>
                    if has_tsop {
                        Some(TellOldTS { key, val, time: t1, ttl: dt })
                    } else {
                        Some(TellOld { key, val })
                    },
                "?" =>
                    if has_tsop && dt != 0. {
                        Some(AskHist { key, from: t1, delta: dt })
                    } else {
                        Some(Ask { key, with_ts: has_tsop })
                    },
                "*" =>  Some(AskWild { key, with_ts: has_tsop }),
                ":" =>  Some(Subscribe { key, with_ts: has_tsop }),
                "|" =>  Some(Unsub { key, with_ts: has_tsop }),
                "$" => {
                    let client = &val[1..];
                    if &val[0..1] == "+" {
                        Some(Lock { key, client, time: t1, ttl: dt })
                    } else if &val[0..1] == "-" {
                        Some(Unlock { key, client })
                    } else {
                        Some(LockRes { key, client: val })
                    }},
                "~" =>  Some(Rewrite { new_prefix: key, old_prefix: val }),
                _   =>  None,
            }
        } else if line.trim() == "" {
            Some(Quit)
        } else {
            None
        }
    }
}

/// "Serialize" a `CacheMsg` back to a String.
///
/// Not all messages are actually used for stringification, but this is also
/// nice for debugging purposes.
impl<'a> ToString for CacheMsg<'a> {
    fn to_string(&self) -> String {
        match *self {
            Quit => String::from("\n"),
            Tell { key, val, no_store } =>
                format!("{}{}={}\n", key, if no_store { "#" } else { "" }, val),
            TellTS { key, val, time, ttl, no_store } =>
                if ttl > 0. {
                    format!("{}+{}@{}{}={}\n", time, ttl, key, if no_store { "#" } else { "" }, val)
                } else {
                    format!("{}@{}{}={}\n", time, key, if no_store { "#" } else { "" }, val)
                },
            TellOld { key, val } =>
                format!("{}!{}\n", key, val),
            TellOldTS { key, val, time, ttl } =>
                format!("{}+{}@{}!{}\n", time, ttl, key, val),
            Ask { key, with_ts } =>
                if with_ts {
                    format!("@{}?\n", key)
                } else {
                    format!("{}?\n", key)
                },
            AskWild { key, with_ts } =>
                if with_ts {
                    format!("@{}*\n", key)
                } else {
                    format!("{}*\n", key)
                },
            AskHist { key, from, delta } =>
                format!("{}+{}@{}?\n", from, delta, key),
            Subscribe { key, with_ts } =>
                if with_ts {
                    format!("@{}:\n", key)
                } else {
                    format!("{}:\n", key)
                },
            Unsub { key, with_ts } =>
                if with_ts {
                    format!("@{}|\n", key)
                } else {
                    format!("{}|\n", key)
                },
            Lock { key, client, time, ttl } => {
                format!("{}+{}@{}$+{}\n", time, ttl, key, client)},
            Unlock { key, client } => {
                format!("{}$-{}\n", key, client)},
            LockRes { key, client } => {
                format!("{}${}\n", key, client)},
            Rewrite { new_prefix, old_prefix } =>
                format!("{}~{}\n", new_prefix, old_prefix),
        }
    }
}
