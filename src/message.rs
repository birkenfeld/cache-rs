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

use std::borrow::Cow;

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

pub type Cstr<'a> = Cow<'a, str>;

/// An algebraic data type that represents any message (line) that can be sent
/// over the network in the cache protocol.
///
/// String entries here are Cow<str> because that way we can reuse slices from
/// the original string coming from the network.
#[derive(Debug)]
pub enum CacheMsg<'a> {
    /// quit a.k.a. empty line
    Quit,
    /// value update without timestamp
    Tell      { key: Cstr<'a>, val: Cstr<'a>, no_store: bool },
    /// value update with timestamp
    TellTS    { key: Cstr<'a>, val: Cstr<'a>, time: f64, ttl: f64, no_store: bool },
    /// expired value update without timestamp
    TellOld   { key: Cstr<'a>, val: Cstr<'a> },
    /// expired value update with timestamp
    TellOldTS { key: Cstr<'a>, val: Cstr<'a>, time: f64, ttl: f64 },
    /// query for a single key
    Ask       { key: Cstr<'a>, with_ts: bool },
    /// query for multiple keys with a wildcard
    AskWild   { key_wc: Cstr<'a>, with_ts: bool },
    /// query for history of a single key
    AskHist   { key: Cstr<'a>, from: f64, delta: f64 },
    /// subscription to a key substring
    Subscribe { key_sub: Cstr<'a>, with_ts: bool },
    /// unsubscription
    Unsub     { key_sub: Cstr<'a>, with_ts: bool },
    /// set or delete of a prefix rewrite
    Rewrite   { new_prefix: Cstr<'a>, old_prefix: Cstr<'a> },
    /// lock request
    Lock      { key: Cstr<'a>, client: Cstr<'a>, time: f64, ttl: f64 },
    /// unlock request
    Unlock    { key: Cstr<'a>, client: Cstr<'a> },
    /// result of a lock or unlock request
    LockRes   { key: Cstr<'a>, client: Cstr<'a> },
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
                    let real_key = if no_store { &key[0..key.len() - 1] } else { &key };
                    if has_tsop {
                        Some(TellTS { key: real_key.into(), val: val.into(),
                                      time: t1, ttl: dt, no_store: no_store })
                    } else {
                        Some(Tell { key: real_key.into(), val: val.into(),
                                    no_store: no_store })
                    }},
                "!" =>
                    if has_tsop {
                        Some(TellOldTS { key: key.into(), val: val.into(), time: t1, ttl: dt })
                    } else {
                        Some(TellOld { key: key.into(), val: val.into() })
                    },
                "?" =>
                    if has_tsop && dt != 0. {
                        Some(AskHist { key: key.into(), from: t1, delta: dt })
                    } else {
                        Some(Ask { key: key.into(), with_ts: has_tsop })
                    },
                "*" =>  Some(AskWild { key_wc: key.into(), with_ts: has_tsop }),
                ":" =>  Some(Subscribe { key_sub: key.into(), with_ts: has_tsop }),
                "|" =>  Some(Unsub { key_sub: key.into(), with_ts: has_tsop }),
                "$" => {
                    let client = &val[1..];
                    if &val[0..1] == "+" {
                        Some(Lock { key: key.into(), client: client.into(), time: t1, ttl: dt })
                    } else if &val[0..1] == "-" {
                        Some(Unlock { key: key.into(), client: client.into() })
                    } else {
                        Some(LockRes { key: key.into(), client: val.into() })
                    }},
                "~" =>  Some(Rewrite { new_prefix: key.into(), old_prefix: val.into() }),
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
            Tell { ref key, ref val, no_store } =>
                format!("{}{}={}\n", key, if no_store { "#" } else { "" }, val),
            TellTS { ref key, ref val, time, ttl, no_store } =>
                if ttl > 0. {
                    format!("{}+{}@{}{}={}\n", time, ttl, key, if no_store { "#" } else { "" }, val)
                } else {
                    format!("{}@{}{}={}\n", time, key, if no_store { "#" } else { "" }, val)
                },
            TellOld { ref key, ref val } =>
                format!("{}!{}\n", key, val),
            TellOldTS { ref key, ref val, time, ttl } =>
                format!("{}+{}@{}!{}\n", time, ttl, key, val),
            Ask { ref key, with_ts } =>
                if with_ts {
                    format!("@{}?\n", key)
                } else {
                    format!("{}?\n", key)
                },
            AskWild { ref key_wc, with_ts } =>
                if with_ts {
                    format!("@{}*\n", key_wc)
                } else {
                    format!("{}*\n", key_wc)
                },
            AskHist { ref key, from, delta } =>
                format!("{}+{}@{}?\n", from, delta, key),
            Subscribe { ref key_sub, with_ts } =>
                if with_ts {
                    format!("@{}:\n", key_sub)
                } else {
                    format!("{}:\n", key_sub)
                },
            Unsub { ref key_sub, with_ts } =>
                if with_ts {
                    format!("@{}|\n", key_sub)
                } else {
                    format!("{}|\n", key_sub)
                },
            Lock { ref key, ref client, time, ttl } => {
                format!("{}+{}@{}$+{}\n", time, ttl, key, client)},
            Unlock { ref key, ref client } => {
                format!("{}$-{}\n", key, client)},
            LockRes { ref key, ref client } => {
                format!("{}${}\n", key, client)},
            Rewrite { ref new_prefix, ref old_prefix } =>
                format!("{}~{}\n", new_prefix, old_prefix),
        }
    }
}
