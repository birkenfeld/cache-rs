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
//! Flat-file database store.

use std::mem;
use std::fs::{File, OpenOptions, read_dir, remove_file, hard_link, remove_dir_all};
use std::io::{self, BufRead, BufReader, Seek, SeekFrom, Write};
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use log::{info, warn};
use time::{OffsetDateTime, Time, Duration};
use hashbrown::HashMap;
use mlzutil::fs::ensure_dir;
use mlzutil::time::{to_timespec, to_timefloat};

use crate::database::{self, EntryMap};
use crate::entry::{Entry, split_key};

/// Get the store subdir for a certain day.
pub fn day_path(day: OffsetDateTime) -> String {
    format!("{:04}/{:02}-{:02}", day.year(), day.month() as u8, day.day())
}

fn thisday() -> OffsetDateTime {
    OffsetDateTime::now_local().unwrap().replace_time(Time::MIDNIGHT)
}

/// Get all days between two timestamps.
pub fn all_days(from: f64, to: f64) -> Vec<String> {
    let mut res = Vec::new();
    let to = to_timespec(to);
    let mut tm = to_timespec(from);
    while tm < to {
        res.push(day_path(tm));
        tm += Duration::days(1);
    }
    res
}

impl Entry {
    /// Write the Entry to a store file.
    fn to_file(&self, subkey: &str, fp: &mut File) -> io::Result<()> {
        let ttlsign = if self.ttl > 0. || self.expired { "-" } else { "+" };
        writeln!(fp, "{}\t{}\t{}\t{}",
                 subkey, self.time, ttlsign,
                 if self.expired { "-" } else { &self.value })
    }
}

/// Represents the flat-file backend store.
pub struct Store {
    /// Root path for cache file storage.
    storepath:    PathBuf,
    /// YYYY/MM-DD path component.
    ymd_path:     String,
    /// Map of store files, by categories.
    files:        HashMap<String, File>,
    /// Last and next midnight as floating timestamps.
    midnights:    (f64, f64),
}

impl Store {
    pub fn new(storepath: PathBuf) -> Store {
        let thisday = thisday();
        Store {
            storepath,
            files: HashMap::default(),
            midnights: (to_timefloat(thisday),
                        to_timefloat(thisday + Duration::days(1))),
            ymd_path: day_path(thisday),
        }
    }
}

impl database::Store for Store {
    /// Clear DB by removing all store files.
    fn clear(&mut self) -> io::Result<()> {
        if self.storepath.is_dir() {
            remove_dir_all(&self.storepath)?;
            ensure_dir(&self.storepath)?;
            self.set_lastday();
        }
        Ok(())
    }

    /// Load the latest DB entries from the store.
    fn load_latest(&mut self, entry_map: &mut EntryMap) -> io::Result<()> {
        ensure_dir(&self.storepath)?;
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
            for dentry in dentry_iter.flatten() {
                if !dentry.metadata().map(|m| m.is_file()).unwrap_or(false) {
                    continue;
                }
                let path = dentry.path();
                match self.load_one_file(&path) {
                    Ok(map) => {
                        let catname = path.file_name().unwrap().to_string_lossy()
                                                               .replace('-', "/");
                        nentries += map.len();
                        nfiles += 1;
                        entry_map.insert(catname, map);
                    },
                    Err(err) => {
                        warn!("could not read data from store file {:?}: {}",
                              path.display(), err);
                    }
                }
            }
        }
        info!("db: read {} entries from {} storefiles", nentries, nfiles);
        if need_rollover {
            self.rollover(entry_map)
        } else {
            Ok(())
        }
    }

    /// Roll over store files when needed.
    fn tell_hook(&mut self, entry: &Entry, entry_map: &mut EntryMap) -> io::Result<()> {
        if entry.time >= self.midnights.1 {
            self.rollover(entry_map)?;
        }
        Ok(())
    }

    /// Save new key-value entry to the right file.
    fn save(&mut self, cat: &str, subkey: &str, entry: &Entry) -> io::Result<()> {
        if !self.files.contains_key(cat) {
            let fp = self.create_fd(cat)?;
            self.files.insert(cat.into(), fp);
        }
        let fp = self.files.get_mut(cat).unwrap();
        entry.to_file(subkey, fp)
    }

    /// Send history of a key to client.
    fn query_history(&mut self, key: &str, from: f64, to: f64, send: &mut dyn FnMut(f64, &str)) {
        let (catname, subkey) = split_key(key);
        let paths = if from >= self.midnights.0 {
            vec![self.ymd_path.clone()]
        } else {
            all_days(from, to)
        };
        for path in paths {
            if let Err(e) = self.read_history(&path, catname, subkey, from, to, send) {
                warn!("could not read histfile for {}/{}: {}", path, catname, e);
            }
        }
    }
}

impl Store {
    /// Load keys from a single file for category "catname".
    fn load_one_file(&self, filename: &Path) -> io::Result<HashMap<String, Entry>> {
        let fp = File::open(filename)?;
        let mut map = HashMap::default();
        Self::read_storefile(fp, |parts| {
            let subkey = parts[0];
            if parts[2] == "+" {
                // value is non-expiring: we can take it as valid
                if let Ok(v) = parts[1].parse() {
                    map.insert(subkey.into(), Entry::new(v, 0., parts[3]));
                }
            } else if parts[3] != "-" {
                // value was expiring but is not empty: take it as expired
                if let Ok(v) = parts[1].parse() {
                    map.insert(subkey.into(), Entry::new(v, 0., parts[3]).expired());
                }
            } else if let Some(entry) = map.get_mut(subkey) {
                // value is empty: be sure to mark any current value as expired
                entry.expired = true;
            }
        });
        Ok(map)
    }

    /// Set the "lastday" symlink to the latest yyyy/mm-dd directory.
    fn set_lastday(&self) {
        let path = self.storepath.join("lastday");
        let _ = remove_file(&path);
        if let Err(e) = symlink(&self.ymd_path, &path) {
            warn!("could not set \"lastday\" symlink: {}", e);
        }
    }

    /// Roll over all store files after midnight has passed.
    fn rollover(&mut self, entry_map: &mut EntryMap) -> io::Result<()> {
        info!("midnight passed, rolling over data files...");
        let thisday = thisday();
        self.midnights = (to_timefloat(thisday),
                          to_timefloat(thisday + Duration::days(1)));
        self.ymd_path = day_path(thisday);
        let old_files = mem::take(&mut self.files);
        for (catname, fp) in old_files {
            drop(fp);
            let submap = entry_map.get(&catname).unwrap();
            let mut new_fp = self.create_fd(&catname)?;
            for (subkey, entry) in submap {
                if !entry.expired {
                    entry.to_file(subkey, &mut new_fp)?;
                }
            }
        }
        self.set_lastday();
        Ok(())
    }

    /// Create a new file for a category.
    fn create_fd(&self, catname: &str) -> io::Result<File> {
        let safe_catname = catname.replace('/', "-");
        let subpath = self.storepath.join(&self.ymd_path);
        let linkfile = self.storepath.join(&safe_catname).join(&self.ymd_path);
        ensure_dir(&subpath)?;
        let file = subpath.join(safe_catname);
        let mut fp = OpenOptions::new().create(true).append(true).open(&file)?;
        if fp.seek(SeekFrom::Current(0))? == 0 {
            fp.write_all(b"# NICOS cache store file v2\n")?;
        }
        ensure_dir(linkfile.parent().unwrap())?;
        if !linkfile.is_file() {
            hard_link(file, linkfile)?;
        }
        Ok(fp)
    }

    /// Read history for a given subkey from a file.
    fn read_history<F>(&self, path: &str, catname: &str, subkey: &str,
                       from: f64, to: f64, send: &mut F) -> io::Result<()>
    where F: FnMut(f64, &str) + ?Sized
    {
        let catname = catname.replace('/', "-");
        let path = self.storepath.join(path).join(catname);
        if path.is_file() {
            let fp = File::open(path)?;
            Self::read_storefile(fp, |parts| {
                if parts[0] == subkey {
                    let time = parts[1].parse().unwrap_or(0.);
                    if from <= time && time <= to {
                        send(time, if parts[3] == "-" { "" } else { parts[3] });
                    }
                }
            });
        }
        Ok(())
    }

    /// Read a store file and call the closure for each entry.
    fn read_storefile<F: FnMut(Vec<&str>)>(fp: File, mut f: F) {
        let mut reader = BufReader::new(fp);
        let mut line = String::new();
        while let Ok(n) = reader.read_line(&mut line) {
            if n == 0 {
                break;
            }
            let parts = line.trim().split('\t').collect::<Vec<_>>();
            if parts.len() == 4 {
                f(parts);
            }
            line.clear();
        }
    }
}
