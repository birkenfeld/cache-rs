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
//! Flat-file database store.

use std::mem;
use std::fs::{File, read_dir, remove_file, hard_link, remove_dir_all};
use std::io::{self, BufRead, BufReader, Seek, SeekFrom, Write};
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use fnv::FnvHashMap as HashMap;
use time::{now, Tm, Duration};

use database::{self, EntryMap};
use entry::{Entry, split_key};
use util::{ensure_dir, to_timefloat, day_path, all_days, open_file};

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
        let thisday = Tm { tm_hour: 0, tm_min: 0, tm_sec: 0, tm_nsec: 0, ..now() };
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
            for dentry in dentry_iter {
                if let Ok(dentry) = dentry {
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
    fn query_history(&mut self, key: &str, from: f64, to: f64, send: &mut FnMut(f64, &str)) {
        let (catname, subkey) = split_key(key);
        let paths = if from >= self.midnights.0 {
            vec![self.ymd_path.clone()]
        } else {
            all_days(from, to)
        };
        for path in paths {
            match self.read_history(&path, catname, subkey) {
                Err(e)   => warn!("could not read histfile for {}/{}: {}", path, catname, e),
                Ok(msgs) => for (time, val) in msgs {
                    if from <= time && time <= to {
                        send(time, &val);
                    }
                },
            }
        }
    }
}

impl Store {
    /// Load keys from a single file for category "catname".
    fn load_one_file(&self, filename: &Path) -> io::Result<HashMap<String, Entry>> {
        let fp = open_file(filename, "r")?;
        let mut reader = BufReader::new(fp);
        let mut line = String::new();
        let mut map = HashMap::default();
        while let Ok(n) = reader.read_line(&mut line) {
            if n == 0 {
                break;
            } else {
                let parts = line.trim().split('\t').collect::<Vec<_>>();
                if parts.len() == 4 {
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
                }
            }
            line.clear();
        }
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
        let thisday = Tm { tm_hour: 0, tm_min: 0, tm_sec: 0, tm_nsec: 0, ..now() };
        self.midnights = (to_timefloat(thisday),
                          to_timefloat(thisday + Duration::days(1)));
        self.ymd_path = day_path(thisday);
        let old_files = mem::replace(&mut self.files, HashMap::default());
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
        let mut fp = open_file(&file, "wa")?;
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
    fn read_history(&self, path: &str, catname: &str, subkey: &str)
                    -> io::Result<Vec<(f64, String)>> {
        let catname = catname.replace('/', "-");
        let path = self.storepath.join(path).join(catname);
        let mut res = Vec::new();
        if !path.is_file() {
            return Ok(res)
        }
        let fp = open_file(path, "r")?;
        let reader = BufReader::new(fp);
        for line in reader.lines() {
            if let Ok(line) = line {
                let parts = line.trim().split('\t').collect::<Vec<_>>();
                if parts.len() == 4 && parts[0] == subkey {
                    let val = if parts[3] == "-" { "" } else { parts[3] };
                    res.push((parts[1].parse().unwrap_or(0.), val.into()));
                }
            }
        }
        Ok(res)
    }
}
