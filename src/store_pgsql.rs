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
//! PostgreSQL-backed database store.

use std::io;
use std::collections::HashMap;

use postgres::{self, Connection, SslMode};

use database::{self, EntryMap};
use entry::{Entry, split_key, construct_key};

/// Represents the Postgres backend store.
pub struct Store {
    /// Postgres connection.
    connection:   Connection,
}

impl Store {
    pub fn new(url: &str) -> Result<Store, postgres::error::ConnectError> {
        // XXX: create table if not present? check schema?
        Ok(Store { connection: try!(Connection::connect(url, SslMode::None)) })
    }
}

impl database::Store for Store {
    /// Clear all DB values.
    fn clear(&mut self) -> io::Result<()> {
        try!(self.connection.execute("DELETE FROM values;", &[]));
        Ok(())
    }

    /// Load the latest DB entries.
    fn load_latest(&mut self, entry_map: &mut EntryMap) -> io::Result<()> {
        let query = "WITH max_ts AS \
                     ( SELECT key, MAX(time) \"time\" FROM values GROUP BY key ) \
                     SELECT values.key, values.value, values.time, values.expires \
                     FROM values, max_ts \
                     WHERE max_ts.key = values.key AND max_ts.time = values.time;";
        let result = try!(self.connection.query(query, &[]));
        let num_rows = result.len();
        for row in &result {
            let key: String = row.get(0);
            let (cat, subkey) = split_key(&key);
            let submap = entry_map.entry(cat.into()).or_insert_with(HashMap::new);
            let mut entry = Entry::new_owned(row.get(2), 0., row.get(1));
            if row.get(3) {
                entry = entry.expired();
            }
            submap.insert(subkey.into(), entry);
        }
        info!("db: read {} entries from SQL database", num_rows);
        Ok(())
    }

    /// Nothing to do here.
    fn tell_hook(&mut self, _: &Entry, _: &mut EntryMap) -> io::Result<()> {
        Ok(())
    }

    /// Insert a new key-value entry.
    fn save(&mut self, catname: &str, subkey: &str, entry: &Entry) -> io::Result<()> {
        let query = "INSERT INTO values ( key, value, time, expires ) \
                     VALUES ( $1, $2, $3, $4 );";
        let key = construct_key(catname, subkey);
        let expires = entry.ttl > 0. || entry.expired;
        try!(self.connection.execute(query, &[&key, &entry.value, &entry.time, &expires]));
        Ok(())
    }

    /// Send history to client.
    fn query_history(&mut self, key: &str, from: f64, to: f64, send: &mut FnMut(f64, &str)) {
        let query = "SELECT values.key, values.value, values.time FROM values \
                     WHERE key = $1 AND time >= $2 AND time <= $3 ORDER BY time;";
        if let Ok(result) = self.connection.query(query, &[&key, &from, &to]) {
            for row in &result {
                let val: String = row.get(1);
                send(row.get(2), &val);
            }
        }
    }
}
