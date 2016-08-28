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
//! The main entry point and crate definitions.

#[macro_use]
extern crate log;
extern crate log4rs;
extern crate regex;
extern crate time;
extern crate docopt;
extern crate ansi_term;
#[macro_use]
extern crate lazy_static;
extern crate daemonize;
extern crate rustc_serialize;
extern crate chan_signal;
extern crate postgres;

use chan_signal::Signal;

mod entry;
mod database;
mod store_flat;
mod store_pgsql;
mod handler;
mod message;
mod server;
mod logging;
mod util;


const USAGE: &'static str = "
Usage: cache-rs [options]
       cache-rs --help

A Rust implementation of the NICOS cache.

Options:

    -v                 Debug logging output?
    --bind ADDR        Bind address (host:port) [default: 127.0.0.1:14869]
    --store STOREPATH  Store path [default: data]
    --log LOGPATH      Logging path [default: log]
    --pid PIDPATH      PID path [default: pid]
    -d                 Daemonize?
    --user USER        User name for daemon
    --group GROUP      Group name for daemon
    --clear            Clear the database on startup?
";


#[derive(Debug, RustcDecodable)]
struct Args {
    flag_v: bool,
    flag_bind: String,
    flag_store: String,
    flag_log: String,
    flag_pid: String,
    flag_d: bool,
    flag_user: Option<String>,
    flag_group: Option<String>,
    flag_clear: bool,
}

fn main() {
    let args: Args = docopt::Docopt::new(USAGE).unwrap().decode().unwrap_or_else(|e| e.exit());

    let log_path = std::path::Path::new(&args.flag_log);
    if let Err(err) = logging::init(&log_path, "cache-rs",
                                    args.flag_v, !args.flag_d) {
        println!("could not initialize logging: {}", err);
    }
    if args.flag_d {
        let mut daemon = daemonize::Daemonize::new();
        if let Some(user) = args.flag_user {
            daemon = daemon.user(user.as_str());
        }
        if let Some(group) = args.flag_group {
            daemon = daemon.group(group.as_str());
        }
        if let Err(err) = daemon.start() {
            error!("could not daemonize process: {}", err);
        }
    }
    if let Err(err) = util::write_pidfile(&args.flag_pid) {
        error!("could not write PID file: {}", err);
    }

    // handle SIGINT and SIGTERM
    let signal_chan = chan_signal::notify(&[Signal::INT, Signal::TERM]);

    let server = server::Server::new(&args.flag_store, args.flag_clear);
    info!("starting server on {}...", args.flag_bind);
    if let Err(err) = server.start(&args.flag_bind) {
        error!("could not initialize server: {}", err);
    }

    // wait for a signal to finish
    signal_chan.recv().unwrap();
    info!("quitting...");
    util::remove_pidfile(&args.flag_pid);
}
