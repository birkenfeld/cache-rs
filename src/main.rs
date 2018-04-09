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

#![feature(nll)]

#[macro_use]
extern crate log;
extern crate mlzlog;
extern crate time;
extern crate fnv;
extern crate regex;
extern crate memchr;
extern crate mlzutil;
#[macro_use]
extern crate structopt;
#[macro_use]
extern crate lazy_static;
extern crate aho_corasick;
extern crate parking_lot;
extern crate daemonize;
extern crate chan_signal;
extern crate crossbeam_channel;
#[cfg(feature = "postgres")]
extern crate postgres;

mod entry;
mod database;
mod store_flat;
#[cfg(feature = "postgres")]
mod store_pgsql;
mod handler;
mod message;
mod server;

use std::path::PathBuf;
use structopt::{StructOpt, clap};

#[derive(StructOpt)]
#[structopt(author = "")]
#[structopt(about = "A Rust implementation of the NICOS cache.")]
#[structopt(raw(setting = "clap::AppSettings::UnifiedHelpMessage"))]
#[structopt(raw(setting = "clap::AppSettings::DeriveDisplayOrder"))]
struct Options {
    #[structopt(long="bind", default_value="127.0.0.1:14869", help="Bind address (host:port)")]
    bind_addr: String,
    #[structopt(long="store", default_value="data", help="Store path or URI")]
    store_path: String,
    #[structopt(long="log", default_value="log", help="Logging path")]
    log_path: PathBuf,
    #[structopt(long="pid", default_value="pid", help="PID path")]
    pid_path: PathBuf,
    #[structopt(short="v", help="Debug logging output?")]
    verbose: bool,
    #[structopt(long="clear", help="Clear the database on startup?")]
    clear: bool,
    #[structopt(short="d", help="Daemonize?")]
    daemonize: bool,
    #[structopt(long="user", help="User name for daemon")]
    user: Option<String>,
    #[structopt(long="group", help="Group name for daemon")]
    group: Option<String>,
    #[structopt(raw(hidden="true"))]
    _dummy: Option<String>,
}

fn main() {
    let args = Options::from_args();
    let log_path = mlzutil::fs::abspath(args.log_path);
    let pid_path = mlzutil::fs::abspath(args.pid_path);
    if args.daemonize {
        let mut daemon = daemonize::Daemonize::new();
        if let Some(user) = args.user {
            daemon = daemon.user(&*user);
        }
        if let Some(group) = args.group {
            daemon = daemon.group(&*group);
        }
        if let Err(err) = daemon.start() {
            eprintln!("could not daemonize process: {}", err);
        }
    }
    if let Err(err) = mlzlog::init(Some(log_path), "cache_rs", false,
                                   args.verbose, !args.daemonize) {
        eprintln!("could not initialize logging: {}", err);
    }
    let store_path = server::StorePath::parse(&args.store_path).unwrap_or_else(|err| {
        error!("invalid store path: {}", err);
        std::process::exit(1);
    });
    if let Err(err) = mlzutil::fs::write_pidfile(&pid_path, "cache_rs") {
        error!("could not write PID file: {}", err);
    }

    // handle SIGINT and SIGTERM
    let signal_chan = chan_signal::notify(&[chan_signal::Signal::INT,
                                            chan_signal::Signal::TERM]);

    let server = server::Server::new(store_path, args.clear)
        .unwrap_or_else(|_| std::process::exit(1));
    info!("starting server on {}...", args.bind_addr);
    if let Err(err) = server.start(&args.bind_addr) {
        error!("could not initialize server: {}", err);
    }

    // wait for a signal to finish
    signal_chan.recv().unwrap();
    info!("quitting...");
    mlzutil::fs::remove_pidfile(pid_path, "cache_rs");
}
