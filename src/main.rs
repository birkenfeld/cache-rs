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
//! The main entry point and crate definitions.

mod entry;
mod database;
mod store_flat;
#[cfg(feature = "postgres")]
mod store_pgsql;
mod handler;
mod message;
mod server;

use log::{info, error};
use structopt::{StructOpt, clap};
use signal_hook::iterator::Signals;

#[derive(StructOpt)]
#[structopt(about = "A Rust implementation of the NICOS cache.")]
#[structopt(setting = clap::AppSettings::UnifiedHelpMessage)]
#[structopt(setting = clap::AppSettings::DeriveDisplayOrder)]
struct Options {
    #[structopt(long="bind", default_value="127.0.0.1:14869", help="Bind address (host:port)")]
    bind_addr: String,
    #[structopt(long="store", default_value="data", help="Store path or URI")]
    store_path: String,
    #[structopt(long="log", default_value="log", help="Logging path")]
    log_path: String,
    #[structopt(long="pid", default_value="pid", help="PID path")]
    pid_path: String,
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
    #[structopt(hidden=true)]
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

    let server = server::Server::new(store_path, args.clear)
        .unwrap_or_else(|_| std::process::exit(1));
    info!("starting server on {}...", args.bind_addr);
    if let Err(err) = server.start(&args.bind_addr) {
        error!("could not initialize server: {}", err);
    }

    // wait for a signal to finish
    Signals::new(&[libc::SIGINT, libc::SIGTERM]).unwrap().wait();
    info!("quitting...");
    mlzutil::fs::remove_pidfile(pid_path, "cache_rs");
}
