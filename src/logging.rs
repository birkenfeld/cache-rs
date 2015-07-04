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
//! Additions to the log4rs library for logging.

use std::error::Error;
use std::fs::{ File, remove_file };
use std::io::{ self, Stdout, Write, BufWriter };
use std::os::unix::fs::symlink;
use std::path::{ Path, PathBuf };

use time::{ Timespec, Tm, Duration, get_time, now, strftime };
use log::{ LogLevel, LogRecord, LogLevelFilter };
use log4rs::{ self, Append };
use log4rs::config::{ Config, Root, Appender };
use log4rs::pattern::PatternLayout;
use ansi_term::Colour::{ Red, White, Purple };

use util::{ ensure_dir, open_file };


struct ConsoleAppender {
    stdout: Stdout,
}

impl ConsoleAppender {
    pub fn new() -> ConsoleAppender {
        ConsoleAppender { stdout: io::stdout() }
    }
}

impl Append for ConsoleAppender {
    fn append(&mut self, record: &LogRecord) -> Result<(), Box<Error>> {
        let mut stdout = self.stdout.lock();
        let time_str = strftime("[%H:%M:%S]", &now()).unwrap();
        let time = White.paint(&time_str);
        let msg = format!("{}", record.args());
        try!(match record.level() {
            LogLevel::Error => write!(stdout, "{} ERROR: {}\n", time, Red.bold().paint(&msg)),
            LogLevel::Warn  => write!(stdout, "{} WARNING: {}\n", time, Purple.bold().paint(&msg)),
            LogLevel::Debug => write!(stdout, "{} {}\n", time, White.paint(&msg)),
            _               => write!(stdout, "{} {}\n", time, msg),
        });
        try!(stdout.flush());
        Ok(())
    }
}

struct RollingFileAppender {
    dir:     PathBuf,
    prefix:  String,
    link_fn: PathBuf,
    file:    Option<BufWriter<File>>,
    roll_at: Timespec,
    pattern: PatternLayout,
}

impl RollingFileAppender {
    pub fn new(dir: &Path, prefix: &str) -> RollingFileAppender {
        let thisday = Tm { tm_hour: 0, tm_min: 0, tm_sec: 0, tm_nsec: 0, ..now() };
        let roll_at = (thisday + Duration::days(1)).to_timespec();
        let pattern = PatternLayout::new("%d{%H:%M:%S,%f} : %l : %m").unwrap();
        let link_fn = dir.join("current");
        RollingFileAppender { dir: dir.to_path_buf(), prefix: prefix.to_owned(),
                              link_fn: link_fn, file: None, roll_at: roll_at,
                              pattern: pattern }
    }

    fn rollover(&mut self) -> io::Result<()> {
        self.file.take();  // will drop the file if open
        let time = strftime("%Y-%m-%d", &now()).unwrap();
        let full = format!("{}-{}.log", self.prefix, time);
        let new_fn = self.dir.join(full);
        let fp = try!(open_file(&new_fn, "wa"));
        self.file = Some(BufWriter::new(fp));
        let _ = remove_file(&self.link_fn);
        let _ = symlink(&new_fn.file_name().unwrap(), &self.link_fn);
        self.roll_at = self.roll_at + Duration::days(1);
        Ok(())
    }
}

impl Append for RollingFileAppender {
    fn append(&mut self, record: &LogRecord) -> Result<(), Box<Error>> {
        if self.file.is_none() || get_time() >= self.roll_at {
            try!(self.rollover());
        }
        let fp = self.file.as_mut().unwrap();
        try!(self.pattern.append(fp, record));
        try!(fp.flush());
        Ok(())
    }
}


pub fn init(log_path: &Path, use_stdout: bool) -> io::Result<()> {
    try!(ensure_dir(log_path));

    let file_appender = RollingFileAppender::new(log_path, "cache-rs");
    let mut root_cfg = Root::builder(LogLevelFilter::Info)
        .appender("file".into());
    if use_stdout {
        root_cfg = root_cfg.appender("con".into());
    }
    let mut config = Config::builder(root_cfg.build())
        .appender(Appender::builder("file".into(), box file_appender).build());
    if use_stdout {
        let con_appender = ConsoleAppender::new();
        config = config.appender(Appender::builder("con".into(), box con_appender).build());
    }
    let config = config.build().expect("error building logging config");

    let _ = log4rs::init_config(config);
    Ok(())
}
