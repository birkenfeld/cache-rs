# cache-rs

A Rust implementation of the [NICOS](http://nicos-controls.org) cache server and
protocol.

## Building

    cargo build --release
    
should download all required dependencies.  Use a nightly Rust compiler!

## Basic usage

    cargo run --release -- [options]
    
Options are:

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

## Benchmarks

Use

    python bench.py <name_of_benchmark> -n <#keys> -s <#clients>
  
Useful benchmarks are:

    python bench.py ask_only -n 100000
    python bench.py single_writer -n 20000 -s 25
    python bench.py multi_writer -n 20000 -s 25
    python bench.py udp -n 10000
  
**Take care** to restart the cache with `--clear` after each benchmark to get
reproducible numbers. The large amount of messages degrades hashtable
performance for subsequent benchmarks.
