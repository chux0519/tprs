#[macro_use]
extern crate clap;
extern crate structopt;
use structopt::StructOpt;

use std::net::SocketAddr;

extern crate kvs;

#[derive(StructOpt)]
struct Opts {
    #[structopt(
        long,
        help = "Set server address",
        value_name = "IP:PORT",
        default_value = "127.0.0.1:4000",
        parse(try_from_str)
    )]
    addr: SocketAddr,
    #[structopt(
        long,
        help = "Set kv engine",
        value_name = "ENGINE-NAME",
        raw(possible_values = "&Engine::variants()")
    )]
    engine: Option<Engine>,
}

arg_enum! {
    #[allow(non_camel_case_types)]
    #[derive(PartialEq, Debug)]
    pub enum Engine {
        sled,
        kvs
    }
}
fn main() {
    let opt = Opts::from_args();
}
