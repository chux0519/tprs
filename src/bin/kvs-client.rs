extern crate structopt;
use structopt::StructOpt;

use std::net::SocketAddr;
use std::process;

extern crate kvs;

#[derive(StructOpt)]
enum Opts {
    #[structopt(name = "set", about = "Set key value pair")]
    Set(SetArgs),
    #[structopt(name = "get", about = "Get value of given key")]
    Get(GetArgs),
    #[structopt(name = "rm", about = "Remove key")]
    Remove(RemoveArgs),
}

#[derive(StructOpt)]
struct SetArgs {
    #[structopt(name = "KEY")]
    key: String,
    #[structopt(name = "VALUE")]
    value: String,
    #[structopt(
        long,
        help = "Set server address",
        value_name = "IP:PORT",
        default_value = "127.0.0.1:4000",
        parse(try_from_str)
    )]
    addr: SocketAddr,
}

#[derive(StructOpt)]
struct GetArgs {
    #[structopt(name = "KEY")]
    key: String,
    #[structopt(
        long,
        help = "Set server address",
        value_name = "IP:PORT",
        default_value = "127.0.0.1:4000",
        parse(try_from_str)
    )]
    addr: SocketAddr,
}

#[derive(StructOpt)]
struct RemoveArgs {
    #[structopt(name = "KEY")]
    key: String,
    #[structopt(
        long,
        help = "Set server address",
        value_name = "IP:PORT",
        default_value = "127.0.0.1:4000",
        parse(try_from_str)
    )]
    addr: SocketAddr,
}

fn main() {
    let opt = Opts::from_args();
}
