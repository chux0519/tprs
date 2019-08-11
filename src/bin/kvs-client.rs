extern crate structopt;
use structopt::StructOpt;

use std::net::SocketAddr;

extern crate kvs;

use kvs::network::KvsClient;
use kvs::Result;

#[derive(StructOpt, Debug)]
enum Opts {
    #[structopt(name = "set", about = "Set key value pair")]
    Set(SetArgs),
    #[structopt(name = "get", about = "Get value of given key")]
    Get(GetArgs),
    #[structopt(name = "rm", about = "Remove key")]
    Remove(RemoveArgs),
}

#[derive(StructOpt, Debug)]
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

#[derive(StructOpt, Debug)]
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

#[derive(StructOpt, Debug)]
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

fn main() -> Result<()> {
    let opt = Opts::from_args();
    match opt {
        Opts::Set(set_args) => {
            let mut client = KvsClient::new(set_args.addr)?;
            client.handshake()?;
            client.set(set_args.key, set_args.value)?;
            client.quit()?;
        }
        Opts::Get(get_args) => {
            let mut client = KvsClient::new(get_args.addr)?;
            client.handshake()?;
            let resp = client.get(get_args.key)?;
            match resp {
                Some(v) => println!("{}", v),
                None => println!("Key not found"),
            }
            client.quit()?;
        }
        Opts::Remove(remove_args) => {
            let mut client = KvsClient::new(remove_args.addr)?;
            client.handshake()?;
            client.remove(remove_args.key)?;
            client.quit()?;
        }
    };
    Ok(())
}
