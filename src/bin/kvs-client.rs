extern crate structopt;
use structopt::StructOpt;

use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::process;

extern crate kvs;

use kvs::network::{SessionClientCommand, SessionServerResp};
use kvs::{KvStoreError, Result};

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
    let (addr, cmd) = match opt {
        Opts::Set(set_args) => (
            set_args.addr,
            SessionClientCommand::Set(set_args.key, set_args.value),
        ),
        Opts::Get(get_args) => (get_args.addr, SessionClientCommand::Get(get_args.key)),
        Opts::Remove(remove_args) => (
            remove_args.addr,
            SessionClientCommand::Remove(remove_args.key),
        ),
    };
    let mut buf = vec![0u8; 1024];

    let mut stream = TcpStream::connect(addr)?;
    let handshake = SessionClientCommand::Handshake;

    let quit = SessionClientCommand::Quit;
    stream.write_all(&serde_json::to_string(&handshake)?.as_bytes())?;
    stream.read(&mut buf)?;

    stream.write_all(&serde_json::to_string(&cmd)?.as_bytes())?;
    let len = stream.read(&mut buf)?;
    let resp: SessionServerResp = serde_json::from_slice(&buf[..len])?;

    stream.write_all(&serde_json::to_string(&quit)?.as_bytes())?;
    match resp {
        SessionServerResp::Value(v) => {
            println!("{}", v);
        }
        SessionServerResp::NotFound => {
            println!("Key not found");
        }
        SessionServerResp::ERR(e) => {
            eprintln!("{}", e);
            return Err(KvStoreError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                e,
            )));
        }
        _ => {}
    }
    Ok(())
}
