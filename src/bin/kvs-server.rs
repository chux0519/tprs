#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate env_logger;
use std::env;
use std::fs;
use std::net::{SocketAddr, TcpListener};
use structopt::StructOpt;

extern crate kvs;
use kvs::network::serve;
use kvs::{KvStore, KvStoreError, Result, SledKvsEngine};

#[derive(StructOpt, Debug)]
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
    #[derive(Copy, Clone, PartialEq, Debug)]
    pub enum Engine {
        sled,
        kvs
    }
}

fn main() -> Result<()> {
    env_logger::init();
    let opt = Opts::from_args();
    let engine = check_engine(&opt.engine)?;
    error!(
        "{} version: {}",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );
    error!("Configuration: --addr {} --engine {}", opt.addr, engine);

    if engine == Engine::kvs {
        let store = KvStore::open(&env::current_dir()?)?;
        let listener = TcpListener::bind(opt.addr)?;

        serve(listener, store)?;
    } else if engine == Engine::sled {
        let store = SledKvsEngine::open(&env::current_dir()?)?;
        let listener = TcpListener::bind(opt.addr)?;

        serve(listener, store)?;
    }
    Ok(())
}

fn check_engine(e: &Option<Engine>) -> Result<Engine> {
    let _engine = match e {
        None => Engine::kvs,
        Some(ng) => ng.clone(),
    };
    let cur_dir = env::current_dir()?;
    let engine_file = cur_dir.join("engine");
    match fs::File::open(&engine_file) {
        Err(e) => match e.kind() {
            // If no engine file, write the engine into a new engine file
            std::io::ErrorKind::NotFound => {
                fs::write(&engine_file, format!("{}", _engine))?;
                return Ok(_engine);
            }
            _ => return Err(e.into()),
        },
        Ok(_) => {
            let last_engine = fs::read_to_string(&engine_file)?
                .parse()
                .expect("Can not parse engine from engine file");
            if last_engine != _engine {
                return Err(KvStoreError::EngineNotMatch);
            }
            return Ok(last_engine);
        }
    }
}
