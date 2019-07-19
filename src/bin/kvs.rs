extern crate structopt;
use structopt::StructOpt;

use std::process;

extern crate kvs;
use kvs::KvStore;

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
}

#[derive(StructOpt)]
struct GetArgs {
    #[structopt(name = "KEY")]
    key: String,
}

#[derive(StructOpt)]
struct RemoveArgs {
    #[structopt(name = "KEY")]
    key: String,
}

fn main() {
    let opt = Opts::from_args();
    let mut kv = KvStore::open(std::path::Path::new(".")).expect("open db error");
    match opt {
        Opts::Set(cmd) => match kv.set(cmd.key, cmd.value) {
            Err(e) => {
                println!("{}", e);
                process::exit(1);
            }
            _ => {}
        },
        Opts::Get(cmd) => match kv.get(cmd.key) {
            Ok(res) => match res {
                Some(v) => {
                    println!("{}", v);
                }
                None => println!("Key not found"),
            },
            Err(e) => {
                println!("{}", e);
                process::exit(1);
            }
        },
        Opts::Remove(cmd) => match kv.remove(cmd.key) {
            Err(e) => {
                println!("{}", e);
                process::exit(1);
            }
            _ => {}
        },
    }
}
