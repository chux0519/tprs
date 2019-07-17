extern crate structopt;
use structopt::StructOpt;

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
        Opts::Set(cmd) => {
            kv.set(cmd.key, cmd.value).expect("set error");
        }
        Opts::Get(cmd) => {
            let val = kv.get(cmd.key).expect("get error");
            if let Some(v) = val {
                println!("{}", v);
            }
        }
        Opts::Remove(cmd) => {
            kv.remove(cmd.key).expect("remove error");
        }
    }
}
