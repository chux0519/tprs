extern crate structopt;
use structopt::StructOpt;

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
    match opt {
        Opts::Set(_x) => {
            panic!("unimplemented!");
        }
        Opts::Get(_x) => {
            panic!("unimplemented!");
        }
        Opts::Remove(_x) => {
            panic!("unimplemented!");
        }
    }
}
