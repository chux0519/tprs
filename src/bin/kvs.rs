extern crate clap;
use clap::{App, Arg, SubCommand};

fn main() {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(
            SubCommand::with_name("get").arg(
                Arg::with_name("KEY")
                    .required(true)
                    .help("get value of key"),
            ),
        )
        .subcommand(
            SubCommand::with_name("set")
                .arg(
                    Arg::with_name("KEY")
                        .required(true)
                        .help("set value of key"),
                )
                .arg(
                    Arg::with_name("VALUE")
                        .required(true)
                        .help("set value of key"),
                ),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .arg(Arg::with_name("KEY").required(true).help("delete key")),
        )
        .get_matches();
    match matches.subcommand() {
        ("set", Some(sub_m)) => {
            let k = sub_m.value_of("KEY").unwrap();
            let v = sub_m.value_of("VALUE").unwrap();
            panic!("unimplemented");
        }
        ("get", Some(sub_m)) => {
            let k = sub_m.value_of("KEY").unwrap();
            panic!("unimplemented");
        }
        ("rm", Some(sub_m)) => {
            let k = sub_m.value_of("KEY").unwrap();
            panic!("unimplemented");
        }
        _ => unimplemented!(),
    }
}
