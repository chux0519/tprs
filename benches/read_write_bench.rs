#[macro_use]
extern crate criterion;

use criterion::{BatchSize, Criterion};
use kvs::{KvStore, KvsEngine, SledKvsEngine};
use rand::prelude::*;
use serde_json;
use std::env;
use std::fs;
use tempfile::TempDir;

fn gen_kvs(max_k_bytes: usize, max_v_bytes: usize, times: usize) -> Vec<(String, String)> {
    let mut ret = vec![];
    let mut rng = rand::thread_rng();
    for _ in 0..times {
        let mut k = vec![];
        let mut v = vec![];
        let k_bytes = rng.gen_range(0, max_k_bytes + 1);
        let v_bytes = rng.gen_range(0, max_v_bytes + 1);
        for _ in 0..k_bytes {
            let c = rng.gen_range(33, 127); // printable ascii
            k.push(c);
        }
        for _ in 0..v_bytes {
            let c = rng.gen_range(33, 127);
            v.push(c);
        }
        ret.push((String::from_utf8(k).unwrap(), String::from_utf8(v).unwrap()));
    }
    ret
}

fn load_kvs() -> Vec<(String, String)> {
    let p = env::current_dir().unwrap();
    let bench_kv_file = p.join("bench.kvs");
    match fs::File::open(&bench_kv_file) {
        Err(e) => match e.kind() {
            std::io::ErrorKind::NotFound => {
                let test_kvs: Vec<(String, String)> = gen_kvs(100000, 100000, 100);

                fs::write(&bench_kv_file, serde_json::to_string(&test_kvs).unwrap()).unwrap();
                return test_kvs;
            }
            _ => unimplemented!(),
        },
        Ok(_) => {
            let test_kvs_string = fs::read_to_string(&bench_kv_file).unwrap();
            let test_kvs = serde_json::from_str(&test_kvs_string).unwrap();
            return test_kvs;
        }
    }
}

fn kvs_write_benchmark(c: &mut Criterion) {
    let test_kvs = load_kvs();
    let temp_dir = TempDir::new().unwrap();
    let mut idx = 0;

    c.bench_function("kvs write", move |b| {
        b.iter_batched(
            || {
                let db_path = temp_dir.path().join(format!("{}", idx));
                idx += 1;

                fs::create_dir_all(&db_path).unwrap();
                KvStore::open(&db_path).unwrap()
            },
            |mut store| {
                for (k, v) in &test_kvs {
                    store.set(k.clone(), v.clone()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
}

fn sled_write_benchmark(c: &mut Criterion) {
    let test_kvs = load_kvs();
    let temp_dir = TempDir::new().unwrap();
    let mut idx = 0;

    c.bench_function("sled write", move |b| {
        b.iter_batched(
            || {
                let db_path = temp_dir.path().join(format!("{}", idx));
                idx += 1;

                fs::create_dir_all(&db_path).unwrap();
                SledKvsEngine::open(&db_path).unwrap()
            },
            |mut store| {
                for (k, v) in &test_kvs {
                    store.set(k.clone(), v.clone()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
}

fn kvs_read_benchmark(c: &mut Criterion) {
    let test_kvs = load_kvs();
    let temp_dir = TempDir::new().unwrap();
    {
        let mut store = KvStore::open(&temp_dir.path()).unwrap();
        for (k, v) in &test_kvs {
            store.set(k.clone(), v.clone()).unwrap();
        }
    }
    c.bench_function("kvs read", move |b| {
        b.iter_batched(
            || KvStore::open(&temp_dir.path()).unwrap(),
            |mut store| {
                for x in 0..10 {
                    // 10 * 100 = 1000
                    for (k, v) in &test_kvs {
                        store.get(k.clone()).unwrap();
                    }
                }
            },
            BatchSize::SmallInput,
        )
    });
}

fn sled_read_benchmark(c: &mut Criterion) {
    let test_kvs = load_kvs();
    let temp_dir = TempDir::new().unwrap();
    c.bench_function("sled read", move |b| {
        b.iter_batched(
            || {
                let mut store = SledKvsEngine::open(&temp_dir.path()).unwrap();
                for (k, v) in &test_kvs {
                    store.set(k.clone(), v.clone()).unwrap();
                }
                store
            },
            |mut store| {
                for _ in 0..10 {
                    for (k, v) in &test_kvs {
                        store.get(k.clone()).unwrap();
                    }
                }
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = kvs_write_benchmark, sled_write_benchmark, kvs_read_benchmark, sled_read_benchmark
}
criterion_main!(benches);
