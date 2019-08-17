#[macro_use]
extern crate criterion;

use criterion::{BatchSize, Criterion};
use crossbeam::channel::unbounded;
use kvs::network::{KvsClient, KvsServer};
use kvs::thread_pool::{SharedQueueThreadPool, ThreadPool};
use kvs::KvStore;
use std::fs;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

fn write_queued_kvstore(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let mut idx = 0;
    let inputs = &[1, 2, 4, 8];

    c.bench_function_over_inputs(
        "write_queued_kvstore",
        move |b, &&num| {
            // do setup here
            let mut kvs = vec![];
            for i in 0..1000 {
                let k = format!("{:0>8}", i);
                let v = "value".to_owned();
                kvs.push((k, v));
            }

            let (c_tx, s_rx) = unbounded();
            let (s_tx, c_rx) = unbounded();

            b.iter_batched(
                || {
                    // setup store
                    let db_path = temp_dir.path().join(format!("{}", idx));
                    idx += 1;

                    fs::create_dir_all(&db_path).unwrap();
                    let store = KvStore::open(&db_path).unwrap();
                    dbg!(&db_path);
                    let pool = SharedQueueThreadPool::new(num).unwrap();

                    let mut server = KvsServer::new(store, pool)
                        .rx(c_rx.clone())
                        .tx(c_tx.clone());
                    thread::spawn(move || {
                        server.listen("127.0.0.1:4001".parse().unwrap()).unwrap();
                    });
                    let client_pool = SharedQueueThreadPool::new(num).unwrap();
                    client_pool
                },
                |client_pool| {
                    let done_jobs = Arc::new(AtomicI32::new(0));
                    for (k, v) in &kvs {
                        let _k = k.clone();
                        let _v = v.clone();
                        let _done_jobs = done_jobs.clone();
                        client_pool.spawn(move || {
                            let mut client =
                                KvsClient::new("127.0.0.1:4001".parse().unwrap()).unwrap();
                            client.handshake().unwrap();
                            client.set(_k, _v).unwrap();
                            client.quit().unwrap();
                            _done_jobs.fetch_add(1, Ordering::Relaxed);
                        })
                    }
                    // all jobs done
                    loop {
                        if done_jobs.load(Ordering::Relaxed) == 1000 {
                            break;
                        }
                    }

                    let mut check_client =
                        KvsClient::new("127.0.0.1:4001".parse().unwrap()).unwrap();
                    check_client.handshake().unwrap();
                    for (k, v) in &kvs {
                        let _k = k.clone();
                        let _v = check_client.get(_k).unwrap().unwrap();
                        assert_eq!(_v, *v);
                    }
                    check_client.quit().unwrap();

                    // send shutdown to server
                    s_tx.send(()).unwrap();
                    // trigger quit
                    KvsClient::new("127.0.0.1:4001".parse().unwrap()).unwrap();
                    // wait for shutdown ack
                    s_rx.recv().unwrap();
                },
                BatchSize::SmallInput,
            )
        },
        inputs,
    );
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = write_queued_kvstore
}
criterion_main!(benches);
