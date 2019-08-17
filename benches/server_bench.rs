#[macro_use]
extern crate criterion;

use criterion::{BatchSize, Criterion, ParameterizedBenchmark};
use crossbeam::channel::unbounded;
use kvs::network::{KvsClient, KvsServer};
use kvs::thread_pool::{RayonThreadPool, SharedQueueThreadPool, ThreadPool};
use kvs::{KvStore, SledKvsEngine};
use std::fs;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

fn write_with_different_threadpool(c: &mut Criterion) {
    let inputs = &[1, 2, 4, 8];

    c.bench(
        "write_with_different_threadpool",
        ParameterizedBenchmark::new(
            "SharedQueueThreadPool",
            move |b, &&num| {
                let temp_dir = TempDir::new().unwrap();
                let mut idx = 0;

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
        )
        .with_function("RayonThreadPool", move |b, &&num| {
            let temp_dir = TempDir::new().unwrap();
            let mut idx = 0;

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
                    let pool = RayonThreadPool::new(num).unwrap();

                    let mut server = KvsServer::new(store, pool)
                        .rx(c_rx.clone())
                        .tx(c_tx.clone());
                    thread::spawn(move || {
                        server.listen("127.0.0.1:4002".parse().unwrap()).unwrap();
                    });
                    let client_pool = RayonThreadPool::new(num).unwrap();
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
                                KvsClient::new("127.0.0.1:4002".parse().unwrap()).unwrap();
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
                        KvsClient::new("127.0.0.1:4002".parse().unwrap()).unwrap();
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
                    KvsClient::new("127.0.0.1:4002".parse().unwrap()).unwrap();
                    // wait for shutdown ack
                    s_rx.recv().unwrap();
                },
                BatchSize::SmallInput,
            )
        }),
    );
}

fn write_with_different_kvengine(c: &mut Criterion) {
    let inputs = &[1, 2, 4, 8];

    c.bench(
        "write_with_different_kvengine",
        ParameterizedBenchmark::new(
            "KvStore",
            move |b, &&num| {
                let temp_dir = TempDir::new().unwrap();
                let mut idx = 0;

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
                        let pool = SharedQueueThreadPool::new(num).unwrap();

                        let mut server = KvsServer::new(store, pool)
                            .rx(c_rx.clone())
                            .tx(c_tx.clone());
                        thread::spawn(move || {
                            server.listen("127.0.0.1:4003".parse().unwrap()).unwrap();
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
                                    KvsClient::new("127.0.0.1:4003".parse().unwrap()).unwrap();
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
                            KvsClient::new("127.0.0.1:4003".parse().unwrap()).unwrap();
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
                        KvsClient::new("127.0.0.1:4003".parse().unwrap()).unwrap();
                        // wait for shutdown ack
                        s_rx.recv().unwrap();
                    },
                    BatchSize::SmallInput,
                )
            },
            inputs,
        )
        .with_function("SledKvsEngine", move |b, &&num| {
            let temp_dir = TempDir::new().unwrap();
            let mut idx = 0;

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
                    let store = SledKvsEngine::open(&db_path).unwrap();
                    let pool = SharedQueueThreadPool::new(num).unwrap();

                    let mut server = KvsServer::new(store, pool)
                        .rx(c_rx.clone())
                        .tx(c_tx.clone());
                    thread::spawn(move || {
                        server.listen("127.0.0.1:4004".parse().unwrap()).unwrap();
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
                                KvsClient::new("127.0.0.1:4004".parse().unwrap()).unwrap();
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
                        KvsClient::new("127.0.0.1:4004".parse().unwrap()).unwrap();
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
                    KvsClient::new("127.0.0.1:4004".parse().unwrap()).unwrap();
                    // wait for shutdown ack
                    s_rx.recv().unwrap();
                },
                BatchSize::SmallInput,
            )
        }),
    );
}

fn read_with_different_threadpool(c: &mut Criterion) {
    let inputs = &[1, 2, 4, 8];

    c.bench(
        "read_with_different_threadpool",
        ParameterizedBenchmark::new(
            "SharedQueueThreadPool",
            move |b, &&num| {
                let temp_dir = TempDir::new().unwrap();
                let mut idx = 0;

                let (c_tx, s_rx) = unbounded();
                let (s_tx, c_rx) = unbounded();

                b.iter_batched(
                    || {
                        // setup store
                        let db_path = temp_dir.path().join(format!("{}", idx));
                        idx += 1;

                        fs::create_dir_all(&db_path).unwrap();
                        let store = KvStore::open(&db_path).unwrap();
                        let pool = SharedQueueThreadPool::new(num).unwrap();

                        let mut server = KvsServer::new(store, pool)
                            .rx(c_rx.clone())
                            .tx(c_tx.clone());
                        thread::spawn(move || {
                            server.listen("127.0.0.1:4005".parse().unwrap()).unwrap();
                        });

                        let mut _client =
                            KvsClient::new("127.0.0.1:4005".parse().unwrap()).unwrap();
                        _client.handshake().unwrap();
                        for i in 0..1000 {
                            let k = format!("{:0>8}", i);
                            let v = "value".to_owned();
                            _client.set(k, v).unwrap();
                        }
                        _client.quit().unwrap();

                        let client_pool = SharedQueueThreadPool::new(num).unwrap();
                        client_pool
                    },
                    |client_pool| {
                        let done_jobs = Arc::new(AtomicI32::new(0));
                        for i in 0..1000 {
                            let k = format!("{:0>8}", i);
                            let v = "value".to_owned();
                            let _done_jobs = done_jobs.clone();
                            client_pool.spawn(move || {
                                let mut client =
                                    KvsClient::new("127.0.0.1:4005".parse().unwrap()).unwrap();
                                client.handshake().unwrap();
                                let _v = client.get(k).unwrap().unwrap();
                                assert_eq!(_v, v);
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
                        // send shutdown to server
                        s_tx.send(()).unwrap();
                        // trigger quit
                        KvsClient::new("127.0.0.1:4005".parse().unwrap()).unwrap();
                        // wait for shutdown ack
                        s_rx.recv().unwrap();
                    },
                    BatchSize::SmallInput,
                )
            },
            inputs,
        )
        .with_function("RayonThreadPool", move |b, &&num| {
            let temp_dir = TempDir::new().unwrap();
            let mut idx = 0;

            let (c_tx, s_rx) = unbounded();
            let (s_tx, c_rx) = unbounded();

            b.iter_batched(
                || {
                    // setup store
                    let db_path = temp_dir.path().join(format!("{}", idx));
                    idx += 1;

                    fs::create_dir_all(&db_path).unwrap();
                    let store = KvStore::open(&db_path).unwrap();
                    let pool = RayonThreadPool::new(num).unwrap();

                    let mut server = KvsServer::new(store, pool)
                        .rx(c_rx.clone())
                        .tx(c_tx.clone());
                    thread::spawn(move || {
                        server.listen("127.0.0.1:4006".parse().unwrap()).unwrap();
                    });

                    let mut _client = KvsClient::new("127.0.0.1:4006".parse().unwrap()).unwrap();
                    _client.handshake().unwrap();
                    for i in 0..1000 {
                        let k = format!("{:0>8}", i);
                        let v = "value".to_owned();
                        _client.set(k, v).unwrap();
                    }
                    _client.quit().unwrap();

                    let client_pool = RayonThreadPool::new(num).unwrap();
                    client_pool
                },
                |client_pool| {
                    let done_jobs = Arc::new(AtomicI32::new(0));
                    for i in 0..1000 {
                        let k = format!("{:0>8}", i);
                        let v = "value".to_owned();
                        let _done_jobs = done_jobs.clone();
                        client_pool.spawn(move || {
                            let mut client =
                                KvsClient::new("127.0.0.1:4006".parse().unwrap()).unwrap();
                            client.handshake().unwrap();
                            let _v = client.get(k).unwrap().unwrap();
                            assert_eq!(_v, v);
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
                    // send shutdown to server
                    s_tx.send(()).unwrap();
                    // trigger quit
                    KvsClient::new("127.0.0.1:4006".parse().unwrap()).unwrap();
                    // wait for shutdown ack
                    s_rx.recv().unwrap();
                },
                BatchSize::SmallInput,
            )
        }),
    );
}

fn read_with_different_kvengine(c: &mut Criterion) {
    let inputs = &[1, 2, 4, 8];

    c.bench(
        "read_with_different_kvengine",
        ParameterizedBenchmark::new(
            "KvStore",
            move |b, &&num| {
                let temp_dir = TempDir::new().unwrap();
                let mut idx = 0;

                let (c_tx, s_rx) = unbounded();
                let (s_tx, c_rx) = unbounded();

                b.iter_batched(
                    || {
                        // setup store
                        let db_path = temp_dir.path().join(format!("{}", idx));
                        idx += 1;

                        fs::create_dir_all(&db_path).unwrap();
                        let store = KvStore::open(&db_path).unwrap();
                        let pool = SharedQueueThreadPool::new(num).unwrap();

                        let mut server = KvsServer::new(store, pool)
                            .rx(c_rx.clone())
                            .tx(c_tx.clone());
                        thread::spawn(move || {
                            server.listen("127.0.0.1:4007".parse().unwrap()).unwrap();
                        });

                        let mut _client =
                            KvsClient::new("127.0.0.1:4007".parse().unwrap()).unwrap();
                        _client.handshake().unwrap();
                        for i in 0..1000 {
                            let k = format!("{:0>8}", i);
                            let v = "value".to_owned();
                            _client.set(k, v).unwrap();
                        }
                        _client.quit().unwrap();

                        let client_pool = SharedQueueThreadPool::new(num).unwrap();
                        client_pool
                    },
                    |client_pool| {
                        let done_jobs = Arc::new(AtomicI32::new(0));
                        for i in 0..1000 {
                            let k = format!("{:0>8}", i);
                            let v = "value".to_owned();
                            let _done_jobs = done_jobs.clone();
                            client_pool.spawn(move || {
                                let mut client =
                                    KvsClient::new("127.0.0.1:4007".parse().unwrap()).unwrap();
                                client.handshake().unwrap();
                                let _v = client.get(k).unwrap().unwrap();
                                assert_eq!(_v, v);
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
                        // send shutdown to server
                        s_tx.send(()).unwrap();
                        // trigger quit
                        KvsClient::new("127.0.0.1:4007".parse().unwrap()).unwrap();
                        // wait for shutdown ack
                        s_rx.recv().unwrap();
                    },
                    BatchSize::SmallInput,
                )
            },
            inputs,
        )
        .with_function("SledKvsEngine", move |b, &&num| {
            let temp_dir = TempDir::new().unwrap();
            let mut idx = 0;

            let (c_tx, s_rx) = unbounded();
            let (s_tx, c_rx) = unbounded();

            b.iter_batched(
                || {
                    // setup store
                    let db_path = temp_dir.path().join(format!("{}", idx));
                    idx += 1;

                    fs::create_dir_all(&db_path).unwrap();
                    let store = SledKvsEngine::open(&db_path).unwrap();
                    let pool = SharedQueueThreadPool::new(num).unwrap();

                    let mut server = KvsServer::new(store, pool)
                        .rx(c_rx.clone())
                        .tx(c_tx.clone());
                    thread::spawn(move || {
                        server.listen("127.0.0.1:4008".parse().unwrap()).unwrap();
                    });

                    let mut _client = KvsClient::new("127.0.0.1:4008".parse().unwrap()).unwrap();
                    _client.handshake().unwrap();
                    for i in 0..1000 {
                        let k = format!("{:0>8}", i);
                        let v = "value".to_owned();
                        _client.set(k, v).unwrap();
                    }
                    _client.quit().unwrap();

                    let client_pool = SharedQueueThreadPool::new(num).unwrap();
                    client_pool
                },
                |client_pool| {
                    let done_jobs = Arc::new(AtomicI32::new(0));
                    for i in 0..1000 {
                        let k = format!("{:0>8}", i);
                        let v = "value".to_owned();
                        let _done_jobs = done_jobs.clone();
                        client_pool.spawn(move || {
                            let mut client =
                                KvsClient::new("127.0.0.1:4008".parse().unwrap()).unwrap();
                            client.handshake().unwrap();
                            let _v = client.get(k).unwrap().unwrap();
                            assert_eq!(_v, v);
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
                    // send shutdown to server
                    s_tx.send(()).unwrap();
                    // trigger quit
                    KvsClient::new("127.0.0.1:4008".parse().unwrap()).unwrap();
                    // wait for shutdown ack
                    s_rx.recv().unwrap();
                },
                BatchSize::SmallInput,
            )
        }),
    );
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(2);
    targets = write_with_different_threadpool,write_with_different_kvengine,read_with_different_threadpool,read_with_different_kvengine
}
criterion_main!(benches);
