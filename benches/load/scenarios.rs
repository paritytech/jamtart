use criterion::{criterion_group, BenchmarkId, Criterion, Throughput};
use tokio::io;
use tokio::net::TcpStream;

fn bench_jip_3_throughput(bencher: &mut Criterion) {
    let tart_addr = std::env::var("BENCH_TART_ADDR")
        .unwrap();
    let proxy_addr = "127.0.0.1:9999";

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();

    let input = runtime.block_on({
        let tart_addr = tart_addr.clone();

        async move {
            super::input::collect_input(
                proxy_addr,
                tart_addr.as_str(),
            ).await
        }
    }).unwrap();

    let mut stream = runtime.block_on({
        let tart_addr = tart_addr.clone();

        async move {
            TcpStream::connect(tart_addr).await.unwrap()
        }
    });

    let mut bencher = bencher.benchmark_group("JIP-3");

    bencher.throughput(Throughput::Elements(input.event_count as u64));
    bencher.sample_size(10);
    bencher.bench_with_input(BenchmarkId::from_parameter("test"), &input.data, {
        |b, input| {
            b.iter(
                || runtime.block_on(async {
                    io::copy(&mut input.as_slice(), &mut stream).await.unwrap();
                }),
            );
        }
    });
}

criterion_group!(benches, bench_jip_3_throughput);
