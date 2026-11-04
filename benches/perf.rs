use criterion::criterion_main;

mod load;

criterion_main!(load::benches);
