use criterion::{criterion_group, criterion_main, Criterion};
use chewdata::connector::in_memory::InMemory;
use chewdata::document::json::Json;
use chewdata::document::Document;
use std::io::Read;

const JSON_DATA: &str = r#"[{"array1":[{"field":"value1"},{"field":"value2"}]},{"object":{"object_key":"object_value"}}]"#;

fn read_json_benchmark(c: &mut Criterion) {
    let connector = InMemory::new(JSON_DATA);
    let document = Json::default();
    c.bench_function("Read json", move |b| {
        b.iter(|| for _data in document.read_data(Box::new(connector.clone())).unwrap().into_iter() {})
    });
}

fn read_in_memory_benchmark(c: &mut Criterion) {
    let mut connector = InMemory::new(JSON_DATA);
    let mut vec = Vec::default();
    c.bench_function("Read in memory", move |b| {
        b.iter(|| connector.read_to_end(&mut vec))
    });
}

criterion_group!(benches, 
    read_in_memory_benchmark,
    read_json_benchmark,
);
criterion_main!(benches);
