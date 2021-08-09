use chewdata::connector::in_memory::InMemory;
use chewdata::connector::Connector;
use chewdata::document::json::Json;
use chewdata::document::jsonl::Jsonl;
use chewdata::document::Document;
use criterion::{criterion_group, criterion_main, Criterion};
use futures::stream::StreamExt;
use criterion::async_executor::FuturesExecutor;

const JSON_DATA: &str = r#"[{"array1":[{"field":"value1"},{"field":"value2"}]},{"object":{"object_key":"object_value"}}]"#;

fn read_json_benchmark(c: &mut Criterion) {
    let connector = InMemory::new(JSON_DATA);
    let document = Json::default();
    c.bench_function("Read json", move |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            let mut connector: Box<dyn Connector> = Box::new(connector.clone());
            let mut dataset = document.read_data(&mut connector).await.unwrap();
            while let Some(_data_result) = dataset.next().await {}
        });
    });
}

fn read_jsonl_benchmark(c: &mut Criterion) {
    let connector = InMemory::new(JSON_DATA);
    let document = Jsonl::default();
    c.bench_function("Read jsonl", move |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            let mut connector: Box<dyn Connector> = Box::new(connector.clone());
            let mut dataset = document.read_data(&mut connector).await.unwrap();
            while let Some(_data_result) = dataset.next().await {}
        });
    });
}

criterion_group!(benches, read_jsonl_benchmark, read_json_benchmark,);
criterion_main!(benches);
