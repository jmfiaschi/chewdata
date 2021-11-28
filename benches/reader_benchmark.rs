use chewdata::connector::Connector;
use chewdata::document::csv::Csv;
use chewdata::document::json::Json;
use chewdata::document::jsonl::Jsonl;
use chewdata::document::toml::Toml;
use chewdata::document::yaml::Yaml;
use chewdata::document::Document;
use chewdata::{connector::in_memory::InMemory, document::xml::Xml};
use criterion::async_executor::FuturesExecutor;
use criterion::{criterion_group, criterion_main, Criterion};
use futures::stream::StreamExt;
use std::{fs::OpenOptions, io::Read};

fn document_read_benchmark(c: &mut Criterion) {
    let readers: [(&str, &str, Box<dyn Document>); 6] = [
        ("json", "data/one_line.json", Box::new(Json::default())),
        ("jsonl", "data/one_line.jsonl", Box::new(Jsonl::default())),
        ("xml", "data/one_line.xml", Box::new(Xml::default())),
        ("csv", "data/one_line.csv", Box::new(Csv::default())),
        ("toml", "data/one_line.toml", Box::new(Toml::default())),
        ("yaml", "data/one_line.yml", Box::new(Yaml::default())),
    ];

    for (format, file, document) in readers {
        let mut buff = String::default();
        OpenOptions::new()
            .read(true)
            .open(file)
            .unwrap()
            .read_to_string(&mut buff)
            .unwrap();

        let connector = InMemory::new(buff.as_str());
        let document = document;
        c.bench_function(format!("read_{}/", format).as_str(), move |b| {
            b.to_async(FuturesExecutor).iter(|| async {
                let mut connector: Box<dyn Connector> = Box::new(connector.clone());
                let mut dataset = document.read_data(&mut connector).await.unwrap();
                while let Some(_data_result) = dataset.next().await {}
            });
        });
    }
}

criterion_group!(benches, document_read_benchmark);
criterion_main!(benches);
