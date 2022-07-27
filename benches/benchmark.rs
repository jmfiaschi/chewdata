use chewdata::connector::Connector;
use chewdata::document::csv::Csv;
use chewdata::document::json::Json;
use chewdata::document::jsonl::Jsonl;
use chewdata::document::parquet::Parquet;
use chewdata::document::toml::Toml;
use chewdata::document::yaml::Yaml;
use chewdata::document::Document;
use chewdata::updater::{Action, ActionType, UpdaterType};
use chewdata::{connector::in_memory::InMemory, document::xml::Xml};
use criterion::async_executor::FuturesExecutor;
use criterion::{criterion_group, criterion_main, Criterion};
use futures::stream::StreamExt;
use serde_json::Value;
use std::{fs::OpenOptions, io::Read};

fn document_read_benchmark(c: &mut Criterion) {
    let readers: [(&str, &str, Box<dyn Document>); 7] = [
        ("json", "data/one_line.json", Box::new(Json::default())),
        ("jsonl", "data/one_line.jsonl", Box::new(Jsonl::default())),
        #[cfg(feature = "xml")]
        ("xml", "data/one_line.xml", Box::new(Xml::default())),
        #[cfg(feature = "csv")]
        ("csv", "data/one_line.csv", Box::new(Csv::default())),
        #[cfg(feature = "toml")]
        ("toml", "data/one_line.toml", Box::new(Toml::default())),
        ("yaml", "data/one_line.yml", Box::new(Yaml::default())),
        #[cfg(feature = "parquet")]
        (
            "parquet",
            "data/one_line.parquet",
            Box::new(Parquet::default()),
        ),
    ];

    for (format, file, document) in readers {
        let mut buf = Vec::default();
        OpenOptions::new()
            .read(true)
            .open(file)
            .unwrap()
            .read_to_end(&mut buf)
            .unwrap();

        let connector: InMemory = buf.into();
        let document = document;

        c.bench_function(format!("read_{}/", format).as_str(), move |b| {
            b.to_async(FuturesExecutor).iter(|| async {
                let mut connector: Box<dyn Connector> = Box::new(connector.clone());
                let mut dataset = connector.fetch(document.clone()).await.unwrap().unwrap();
                while let Some(_) = dataset.next().await {}
            });
        });
    }
}

fn faker_benchmark(c: &mut Criterion) {
    let fakers = vec![
        ("words", "{{{{ fake_words() }}}}"),
        ("sentences", "{{{{ fake_sentences() }}}}"),
        ("paragraphs", "{{{{ fake_paragraphs() }}}}"),
        ("phone_number", "{{{{ fake_phone_number() }}}}"),
        ("password", "{{{{ fake_password() }}}}"),
    ];

    for (action_name, action_pattern) in fakers {
        let updater = UpdaterType::default().updater_inner();

        c.bench_function(format!("{}/", action_name).as_str(), move |b| {
            b.to_async(FuturesExecutor).iter(|| async {
                updater.update(
                    Value::Null,
                    Value::Null,
                    None,
                    vec![Action {
                        field: action_name.to_string(),
                        pattern: Some(action_pattern.to_string()),
                        action_type: ActionType::Merge,
                    }],
                    String::default(),
                    String::default(),
                )
            });
        });
    }
}

criterion_group!(benches, document_read_benchmark, faker_benchmark);
criterion_main!(benches);
