#[cfg(feature = "curl")]
use chewdata::connector::curl::Curl;
use chewdata::connector::in_memory::InMemory;
use chewdata::connector::Connector;
#[cfg(feature = "csv")]
use chewdata::document::csv::Csv;
use chewdata::document::json::Json;
use chewdata::document::jsonl::Jsonl;
#[cfg(feature = "parquet")]
use chewdata::document::parquet::Parquet;
#[cfg(feature = "toml")]
use chewdata::document::toml::Toml;
#[cfg(feature = "xml")]
use chewdata::document::xml::Xml;
use chewdata::document::yaml::Yaml;
use chewdata::document::Document;
use chewdata::updater::{Action, ActionType, UpdaterType};
use criterion::async_executor::FuturesExecutor;
use criterion::{criterion_group, Criterion};
use futures::stream::StreamExt;
use serde_json::Value;
use std::io::Read;

fn document_read_benchmark(c: &mut Criterion) {
    let readers: Vec<(&str, &str, Box<dyn Document>)> = vec![
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
        // Load the file into memory once
        let mut buf = Vec::new();
        std::fs::File::open(file)
            .unwrap()
            .read_to_end(&mut buf)
            .unwrap();

        // Keep the original buf immutable and clone it each time
        let document = document.clone_box(); // if trait supports it

        c.bench_function(format!("read_{}", format).as_str(), move |b| {
            let buf = buf.clone();
            b.to_async(FuturesExecutor).iter(|| async {
                let mut connector: InMemory = buf.clone().into();
                connector.set_document(document.clone_box()).unwrap();

                let mut dataset = connector.fetch().await.unwrap().unwrap();
                while dataset.next().await.is_some() {}
            });
        });
    }
}

fn faker_benchmark(c: &mut Criterion) {
    let fakers = vec![
        ("words", "{{ fake_words() }}"),
        ("sentences", "{{ fake_sentences() }}"),
        ("paragraphs", "{{ fake_paragraphs() }}"),
        ("phone_number", "{{ fake_phone_number() }}"),
        ("password", "{{ fake_password() }}"),
    ];

    for (action_name, action_pattern) in fakers {
        let action = Action {
            field: action_name.to_string(),
            pattern: Some(action_pattern.to_string()),
            action_type: ActionType::Merge,
        };

        let updater = UpdaterType::default().updater_inner();
        let actions = vec![action];
        let input_value = Value::default();

        c.bench_function(format!("faker/{}", action_name).as_str(), move |b| {
            b.to_async(FuturesExecutor).iter(|| async {
                // Appel minimal dans chaque itération
                updater
                    .update(&input_value, &input_value, &input_value, &actions)
                    .await
                    .unwrap();
            });
        });
    }
}

#[cfg(feature = "curl")]
fn curl_http1_benchmark(c: &mut Criterion) {
    use http::Method;

    let curls: Vec<(&'static str, Method)> = vec![("/get", Method::GET), ("/get", Method::HEAD)];

    for (path, method) in curls {
        let endpoint = "http://localhost:8080".to_string();
        let path = path.to_string();
        let method = method;
        let document = Json::default();

        c.bench_function(&format!("curl/{}/", method), move |b| {
            b.to_async(FuturesExecutor).iter(|| async {
                let mut connector = Curl::default();
                connector.endpoint = endpoint.clone();
                connector.path = path.clone();
                connector.method = method.clone();
                connector.is_cached = false;
                connector.set_document(Box::new(document.clone())).unwrap();

                let _ = connector.fetch().await.unwrap();
            });
        });
    }
}

#[cfg(feature = "curl")]
fn criterion_http_config() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .measurement_time(std::time::Duration::from_secs(1))
}

criterion_group! {
    name = reader;
    config = Criterion::default();
    targets = document_read_benchmark
}

criterion_group! {
    name = updater;
    config = Criterion::default();
    targets = faker_benchmark
}

#[cfg(feature = "curl")]
criterion_group! {
    name = http;
    config = criterion_http_config();
    targets = curl_http1_benchmark
}

fn main() {
    {
        reader();
    }
    {
        updater();
    }
    {
        #[cfg(feature = "curl")]
        http();
    }

    crate::Criterion::default()
        .configure_from_args()
        .final_summary();
}
