use env_applier::EnvApply;
use std::env;
use std::io;

#[tokio::main]
async fn main() -> io::Result<()> {
    let _guard = slog_envlogger::init().unwrap();

    let config = r#"
    [
        {"type":"e","connector":{"type":"local","path":"./data/out/cascade_file1.json"}}
        ,{"type":"e","connector":{"type":"local","path":"./data/out/cascade_file2.json"}}
        ,{"type":"r","connector":{"type":"local","path":"./data/multi_lines.json"}}
        ,{"type":"t","updater":{"type":"tera","actions":[{"field":"/","pattern":"{% if input.number == 10 %}{{ throw(message='data go to writer.cascade_file2.json') }}{% else %}{{ input | json_encode() }}{% endif %}"}]}}
        ,{"type":"w","connector":{"type":"local","path":"./data/out/cascade_file1.json"},"data_type":"ok"}
        ,{"type":"w","connector":{"type":"local","path":"./data/out/cascade_file2.json"},"data_type":"err"}
    ]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None).await
}
