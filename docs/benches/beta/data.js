window.BENCHMARK_DATA = {
  "lastUpdate": 1636312414630,
  "repoUrl": "https://github.com/jmfiaschi/chewdata",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "jm.fiaschi@gmail.com",
            "name": "jmfiaschi",
            "username": "jmfiaschi"
          },
          "committer": {
            "email": "jm.fiaschi@gmail.com",
            "name": "jmfiaschi",
            "username": "jmfiaschi"
          },
          "distinct": true,
          "id": "b6c1ae46e2a7c69549381a83748d6515e11ca779",
          "message": "fix(docker): CI",
          "timestamp": "2021-11-07T19:55:30+01:00",
          "tree_id": "b9ed72474a39db06f12c9ea8668be8f0e3717ce5",
          "url": "https://github.com/jmfiaschi/chewdata/commit/b6c1ae46e2a7c69549381a83748d6515e11ca779"
        },
        "date": 1636312413383,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 221,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 194,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 217093,
            "range": "± 6288",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 31318,
            "range": "± 1012",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 388,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 600,
            "range": "± 16",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}