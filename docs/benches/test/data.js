window.BENCHMARK_DATA = {
  "lastUpdate": 1700679349881,
  "repoUrl": "https://github.com/jmfiaschi/chewdata",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "jmfiaschi@veepee.com",
            "name": "Jean-Marc Fiaschi",
            "username": "jmfiaschi-veepee"
          },
          "committer": {
            "email": "jm.fiaschi@gmail.com",
            "name": "Jean-Marc Fiaschi",
            "username": "jmfiaschi"
          },
          "distinct": true,
          "id": "3479943860615d936a0d0934487d00b4957383be",
          "message": "feat: upgrade version\n* reuse clients\nBREAKING CHANGE: rename curl fields\nBREAKING CHANGE: simplify autheticator and use it as a middleware",
          "timestamp": "2023-11-22T19:29:32+01:00",
          "tree_id": "81d5685e0a7f4169cabc80b4dae6cfea5e1fa481",
          "url": "https://github.com/jmfiaschi/chewdata/commit/3479943860615d936a0d0934487d00b4957383be"
        },
        "date": 1700679349057,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 5395,
            "range": "± 133",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 5351,
            "range": "± 19",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 298079,
            "range": "± 5234",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 20711,
            "range": "± 338",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 10691,
            "range": "± 41",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 13487,
            "range": "± 1622",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 126459,
            "range": "± 1265",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 19140,
            "range": "± 96",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 19097,
            "range": "± 118",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 19110,
            "range": "± 80",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 19070,
            "range": "± 96",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 19115,
            "range": "± 1229",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}