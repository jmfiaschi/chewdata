window.BENCHMARK_DATA = {
  "lastUpdate": 1639935232905,
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
          "id": "ba19741348171e01fca4c21c6c4d98b2bb09abea",
          "message": "chore(release): commit changelog file",
          "timestamp": "2021-12-19T17:28:15+01:00",
          "tree_id": "c747843cfd14ed93ed58e98b32a4d6d7ea6fdd51",
          "url": "https://github.com/jmfiaschi/chewdata/commit/ba19741348171e01fca4c21c6c4d98b2bb09abea"
        },
        "date": 1639935171611,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 183,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 162,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 159381,
            "range": "± 369",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 24128,
            "range": "± 28",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 292,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 466,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "33b5a20a060261c1de4377c0a1e0b420eb57edf6",
          "message": "fix(bucket): lazyload the client",
          "timestamp": "2021-12-19T18:11:20+01:00",
          "tree_id": "c835ea2ef010ac83bb45c4ba46b1b5ab01f833a4",
          "url": "https://github.com/jmfiaschi/chewdata/commit/33b5a20a060261c1de4377c0a1e0b420eb57edf6"
        },
        "date": 1639935232289,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 187,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 182,
            "range": "± 21",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 177746,
            "range": "± 12984",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 25296,
            "range": "± 2340",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 303,
            "range": "± 22",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 550,
            "range": "± 39",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}