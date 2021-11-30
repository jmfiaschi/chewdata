window.BENCHMARK_DATA = {
  "lastUpdate": 1638309057159,
  "repoUrl": "https://github.com/jmfiaschi/chewdata",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "jm.fiaschi@gmail.com",
            "name": "jm.fiaschi",
            "username": "jmfiaschi"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c23edfac3616a62d3cea2108d5149acb6b06279f",
          "message": "feat(external_input_and_output): give the possibility to inject an input_receiver and output_sender (#12)\n\n* feat(bench): bench reader by format\r\n* feat(docker): build docker image",
          "timestamp": "2021-11-07T22:45:32+01:00",
          "tree_id": "93ce69c81a3bfb49f3556223b3d9cc32518eea5e",
          "url": "https://github.com/jmfiaschi/chewdata/commit/c23edfac3616a62d3cea2108d5149acb6b06279f"
        },
        "date": 1636323099306,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 213,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 189,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 204967,
            "range": "± 2836",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 33035,
            "range": "± 409",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 405,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 610,
            "range": "± 7",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "jm.fiaschi@gmail.com",
            "name": "jm.fiaschi",
            "username": "jmfiaschi"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "70afab8deb53938614b88bd2b951e95acc0d2159",
          "message": "feat(steps): remove the field wait (#13)\n\n* feat(docker): build docker image\r\n* feat(step): remove the field wait",
          "timestamp": "2021-11-29T00:14:35+01:00",
          "tree_id": "ee1c967773739f0d59ad87309a52cc8cee03260b",
          "url": "https://github.com/jmfiaschi/chewdata/commit/70afab8deb53938614b88bd2b951e95acc0d2159"
        },
        "date": 1638142863795,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 205,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 179,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 202325,
            "range": "± 5666",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 29170,
            "range": "± 1186",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 358,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 603,
            "range": "± 16",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "jm.fiaschi@gmail.com",
            "name": "jm.fiaschi",
            "username": "jmfiaschi"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "4accb4e46530d1e6a80804fd6a639fbe2bc66fa3",
          "message": "feat(tera): add object search by path (#14)\n\n* feat(tera): add object search by path\r\n\r\n* fix(lint): fix errors\r\n\r\n* fix(lint): fix warning",
          "timestamp": "2021-11-29T18:15:39+01:00",
          "tree_id": "524893c4e0c92f7e11941c579bc8b9afabbbce3d",
          "url": "https://github.com/jmfiaschi/chewdata/commit/4accb4e46530d1e6a80804fd6a639fbe2bc66fa3"
        },
        "date": 1638207228904,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 232,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 200,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 230195,
            "range": "± 24692",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 33413,
            "range": "± 3257",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 414,
            "range": "± 24",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 645,
            "range": "± 33",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "jm.fiaschi@gmail.com",
            "name": "jm.fiaschi",
            "username": "jmfiaschi"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "0f415b4b5d03b7979facc964c456086b51a41466",
          "message": "fix(transformer): give more detail on the tera errors (#15)\n\n* fix(transformer): give more detail on the tera errors",
          "timestamp": "2021-11-30T22:36:13+01:00",
          "tree_id": "b82859ce882902443c73c4900193ccec0a39ce2f",
          "url": "https://github.com/jmfiaschi/chewdata/commit/0f415b4b5d03b7979facc964c456086b51a41466"
        },
        "date": 1638309056552,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 172,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 157,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 169739,
            "range": "± 10399",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 25984,
            "range": "± 1440",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 300,
            "range": "± 19",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 461,
            "range": "± 22",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}