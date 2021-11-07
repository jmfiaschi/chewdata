window.BENCHMARK_DATA = {
  "lastUpdate": 1636323099927,
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
      }
    ]
  }
}