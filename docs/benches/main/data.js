window.BENCHMARK_DATA = {
  "lastUpdate": 1643427613931,
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
          "id": "5f0a565c853c9f46c3ce573ef509ad29824309d6",
          "message": "fix(eraser): erase data in static connector before to share new data (#16)\n\n* fix(eraser): erase data in static connector before to share new data",
          "timestamp": "2021-11-30T23:07:26+01:00",
          "tree_id": "e543a8476ba40ff10eb66e9c610dee82247c6bda",
          "url": "https://github.com/jmfiaschi/chewdata/commit/5f0a565c853c9f46c3ce573ef509ad29824309d6"
        },
        "date": 1638311049658,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 223,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 201,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 200607,
            "range": "± 8283",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 31582,
            "range": "± 660",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 367,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 570,
            "range": "± 19",
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
          "id": "b638908b9ed5325bb3d3c6da85d2d585a632b86c",
          "message": "fix(erase): can clear data in the document before and after a step (#17)\n\nfix(erase): can clear data in the document before and after a step",
          "timestamp": "2021-12-03T23:50:50+01:00",
          "tree_id": "911f9a9088d8fdbcf3a62c01bd7443abe1eed66e",
          "url": "https://github.com/jmfiaschi/chewdata/commit/b638908b9ed5325bb3d3c6da85d2d585a632b86c"
        },
        "date": 1638573372737,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 214,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 185,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 187864,
            "range": "± 10297",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 28457,
            "range": "± 1451",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 341,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 528,
            "range": "± 21",
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
          "id": "0cd6b09e5f8cf8202350faa81404b4f21b70b252",
          "message": "fix(bucket): use the DefaultCredentialsProvider by default (#18)\n\n* fix(bucket): use the DefaultCredentialsProvider by default\r\n* fix(cargo): improve package description",
          "timestamp": "2021-12-06T23:28:46+01:00",
          "tree_id": "663eca4d24d2d3af0c4b3e152fca843694a38f65",
          "url": "https://github.com/jmfiaschi/chewdata/commit/0cd6b09e5f8cf8202350faa81404b4f21b70b252"
        },
        "date": 1638830954231,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 185,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 168,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 159355,
            "range": "± 562",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 24313,
            "range": "± 33",
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
            "value": 458,
            "range": "± 0",
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
          "id": "25d52c556abcea926322922b554f2e612391a38c",
          "message": "feat(tera): remove set_env function\n\n* fix(cargo): update version automatically",
          "timestamp": "2021-12-19T22:02:20+01:00",
          "tree_id": "30500faaed6000218dbcf43f4f8f7f2e1299bd54",
          "url": "https://github.com/jmfiaschi/chewdata/commit/25d52c556abcea926322922b554f2e612391a38c"
        },
        "date": 1639949100412,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 163,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 147,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 140696,
            "range": "± 671",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 20610,
            "range": "± 36",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 260,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 405,
            "range": "± 0",
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
          "id": "77469bb9e72bd05120a08bfcc88be43a9341b7f4",
          "message": "fix(step_context): add step_context to avoid variable names collision (#20)",
          "timestamp": "2021-12-20T18:46:09+01:00",
          "tree_id": "f0d5c2c52e23fe6481eaac21d1bb3f0a696e793e",
          "url": "https://github.com/jmfiaschi/chewdata/commit/77469bb9e72bd05120a08bfcc88be43a9341b7f4"
        },
        "date": 1640023655775,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 189,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 166,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 164299,
            "range": "± 675",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 23308,
            "range": "± 17",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 290,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 455,
            "range": "± 0",
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
          "id": "895ef8689034cd5c22ee85c12c0690aecfa4f937",
          "message": "feat(validator): Add a validator step\n\n* feat(validator): Add a validator step\r\n* fix(step_context): add step_context to avoid variable names collision\r\n* feat(validator): Add tests and docs\r\n* feat(step): replace alias by name to identify a step",
          "timestamp": "2021-12-29T10:39:26+01:00",
          "tree_id": "4de10c3da498588c5fd315342751364b7f0da91b",
          "url": "https://github.com/jmfiaschi/chewdata/commit/895ef8689034cd5c22ee85c12c0690aecfa4f937"
        },
        "date": 1640772237245,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 219,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 194,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 199040,
            "range": "± 13394",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 29449,
            "range": "± 1106",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 343,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 577,
            "range": "± 27",
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
          "id": "f8b2cadfb11f0b42fdd69e92b2669d3fdbdff3fd",
          "message": "feat(reader): use offset/cursor paginator with iterative/concurrency mode (#22)\n\n* feat(quality): forbid unsafe code\r\n* fix(thread): replace blocking threads code by non blocking threads code",
          "timestamp": "2022-01-29T02:24:43+01:00",
          "tree_id": "6335ab2cd682525bc8cce3b413c33de6936a6335",
          "url": "https://github.com/jmfiaschi/chewdata/commit/f8b2cadfb11f0b42fdd69e92b2669d3fdbdff3fd"
        },
        "date": 1643421037293,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 140,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 124,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 141477,
            "range": "± 246",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 20864,
            "range": "± 25",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 238,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 393,
            "range": "± 0",
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
          "id": "f8b2cadfb11f0b42fdd69e92b2669d3fdbdff3fd",
          "message": "feat(reader): use offset/cursor paginator with iterative/concurrency mode (#22)\n\n* feat(quality): forbid unsafe code\r\n* fix(thread): replace blocking threads code by non blocking threads code",
          "timestamp": "2022-01-29T02:24:43+01:00",
          "tree_id": "6335ab2cd682525bc8cce3b413c33de6936a6335",
          "url": "https://github.com/jmfiaschi/chewdata/commit/f8b2cadfb11f0b42fdd69e92b2669d3fdbdff3fd"
        },
        "date": 1643427612603,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 187,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 166,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 196036,
            "range": "± 14595",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 29453,
            "range": "± 1147",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 303,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 535,
            "range": "± 32",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}