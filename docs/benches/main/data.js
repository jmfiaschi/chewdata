window.BENCHMARK_DATA = {
  "lastUpdate": 1716150791040,
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
          "id": "0839281840df4d16aba2c7a955e33530830eef42",
          "message": "feat(parquet): handle parquet document (#23)\n\n* chore(deny_unknown_fields): add , deny_unknown_fields to avoid understanding errors\r\n* chore(parquet): add read & write parquet file\r\n* fix(connector): add metadata variable for resolving path\r\n* fix(project): use Vec<u8> instead of str to avoid UTF8 error and simply the code",
          "timestamp": "2022-05-11T21:46:07+02:00",
          "tree_id": "4460ed05ff0a866822b325a0bbedd5031aedcc74",
          "url": "https://github.com/jmfiaschi/chewdata/commit/0839281840df4d16aba2c7a955e33530830eef42"
        },
        "date": 1652300551496,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 4963,
            "range": "± 22",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 4875,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 237920,
            "range": "± 799",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 30037,
            "range": "± 63",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 16884,
            "range": "± 41",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 15772,
            "range": "± 45",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 289830,
            "range": "± 406",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 24363,
            "range": "± 55",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 24267,
            "range": "± 59",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 24360,
            "range": "± 56",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 24344,
            "range": "± 60",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 24495,
            "range": "± 62",
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
          "id": "0839281840df4d16aba2c7a955e33530830eef42",
          "message": "feat(parquet): handle parquet document (#23)\n\n* chore(deny_unknown_fields): add , deny_unknown_fields to avoid understanding errors\r\n* chore(parquet): add read & write parquet file\r\n* fix(connector): add metadata variable for resolving path\r\n* fix(project): use Vec<u8> instead of str to avoid UTF8 error and simply the code",
          "timestamp": "2022-05-11T21:46:07+02:00",
          "tree_id": "4460ed05ff0a866822b325a0bbedd5031aedcc74",
          "url": "https://github.com/jmfiaschi/chewdata/commit/0839281840df4d16aba2c7a955e33530830eef42"
        },
        "date": 1652303576345,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 5420,
            "range": "± 60",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 5295,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 330047,
            "range": "± 793",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 35784,
            "range": "± 47",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 18369,
            "range": "± 44",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 18515,
            "range": "± 17",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 371261,
            "range": "± 9332",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 28874,
            "range": "± 76",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 29415,
            "range": "± 59",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 29051,
            "range": "± 75",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 29168,
            "range": "± 55",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 29286,
            "range": "± 69",
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
          "id": "0839281840df4d16aba2c7a955e33530830eef42",
          "message": "feat(parquet): handle parquet document (#23)\n\n* chore(deny_unknown_fields): add , deny_unknown_fields to avoid understanding errors\r\n* chore(parquet): add read & write parquet file\r\n* fix(connector): add metadata variable for resolving path\r\n* fix(project): use Vec<u8> instead of str to avoid UTF8 error and simply the code",
          "timestamp": "2022-05-11T21:46:07+02:00",
          "tree_id": "4460ed05ff0a866822b325a0bbedd5031aedcc74",
          "url": "https://github.com/jmfiaschi/chewdata/commit/0839281840df4d16aba2c7a955e33530830eef42"
        },
        "date": 1652608732860,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 4877,
            "range": "± 339",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 4932,
            "range": "± 311",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 287842,
            "range": "± 20893",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 34443,
            "range": "± 1346",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 17171,
            "range": "± 854",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 15596,
            "range": "± 1169",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 343959,
            "range": "± 27676",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 24948,
            "range": "± 1848",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 24561,
            "range": "± 1558",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 24893,
            "range": "± 2146",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 24992,
            "range": "± 1691",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 25847,
            "range": "± 1832",
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
          "id": "d97e0743776cc6c10a20e435426c9db11c894371",
          "message": "feat(asw_sdk): replace rusoto and remove hardcoding credentials (#32)\n\n* feat(asw_sdk): replace rusoto and remove hardcoding credentials\r\n\r\n* chore(lint): fix lint\r\n\r\n* fix(tokio): replace tokio macro by async_std\r\n\r\n* chore(test): improve test speed\r\n\r\n* feat(cargo): replace crossbeam by async-channel\r\n\r\n* feat(cargo): upgrade uuid",
          "timestamp": "2022-05-21T18:37:39+02:00",
          "tree_id": "1491177aa350793a5905e834e19814bc352e7e7e",
          "url": "https://github.com/jmfiaschi/chewdata/commit/d97e0743776cc6c10a20e435426c9db11c894371"
        },
        "date": 1653153727257,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 6288,
            "range": "± 295",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 6083,
            "range": "± 233",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 329770,
            "range": "± 10570",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 36893,
            "range": "± 1696",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 19051,
            "range": "± 1255",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 19098,
            "range": "± 17",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 410476,
            "range": "± 20827",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 29810,
            "range": "± 90",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 31015,
            "range": "± 2638",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 30839,
            "range": "± 1001",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 30908,
            "range": "± 1525",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 30321,
            "range": "± 1034",
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
          "id": "e8fb7b1e56bce52680e8b969b685c0bbf2856cd7",
          "message": "fix(io): the stream return only one connector (#33)\n\n* fix(io): the stream return only one connector\r\n\r\n* feat(feature): refacto feature names\r\n\r\n* feat(psql): add psql connector feature\r\n\r\n* refacto(project): * add timeout for curl",
          "timestamp": "2022-07-27T09:26:27+02:00",
          "tree_id": "ca0892cb23390a2d2c490e476483a898234498a8",
          "url": "https://github.com/jmfiaschi/chewdata/commit/e8fb7b1e56bce52680e8b969b685c0bbf2856cd7"
        },
        "date": 1658909094180,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 6902,
            "range": "± 420",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 7011,
            "range": "± 459",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 289906,
            "range": "± 20469",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 32763,
            "range": "± 4388",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 15068,
            "range": "± 1473",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 16017,
            "range": "± 1061",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 407340,
            "range": "± 43037",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 25048,
            "range": "± 1637",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 24577,
            "range": "± 1609",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 24426,
            "range": "± 1769",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 24833,
            "range": "± 1338",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 24163,
            "range": "± 1383",
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
          "id": "94dbf01e41e236f45f3f70b7bf03d585c13785eb",
          "message": "fix(makefile):  fix run command (#34)\n\n* fix(makefile):  fix run command",
          "timestamp": "2022-07-29T23:49:20+02:00",
          "tree_id": "f153039a670563e80a986377e493490dfff6f4d1",
          "url": "https://github.com/jmfiaschi/chewdata/commit/94dbf01e41e236f45f3f70b7bf03d585c13785eb"
        },
        "date": 1659133872654,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 7343,
            "range": "± 216",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 7287,
            "range": "± 204",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 318974,
            "range": "± 4422",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 36021,
            "range": "± 566",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 16996,
            "range": "± 200",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 18248,
            "range": "± 251",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 386327,
            "range": "± 5706",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 29190,
            "range": "± 467",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 29769,
            "range": "± 312",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 30541,
            "range": "± 2123",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 29165,
            "range": "± 479",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 28887,
            "range": "± 467",
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
          "id": "f286108cceebf1580e084c733c4e12e064683884",
          "message": "release 1.13.0 (#37)\n\n* fix(curl): parameters can have value paginator.next for cursor paginator\r\n\r\n* fix(.env): remove CARGO_INCREMENTAL=1\r\n\r\n* fix(cargo): use only postgres for sqlx\r\n\r\n* feat(release): replace semantic-release-rust by standard cli\r\n\r\n* refactor(connector): remove paginator::stream mutable\r\n\r\n* refacto(jwt): reword token value & token name\r\n\r\n* perf(send & fetch): replace &box(T) by  &T\r\n\r\n* lint(documents): convert &vec[u8] into &[u8]\r\n\r\n* feat(examples): add example for psql\r\n\r\n* fix(psql): query sanitized and add example\r\n\r\n* feat(jwt): with Keycloak",
          "timestamp": "2023-01-15T22:39:56+01:00",
          "tree_id": "0f21c7688121123d3e8db7676a396651dbde38cb",
          "url": "https://github.com/jmfiaschi/chewdata/commit/f286108cceebf1580e084c733c4e12e064683884"
        },
        "date": 1673820418367,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 5261,
            "range": "± 29",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 5165,
            "range": "± 20",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 230429,
            "range": "± 1420",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 28082,
            "range": "± 54",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 11526,
            "range": "± 45",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 16849,
            "range": "± 66",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 314209,
            "range": "± 2351",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 25276,
            "range": "± 33",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 25761,
            "range": "± 44",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 25422,
            "range": "± 50",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 25691,
            "range": "± 55",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 25646,
            "range": "± 41",
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
          "id": "9d708b60fcc62c696f0af753c5b4b11bc496f63b",
          "message": "feat(rabbitmq): support publish & consume (#38)\n\n* fix(json): write better array and handle empty data with {}\r\n\r\n* fix(jsonl): write better array and handle empty data with []\r\n\r\n* feat(tracing): add tracing-log and  display lib logs\r\n\r\n* feat(base64): add filters encode & decode\r\n\r\n* refactor(reader): simplify the code\r\n\r\n* feat(curl): fetch can have a body for POST/PATCH/PUT\r\n\r\n* feat(rabbitmq):  publish and consume data\r\n\r\n* refactor(jwt): replace send by fetch data\r\n\r\n* refactor(example): rename files",
          "timestamp": "2023-01-28T21:48:11+01:00",
          "tree_id": "426fcdd93a5ce9d22dfc5867e3b264db0cef2671",
          "url": "https://github.com/jmfiaschi/chewdata/commit/9d708b60fcc62c696f0af753c5b4b11bc496f63b"
        },
        "date": 1674940755057,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 6040,
            "range": "± 365",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 5598,
            "range": "± 231",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 289824,
            "range": "± 27682",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 29924,
            "range": "± 1431",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 11850,
            "range": "± 524",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 18541,
            "range": "± 1304",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 335781,
            "range": "± 19127",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 27897,
            "range": "± 1547",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 27304,
            "range": "± 1483",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 27446,
            "range": "± 1082",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 27260,
            "range": "± 1219",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 27598,
            "range": "± 1300",
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
          "id": "738caf1dec709922e0806cd7c596df185b10e623",
          "message": "feat: add APM\n\n* refactor: replace for...in by while let ... = ....\r\n\r\n* feat(monitoring): add jaeger tracing\r\n\r\n* feat(monitoring): add APM feature",
          "timestamp": "2023-02-08T21:23:02+01:00",
          "tree_id": "0a94674a1e0c1d38eb962c5ce4468c92d8bf0a72",
          "url": "https://github.com/jmfiaschi/chewdata/commit/738caf1dec709922e0806cd7c596df185b10e623"
        },
        "date": 1675889676885,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 5458,
            "range": "± 54",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 5375,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 232269,
            "range": "± 1151",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 29785,
            "range": "± 161",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 11448,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 17429,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 312507,
            "range": "± 853",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 22705,
            "range": "± 37",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 26394,
            "range": "± 31",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 26089,
            "range": "± 57",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 26071,
            "range": "± 27",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 23073,
            "range": "± 46",
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
          "id": "819fa0d81f19a984a5c2ed4904d4c97d3859a262",
          "message": "feat(parquet): upgrade versions and improve code (#42)\n\n* doc(readme): how to change log level\r\n* feat(cargo): upgrade versions\r\n* feat(cargo): upgrade versions for toml & bucket\r\n* feat(example): update tracing\r\n* fix(main): enable opentelemetry if apm feature declared\r\n* fix(makefile): set number of // jobs\r\n* feat(release): speedup the CI",
          "timestamp": "2023-08-09T08:45:21+02:00",
          "tree_id": "30643008a0c4f5064c968072c8d28bc60afcfdac",
          "url": "https://github.com/jmfiaschi/chewdata/commit/819fa0d81f19a984a5c2ed4904d4c97d3859a262"
        },
        "date": 1691565930981,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 5727,
            "range": "± 284",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 5634,
            "range": "± 263",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 436182,
            "range": "± 23375",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 33495,
            "range": "± 1671",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 15874,
            "range": "± 458",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 22293,
            "range": "± 1164",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 333854,
            "range": "± 14098",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 40206,
            "range": "± 1339",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 41194,
            "range": "± 2183",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 39808,
            "range": "± 1779",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 40723,
            "range": "± 1759",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 41124,
            "range": "± 3084",
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
          "id": "40cd6d9a94f8bb24944558ddfd468d9a6e30f264",
          "message": "feat(xml): replace jxon by quick-xml (#43)\n\n* fix(async-std): use default option to avoid issue with --no-default-features\r\n\r\n* fix(local): remove useless features by default\r\n\r\n* fix(features): fix compile error when run features one by one\r\n\r\n* fix(features): specify features to test\r\n\r\n* doc(help): add more usage examples\r\n\r\n* chore(parquet): fix warning lint\r\n\r\n* feat(xml): remove jxon library in order to use quick-xml\r\n\r\n* fix(xml): add xml2json only if xml feature enable",
          "timestamp": "2023-08-23T19:08:48+02:00",
          "tree_id": "caa7f08e45432f05d30a649c9f32d8b34439228f",
          "url": "https://github.com/jmfiaschi/chewdata/commit/40cd6d9a94f8bb24944558ddfd468d9a6e30f264"
        },
        "date": 1692812634101,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 4516,
            "range": "± 305",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 4413,
            "range": "± 288",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 470001,
            "range": "± 25084",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 28005,
            "range": "± 3244",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 13059,
            "range": "± 913",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 16874,
            "range": "± 1089",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 273808,
            "range": "± 16381",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 30237,
            "range": "± 1885",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 30948,
            "range": "± 1421",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 30456,
            "range": "± 1773",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 30295,
            "range": "± 2077",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 30075,
            "range": "± 1659",
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
          "id": "5473c7cd20d057da16abe1a64d93ca4b0ca4c201",
          "message": "feat(configuration): support hjson in the configuration by default (#44)\n\n* refacto(context): rename context's fields\r\n\r\n* feat(local): erase multi files with wildcard in the path.\r\n\r\n* fix(json): write an array generate data without terminator\r\n\r\n* fix(transformer): if new result contain array, the transformer send each element from the array\r\n\r\n* feat(configuration): support hjson in the configuration by default\r\n\r\n* chore(cargo): upgrade version",
          "timestamp": "2023-08-29T09:37:02+02:00",
          "tree_id": "bfa4008526f3b215317b56abdb52755d7c33fdc8",
          "url": "https://github.com/jmfiaschi/chewdata/commit/5473c7cd20d057da16abe1a64d93ca4b0ca4c201"
        },
        "date": 1693296540558,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 4791,
            "range": "± 28",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 4657,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 340202,
            "range": "± 3154",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 29515,
            "range": "± 29",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 13007,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 18639,
            "range": "± 87",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 193530,
            "range": "± 1143",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 28812,
            "range": "± 86",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 29202,
            "range": "± 57",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 29470,
            "range": "± 46",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 28709,
            "range": "± 77",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 29387,
            "range": "± 52",
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
          "id": "1cc3e56006129da90de17dabe39bb4076399a5d0",
          "message": "feat(connectors): use OnceLock for lazy load client (#45)\n\n* fix(main): accept hjson file extension\r\n* docs(how_it_works): add list of steps pointing to the documentation\r\n* refactor(sleep): remove the property and use the native code\r\n* feat: upgrade version\r\n* reuse clients\r\n* simplify logs\r\n* enable cache backend\r\n* Remove useless clone and improve performance\r\n* refacto paginator, add paginate method and improve the reading in concurrency\r\nBREAKING CHANGE: for transformer step, remove step's input/output paramaters and use by default 'input'/'output' variable in the pattern action\r\nBREAKING CHANGE: rename curl fields\r\nBREAKING CHANGE: simplify autheticator and use it as a middleware\r\nBREAKING CHANGE: remove description attributes and use hjson/yaml configuration formats\r\n* fix(release): add missing dependency\r\n* feat(updater): add function & filter env(name=key) or val ¦ env(name=key) ¦ ....\r\n* feat(s3): upgrade version\r\n* feat(minio): upgrade configuration\r\n* feat(bucket): align bucket variables\r\n* feat(bucket): Apply region and endpoint in this priority :\r\n1 - from the config file\r\n2 - from bucket env\r\n3 - from aws env",
          "timestamp": "2023-12-14T20:57:49+01:00",
          "tree_id": "fdd5e4d1f7c4ef8e8a788cb38f8baf3254937b92",
          "url": "https://github.com/jmfiaschi/chewdata/commit/1cc3e56006129da90de17dabe39bb4076399a5d0"
        },
        "date": 1702585003777,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 4713,
            "range": "± 77",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 4625,
            "range": "± 23",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 296400,
            "range": "± 6163",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 20042,
            "range": "± 102",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 10775,
            "range": "± 31",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 13564,
            "range": "± 46",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 122241,
            "range": "± 1310",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 19162,
            "range": "± 113",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 19474,
            "range": "± 115",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 19306,
            "range": "± 108",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 19463,
            "range": "± 193",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 19317,
            "range": "± 334",
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
            "email": "jm.fiaschi@gmail.com",
            "name": "Jean-Marc Fiaschi",
            "username": "jmfiaschi"
          },
          "distinct": true,
          "id": "5f81b7ef5cd11eada53e5e6e394d740d6b929bce",
          "message": "feat(document): add byte format (#46)\n\n* fix(bearer): is_base64 specify if the token is already encoded or not. If not, it will be encoded.\n\n* fix(log): log details not visible even with RUST_LOG=trace\n\n* feat(document): add byte format\n\n* chore(connector): hide sensible data",
          "timestamp": "2023-12-19T21:44:23+01:00",
          "tree_id": "03e35dda5d05877b28fceb718951a4020d27dc9a",
          "url": "https://github.com/jmfiaschi/chewdata/commit/5f81b7ef5cd11eada53e5e6e394d740d6b929bce"
        },
        "date": 1703019581191,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 5258,
            "range": "± 200",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 5120,
            "range": "± 109",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 296001,
            "range": "± 2839",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 19722,
            "range": "± 963",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 10831,
            "range": "± 28",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 13747,
            "range": "± 102",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 121609,
            "range": "± 1002",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 19259,
            "range": "± 330",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 19344,
            "range": "± 209",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 19300,
            "range": "± 78",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 19497,
            "range": "± 98",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 19362,
            "range": "± 243",
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
          "id": "b17a00476857a6af1ebbe5678f4381d97a438a7e",
          "message": "feat(jwt): remove \"token_name\" and use the document.entry_path instead. Replace also format field by signing_type. (#47)",
          "timestamp": "2023-12-20T01:11:12+01:00",
          "tree_id": "1eed4aee19bfe4d621fecd769463073b9534a8a0",
          "url": "https://github.com/jmfiaschi/chewdata/commit/b17a00476857a6af1ebbe5678f4381d97a438a7e"
        },
        "date": 1703031842810,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 4754,
            "range": "± 25",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 4585,
            "range": "± 29",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 295321,
            "range": "± 6417",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 19908,
            "range": "± 127",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 11196,
            "range": "± 92",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 13714,
            "range": "± 69",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 122316,
            "range": "± 775",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 19355,
            "range": "± 114",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 19593,
            "range": "± 403",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 19608,
            "range": "± 80",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 19520,
            "range": "± 131",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 19413,
            "range": "± 129",
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
          "id": "f43fe7acd036959ce864da6c19608937b47a16fe",
          "message": "fix(curl): set count_type optional and None by default",
          "timestamp": "2023-12-20T23:13:22+01:00",
          "tree_id": "bf834256f9440c0f17ad5880acb18442edd8c324",
          "url": "https://github.com/jmfiaschi/chewdata/commit/f43fe7acd036959ce864da6c19608937b47a16fe"
        },
        "date": 1703111228101,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 4729,
            "range": "± 72",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 4647,
            "range": "± 56",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 296208,
            "range": "± 5764",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 19830,
            "range": "± 121",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 10617,
            "range": "± 192",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 13745,
            "range": "± 661",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 126861,
            "range": "± 1106",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 19511,
            "range": "± 314",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 19689,
            "range": "± 114",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 19639,
            "range": "± 82",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 19897,
            "range": "± 161",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 19752,
            "range": "± 724",
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
          "id": "4a121df6058a39d550fda7cbe120cfd0aeacb57f",
          "message": "fix(curl): paginator iterate until reach body without data if the counter is not set. (#49)\n\n* feat(updater): add filter \"find\" and retreive all text match the pattern\r\n\r\n* chore(test_set_env): refacto\r\n\r\n* chore(tera function): improve code and documentation\r\n\r\n* fix(curl): paginator iterate until reach body without data if the counter is not set.\r\n\r\n* fix(keycloak): set a timeout for unit test",
          "timestamp": "2023-12-29T10:55:46+01:00",
          "tree_id": "f74bef0bcd3e51bbe3e4cbd8d6743e3895045cf0",
          "url": "https://github.com/jmfiaschi/chewdata/commit/4a121df6058a39d550fda7cbe120cfd0aeacb57f"
        },
        "date": 1703844768586,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 5280,
            "range": "± 27",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 5235,
            "range": "± 21",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 297301,
            "range": "± 2156",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 20935,
            "range": "± 98",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 10711,
            "range": "± 44",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 14887,
            "range": "± 84",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 141697,
            "range": "± 689",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 20044,
            "range": "± 111",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 20141,
            "range": "± 220",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 20032,
            "range": "± 107",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 20013,
            "range": "± 1088",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 20080,
            "range": "± 106",
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
            "email": "jm.fiaschi@gmail.com",
            "name": "Jean-Marc Fiaschi",
            "username": "jmfiaschi"
          },
          "distinct": true,
          "id": "7321e893e4c278f7b1b4ec4dbe12f243f9ded7fe",
          "message": "feat(updater): add filter \"find\" and retreive all text match the pattern\n\n* chore(test_set_env): refacto\n\n* chore(tera function): improve code and documentation\n\n* fix(curl): paginator iterate until reach body without data if the counter is not set.\n\n* fix(keycloak): set a timeout for unit test",
          "timestamp": "2023-12-29T11:38:34+01:00",
          "tree_id": "f74bef0bcd3e51bbe3e4cbd8d6743e3895045cf0",
          "url": "https://github.com/jmfiaschi/chewdata/commit/7321e893e4c278f7b1b4ec4dbe12f243f9ded7fe"
        },
        "date": 1703847054739,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 5187,
            "range": "± 37",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 5174,
            "range": "± 185",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 295001,
            "range": "± 4792",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 19791,
            "range": "± 88",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 10412,
            "range": "± 42",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 13502,
            "range": "± 63",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 138439,
            "range": "± 919",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 19901,
            "range": "± 170",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 19994,
            "range": "± 107",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 20114,
            "range": "± 113",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 19981,
            "range": "± 1157",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 20064,
            "range": "± 146",
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
          "id": "5f457b16ec2ff58e875917ea72362178bf435ef7",
          "message": "feat(updater): add new filter/function find for tera. (#50)\n\n* feat(updater): add new filter/function `find` for tera.",
          "timestamp": "2024-01-03T10:20:53+01:00",
          "tree_id": "eeecb9a744cee3c9991ff3d97c28eb89604bc24f",
          "url": "https://github.com/jmfiaschi/chewdata/commit/5f457b16ec2ff58e875917ea72362178bf435ef7"
        },
        "date": 1704274410406,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 5214,
            "range": "± 25",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 5085,
            "range": "± 90",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 296694,
            "range": "± 3390",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 20411,
            "range": "± 140",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 10449,
            "range": "± 33",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 13739,
            "range": "± 98",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 140326,
            "range": "± 1010",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 19803,
            "range": "± 116",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 19955,
            "range": "± 134",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 19978,
            "range": "± 141",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 19920,
            "range": "± 603",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 19763,
            "range": "± 113",
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
          "id": "a71928910d8aaaee4a886ff10162691969d210d3",
          "message": "feat(updater): add new filter/function `extract` for tera. Extraction attributes from an object or list of object. (#51)",
          "timestamp": "2024-01-04T16:02:02+01:00",
          "tree_id": "a4f54b7d5648e0584abfa24bf2c9b7e35c108aee",
          "url": "https://github.com/jmfiaschi/chewdata/commit/a71928910d8aaaee4a886ff10162691969d210d3"
        },
        "date": 1704381280666,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 5801,
            "range": "± 129",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 5666,
            "range": "± 63",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 295727,
            "range": "± 3008",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 20342,
            "range": "± 262",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 10480,
            "range": "± 81",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 13960,
            "range": "± 154",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 140025,
            "range": "± 1003",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 19511,
            "range": "± 141",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 19681,
            "range": "± 153",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 19701,
            "range": "± 146",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 19723,
            "range": "± 117",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 19621,
            "range": "± 144",
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
          "id": "bf5e7443986f00729e7c841c3e803b7cb9aa8871",
          "message": "fix(extract): able to extract from a object a list of attribute. allow to use regex. (#52)\n\n* fix(csv): find the most deep object and not miss any columns to write in the csv file.\r\n\r\n* fix(json_pointer): allow to escape `.` if an attribute contains this value.",
          "timestamp": "2024-01-09T09:20:22+01:00",
          "tree_id": "21c835b036a699c804d20deabe0122d95174a3e1",
          "url": "https://github.com/jmfiaschi/chewdata/commit/bf5e7443986f00729e7c841c3e803b7cb9aa8871"
        },
        "date": 1704789197242,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 5398,
            "range": "± 42",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 5328,
            "range": "± 27",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 295663,
            "range": "± 2359",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 20372,
            "range": "± 93",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 10760,
            "range": "± 31",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 13880,
            "range": "± 62",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 139799,
            "range": "± 838",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 19103,
            "range": "± 195",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 19090,
            "range": "± 120",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 19226,
            "range": "± 334",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 19382,
            "range": "± 148",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 19190,
            "range": "± 99",
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
          "id": "d5e7d29caf532c27321ad5d4e58a4ed947ee8698",
          "message": "feat(extract): add merge_replace method for Value. Same as value.merge() but instead of append elements in a array, keep the same position and merge Value. (#53)\n\n* fix(csv): find the most deep object and not miss any columns to write in the csv file.\r\n* fix(json_pointer): all to escape `.`if an attribute contain this value.\r\n* fix(extract): able to extract from a object a list of attribute. allow regex.",
          "timestamp": "2024-01-11T09:32:33+01:00",
          "tree_id": "604c975e355a843403e3f856e9bb64c5fac7954a",
          "url": "https://github.com/jmfiaschi/chewdata/commit/d5e7d29caf532c27321ad5d4e58a4ed947ee8698"
        },
        "date": 1704962727075,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 5581,
            "range": "± 129",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 5540,
            "range": "± 50",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 297985,
            "range": "± 2602",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 20174,
            "range": "± 200",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 10129,
            "range": "± 75",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 13819,
            "range": "± 52",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 144753,
            "range": "± 2003",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 19214,
            "range": "± 1001",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 19330,
            "range": "± 299",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 19258,
            "range": "± 686",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 19444,
            "range": "± 117",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 19365,
            "range": "± 100",
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
          "id": "8512b4cb696a17d6df214a6d1965c1ba342d5ad5",
          "message": "feat(local): add cache for local connector (#54)\n\n* fix(apm): fix error with pretty and jaeger",
          "timestamp": "2024-01-11T22:24:28+01:00",
          "tree_id": "440a90220be0f823abab26c12bbd869352efba5d",
          "url": "https://github.com/jmfiaschi/chewdata/commit/8512b4cb696a17d6df214a6d1965c1ba342d5ad5"
        },
        "date": 1705009038086,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 5393,
            "range": "± 32",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 5295,
            "range": "± 25",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 297869,
            "range": "± 1811",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 20122,
            "range": "± 148",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 10956,
            "range": "± 41",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 13617,
            "range": "± 89",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 138078,
            "range": "± 961",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 19344,
            "range": "± 128",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 19400,
            "range": "± 90",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 19394,
            "range": "± 104",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 19518,
            "range": "± 74",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 19412,
            "range": "± 408",
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
          "id": "5be00d492b8aab9f5b7f1853929eff386d751e6e",
          "message": "feat(referential): group in a struct and add cache for none dynamic connector (#55)\n\n* chore(paginator): remove useless clone\r\n* feat(referential): group in a struct and add cache for none dynamic connector.\r\n* fix(referential): doc\r\n* fix(semantic-release): node version >=20.8.1 is required. Found v18.19.0",
          "timestamp": "2024-01-16T13:58:37+01:00",
          "tree_id": "827a7dac5aae48e38debd3874c54889ed8c2e99a",
          "url": "https://github.com/jmfiaschi/chewdata/commit/5be00d492b8aab9f5b7f1853929eff386d751e6e"
        },
        "date": 1705410849992,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 5505,
            "range": "± 85",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 5488,
            "range": "± 25",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 293389,
            "range": "± 2638",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 20531,
            "range": "± 306",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 10176,
            "range": "± 85",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 13618,
            "range": "± 38",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 138621,
            "range": "± 4229",
            "unit": "ns/iter"
          },
          {
            "name": "words/",
            "value": 19724,
            "range": "± 84",
            "unit": "ns/iter"
          },
          {
            "name": "sentences/",
            "value": 19780,
            "range": "± 98",
            "unit": "ns/iter"
          },
          {
            "name": "paragraphs/",
            "value": 19788,
            "range": "± 106",
            "unit": "ns/iter"
          },
          {
            "name": "phone_number/",
            "value": 19794,
            "range": "± 137",
            "unit": "ns/iter"
          },
          {
            "name": "password/",
            "value": 19897,
            "range": "± 126",
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
          "id": "acd1126a412ca975238efdbf96a4addea06cfca0",
          "message": "perf(transform): use tera::Context::from_value instead of tera_context.insert with Value serialization. (#56)",
          "timestamp": "2024-01-30T08:49:52+01:00",
          "tree_id": "1107b2d211c4f1e5ad87e50eb0eebeec425fd415",
          "url": "https://github.com/jmfiaschi/chewdata/commit/acd1126a412ca975238efdbf96a4addea06cfca0"
        },
        "date": 1706601997194,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 5203,
            "range": "± 77",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 5112,
            "range": "± 17",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 298086,
            "range": "± 1662",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 20029,
            "range": "± 76",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 10804,
            "range": "± 521",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 13742,
            "range": "± 69",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 140952,
            "range": "± 2323",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "jm.fiaschi@gmail.com",
            "name": "Jean-Marc Fiaschi",
            "username": "jmfiaschi"
          },
          "committer": {
            "email": "jm.fiaschi@gmail.com",
            "name": "Jean-Marc Fiaschi",
            "username": "jmfiaschi"
          },
          "distinct": true,
          "id": "e24aeb20992727a3f4bf4e7afc5bdaedfd3586c6",
          "message": "chore(Cargo): fix the version",
          "timestamp": "2024-01-30T09:56:53+01:00",
          "tree_id": "63b41ac2a9a80d0db36763c66c021440849a2541",
          "url": "https://github.com/jmfiaschi/chewdata/commit/e24aeb20992727a3f4bf4e7afc5bdaedfd3586c6"
        },
        "date": 1706605597393,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 5260,
            "range": "± 36",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 5167,
            "range": "± 69",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 296467,
            "range": "± 3856",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 20447,
            "range": "± 139",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 10582,
            "range": "± 128",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 13898,
            "range": "± 190",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 137862,
            "range": "± 930",
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
          "id": "81fa6f602197b51472db091a13f209a57aa9c4e6",
          "message": "feat(json/jsonl): write entry_path if define (#57)\n\n* chore(write): improve logs\r\n* feat(local): add checksum generation and validation\r\n* feat(json/jsonl): write entry_path if define\r\n* feat(curl): remove \" for data with \"x-www-form-urlencoded\"",
          "timestamp": "2024-02-06T08:29:22+01:00",
          "tree_id": "0638d00c78acb9ce80ef963c0c68ee77b773c0ce",
          "url": "https://github.com/jmfiaschi/chewdata/commit/81fa6f602197b51472db091a13f209a57aa9c4e6"
        },
        "date": 1707205269213,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 5548,
            "range": "± 163",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 5474,
            "range": "± 34",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 298184,
            "range": "± 4238",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 20776,
            "range": "± 418",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 10894,
            "range": "± 39",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 13716,
            "range": "± 50",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 137046,
            "range": "± 1140",
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
          "id": "0fc663f7820f5b2bd22fb13ff15007466dd99d19",
          "message": "feat(curl): handle redirection (#58)\n\n* feat(writer): display the total amount of data written.\r\n\r\n* fix(connector): raise error if path not fully resolved\r\n\r\n* feat(curl): add redirection limit\r\n\r\n* feat(reader): If context in input and the connector failed, forward in error the context\r\n\r\n* fix(text): mime type and subtype\r\n\r\n* fix(document type guesser): add jsonl and txt\r\n\r\n* fix(byte): subtype mime\r\n\r\n* fix(json/jsonl): set entry_path to none if empty\r\n\r\n* fix(parquet): set entry_path to none if empty\r\n\r\n* fix(test): add exception in assertion\r\n\r\n* feat(curl): handle redirection\r\n\r\n* feat(jwt): remove payload and token_entry. reuse document.entry_path and refresh_connector.parameters",
          "timestamp": "2024-03-08T22:29:00+01:00",
          "tree_id": "2e9e09c7fedb08e0d411697ccb75a4e7250b9b12",
          "url": "https://github.com/jmfiaschi/chewdata/commit/0fc663f7820f5b2bd22fb13ff15007466dd99d19"
        },
        "date": 1709934338669,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 5441,
            "range": "± 122",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 5313,
            "range": "± 23",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 298395,
            "range": "± 3707",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 22071,
            "range": "± 125",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 10450,
            "range": "± 38",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 13847,
            "range": "± 78",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 141254,
            "range": "± 1755",
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
          "id": "3c2fd034b0cf633650d88350e8bb10c8c1d03cd2",
          "message": "fix(keycloak): adapt test with new version of keycloak (#59)\n\n* chore(example): add jwt decode command as example\r\n\r\n* chore(docker-compose): remove version\r\n\r\n* fix(curl): test redirect with delete comment due the httpbin error.\r\n\r\n* fix(csv): remove terminator function, the serializer already add the terminator.\r\n\r\n* fix(keycloak): adapt test with new version of keycloak",
          "timestamp": "2024-05-19T22:16:51+02:00",
          "tree_id": "74ac3368359ee204776ad9ab47133ffa363dae92",
          "url": "https://github.com/jmfiaschi/chewdata/commit/3c2fd034b0cf633650d88350e8bb10c8c1d03cd2"
        },
        "date": 1716150790298,
        "tool": "cargo",
        "benches": [
          {
            "name": "read_json/",
            "value": 4566,
            "range": "± 24",
            "unit": "ns/iter"
          },
          {
            "name": "read_jsonl/",
            "value": 4469,
            "range": "± 63",
            "unit": "ns/iter"
          },
          {
            "name": "read_xml/",
            "value": 298261,
            "range": "± 3590",
            "unit": "ns/iter"
          },
          {
            "name": "read_csv/",
            "value": 19983,
            "range": "± 114",
            "unit": "ns/iter"
          },
          {
            "name": "read_toml/",
            "value": 10872,
            "range": "± 135",
            "unit": "ns/iter"
          },
          {
            "name": "read_yaml/",
            "value": 13988,
            "range": "± 59",
            "unit": "ns/iter"
          },
          {
            "name": "read_parquet/",
            "value": 126591,
            "range": "± 1007",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}