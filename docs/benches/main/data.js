window.BENCHMARK_DATA = {
  "lastUpdate": 1691565932263,
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
      }
    ]
  }
}