+++
title = "Configuration"
description = "Everything arround the configuration"
date = 2021-05-01T18:20:00+00:00
updated = 2021-05-01T18:20:00+00:00
draft = false
weight = 100
sort_by = "weight"
template = "docs/page.html"

[extra]
lead = "See how to customize your application"
toc = true
top = false
+++

## The structure

The configuration is composed by a list of steps :
* List of steps
  * Step 1
  * Step 2
  * ...
  * Step N

Example in json:
```json
[
    { 
        "type": "reader"
    },
    {
        "type": "writer"
    },
    {
        "type": "transformer"
    },
    ...
]
```

## Order

The application read steps in `FIFO` (First In First Out) mode.

## Behaviour

All steps are link together by internal data queue. When a step finish to handle a data, it is pushed in the queue and the next step will handle the data.
Each steps run in asynchronous.
Each queue contain a limit that can be customize. 
If a queue reach the limit, the step wait that the queue is released by the next step. 
