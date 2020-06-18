# Mono QA

This repository contains the code to run the mono-qa server.

## Prerequisites

### Python

I need python >= 3.6 for everything to work here, as well as pip and venv.

We also require an instance of ElasticSearch to be running.
It assumes that it will be running on http://localhost:9200.
You can download ElasticSearch [here](https://www.elastic.co/downloads/elasticsearch)

Currently I'm running on ElasticSearch 7.7

### pyTorch

*pyTorch* is used here.  On linux, this can be installed with `pip` no
problem.  On windows, it's a bit more complicated but the fellas at pytorch
will provide you an installation command
[here](https://pytorch.org/get-started/locally/).

**BE SURE TO ACTIVATE YOUR VIRTUAL ENVIRONMENT FIRST!**

At this very moment, the installation command for windows is:

    pip install torch===1.5.0 torchvision===0.6.0 -f https://download.pytorch.org/whl/torch_stable.html

## Usage

You can go to the src directory and run the config script:

    # Linux/bash

    cd src
    ./config.sh

    # Windows/powershell

    sl src
    powershell config.ps1


This will set up a virtual environment and install required dependencies.
To use the program you will need to `activate` your virtual environment:

    # Linux/bash

    source bin/activate

    # Windows/powershell

    powershell Scripts/Activate.ps1

Now you can run the server:
    
    python server.py

## API

As of now, the server will accept `POST` requests at

    http://192.168.0.72:8000/question

The request should look like:

    POST /question HTTP/1.1
    host: 192.168.0.72:8000
    content-type: application/json

    {
        "question": "where is your team based?"
    }

Or, in `cURL`:

    curl http://192.168.0.72:8000/question -H 'content-type: application/json' -d '
    {
        "question": "where is your team based?"
    }
    '

You will then receive a `json` response that looks something like:

    {
        "quick_answer": "We are located in southern Europe.",
        "question": { 
            "text": "where is your team based?",
            "uuid": "24f8619a-df94-4104-869f-22ecf0221ede"
        },
        "answers": [
            { "answer": "We are located in southern Europe.",
              "rating": .95,
              "paragraph": "Mono is a company...",
              "paragraph_rank": 1,
              "docId": "wltzwnIBO-Ft5Ide2Lj7"
            },
            ...
        ]
    }

The `quick_answer` is provided for convenience and will correspond to the
highest rated answer in the `answers` array.  It is possible that no answer is
found, in which case you will get a `json` that looks something like:

    {
        "quick_answer": "",
        "question": { 
            "text": "where is your team based?",
            "uuid": "24f8619a-df94-4104-869f-22ecf0221ede"
        },
        "answers": [
            { "answer": "",
              "rating": 0.8,
              "paragraph": "Mono is a company...",
              "paragraph_rank": 1,
              "docId": "wltzwnIBO-Ft5Ide2Lj7"
            },
            ...
        ]
    }

A few important things to note:

* `quick answer` is the empty string\
* the `answers` array is not empty\
* `answer` in the `answers` array has a non-zero rating\

The QA model may decide that a paragraph does not contain the answer to a
question.  This means it will give an empty span the highest rating of all
spans.  For feedback mechanisms we may wish a user to look at the paragraphs
and decide if in fact the answer is there, or if the wrong answer is chosen
(i.e. the `quick_answer` is wrong), but the "correct" answer corresponds to a
different paragraph, this information may be valuable.

In some (probably rare cases) it may happen that ElasticSearch finds no
paragraphs related to some question.  In this case, the `answers` array will
be empty.  Be sure to be prepared to handle this case.

    {
        "quick_answer": "",
        "question": { 
            "text": "gdje se nalazi tvoja ekipa?",
            "uuid": "24f8619a-df94-4104-869f-22ecf0221ede"
        },
        "answers": []
    }

## Contact

The original author of this code can be reached at:

    nchappell@mono-software.com
