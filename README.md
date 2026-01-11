# ScyllaDB Vector Search Chatbot Demo

A minimal [FastAPI](https://fastapi.tiangolo.com/) and [HTMX](https://htmx.org/) app that uses [ScyllaDB Vector Search](https://www.scylladb.com/) and LLMs to turn movie plots into database stories.


## Prerequisites
* [uv](https://github.com/astral-sh/uv) package manager
* [Python 3.14+](https://www.python.org/downloads/)

## Get started
```bash
git clone <this-repo-url>
cd chatbot-demo
```

## Install
```bash
uv sync
```

## Environment variables
Copy and edit `example.env`. Set your ScyllaDB and GROQ API credentials.

## Run the server
```bash
uv run uvicorn chatbot.app:app --reload
```
The app will be available at [http://localhost:8000](http://localhost:8000)
