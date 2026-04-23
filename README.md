# LLM-Powered ETL Pipeline with RAG

A production-grade data engineering pipeline that automatically ingests news articles, extracts structured insights using a local LLM, stores vector embeddings for semantic search, and exposes everything through an interactive dashboard with a RAG chatbot.

> 100% free and local — no API costs, no cloud dependencies.

---

## Architecture

```
NewsAPI
      ↓
Apache Airflow (orchestration)
      ↓
PostgreSQL + pgvector (storage + vector search)
      ↓
Ollama llama3.2:3b (entity extraction + sentiment)
      ↓
Ollama nomic-embed-text (vector embeddings)
      ↓
dbt (data transformation)
      ↓
LangChain RAG + Streamlit (chatbot + dashboard)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9 |
| Database | PostgreSQL 16 + pgvector |
| LLM | Ollama + Llama 3.2 3B |
| Embeddings | Ollama + nomic-embed-text |
| Transformation | dbt |
| RAG | LangChain |
| Dashboard | Streamlit |
| Containerization | Docker Compose |
| Language | Python 3.11 |

---

## Features

- Automated news ingestion from NewsAPI every 6 hours via Airflow DAGs
- Local LLM extracts companies, people, topics and sentiment from each article — no OpenAI costs
- Text chunked and converted to 768-dimensional vectors stored in pgvector
- HNSW index enables fast approximate nearest-neighbor search
- dbt transforms raw data into clean analytical marts
- LangChain RAG chatbot answers natural language questions over the full article corpus
- Streamlit dashboard with entity frequency charts, sentiment breakdown, and article browser

---

## Project Structure

```
llm-etl-pipeline/
├── airflow/
│   └── dags/
│       ├── ingest_newsapi.py      # Fetches news articles every 6 hours
│       ├── llm_extraction.py      # Extracts entities + sentiment with Ollama
│       └── embeddings.py          # Creates vector embeddings with nomic-embed-text
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       └── marts/
│           ├── top_entities.sql       # Most mentioned companies/people/topics
│           ├── sentiment_summary.sql  # Sentiment aggregated by source and day
│           ├── article_overview.sql   # Full article view with all enrichments
│           └── sources.yml
├── src/
│   └── rag_chatbot.py             # RAG pipeline (vector search + LLM generation)
├── streamlit/
│   ├── app.py                     # Dashboard + chatbot UI
│   ├── Dockerfile
│   └── requirements.txt
├── scripts/
│   └── init_db.sql                # PostgreSQL schema + pgvector setup
├── docker-compose.yml             # All services defined here
├── .env.example                   # Environment variables template
└── README.md
```

---

## Prerequisites

- Docker Engine + Docker Compose v2
- Ollama installed locally
- NewsAPI key (free tier at newsapi.org)
- 8GB+ RAM recommended

---

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/fadik972/LLM-ETL-PIPELINE.git
cd LLM-ETL-PIPELINE
```

### 2. Install and configure Ollama

```bash
# Install Ollama
curl -fsSL https://ollama.com/install.sh | sh

# Pull the required models
ollama pull llama3.2:3b
ollama pull nomic-embed-text

# Allow Docker containers to reach Ollama
sudo mkdir -p /etc/systemd/system/ollama.service.d
sudo bash -c 'cat > /etc/systemd/system/ollama.service.d/override.conf << EOF
[Service]
Environment="OLLAMA_HOST=0.0.0.0"
EOF'
sudo systemctl daemon-reload
sudo systemctl restart ollama
```

### 3. Configure environment variables

```bash
cp .env.example .env
nano .env
```

Fill in your NewsAPI key:

```
NEWSAPI_KEY=your_key_here
```

### 4. Set Airflow UID

```bash
echo "AIRFLOW_UID=50000" >> .env
```

### 5. Start the stack

```bash
docker compose up -d
```

Wait about 60 seconds for all services to initialize.

### 6. Find your Docker bridge IP and update .env

```bash
docker network inspect pipeline_network | grep Gateway
```

Update `OLLAMA_BASE_URL` in `.env` with the gateway IP:

```
OLLAMA_BASE_URL=http://<gateway-ip>:11434
```

Then restart:

```bash
docker compose down
docker compose up -d
```

### 7. Access the services

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Streamlit dashboard | http://localhost:8501 | — |
| PostgreSQL | localhost:5432 | airflow / airflow |

### 8. Run the pipeline

In the Airflow UI, trigger the DAGs in this order:

1. `ingest_newsapi` — fetches articles from NewsAPI
2. `llm_extraction` — extracts entities and sentiment using Ollama
3. `embeddings` — creates vector embeddings for RAG

---

## Database Schema

```sql
-- Raw ingestion layer
raw_articles        -- source, title, content, url, published_at, is_processed
extracted_entities  -- article_id, entity_type, entity_value
article_sentiment   -- article_id, sentiment, score, summary
document_chunks     -- article_id, chunk_text, embedding (vector 768)

-- dbt mart layer
marts.top_entities      -- cleaned entity frequency table
marts.sentiment_summary -- sentiment aggregated by source and day
marts.article_overview  -- full joined view of articles + sentiment + entity count
```

---

## Pipeline DAGs

| DAG | Schedule | What it does |
|---|---|---|
| `ingest_newsapi` | Every 6 hours | Fetches articles from NewsAPI, deduplicates by URL, saves to raw_articles |
| `llm_extraction` | Every 6h + 30min | Reads unprocessed articles, sends to Ollama, saves entities and sentiment |
| `embeddings` | Every 6 hours | Chunks processed articles, generates vectors with nomic-embed-text, stores in pgvector |

---

## How the RAG Chatbot Works

```
User asks: "What is happening with AI investments?"
                    ↓
Convert question to 768-dimensional vector
                    ↓
pgvector cosine similarity search over document_chunks
                    ↓
Retrieve top 3 most semantically similar chunks
                    ↓
Send chunks + question to Ollama (llama3.2:3b)
                    ↓
Answer grounded in actual article content with sources
```

---

## Stopping the Stack

```bash
# Stop all containers (data is preserved)
docker compose down

# Restart
docker compose up -d
```

---

## Key Design Decisions

**Why Ollama instead of OpenAI?**
Running LLMs locally means zero API costs, no data leaving your machine, and no rate limits. llama3.2:3b is capable enough for entity extraction and RAG on a standard laptop.

**Why pgvector instead of a dedicated vector database?**
pgvector keeps the entire stack in a single PostgreSQL instance. No extra service to manage, full SQL joins between vectors and metadata, and HNSW indexing provides production-grade search performance.

**Why LocalExecutor in Airflow?**
For a single-machine portfolio project, LocalExecutor is simpler and more resource-efficient than CeleryExecutor. It runs tasks as subprocesses without needing Redis or a message broker.

**Why dbt for transformation?**
dbt enforces a clear separation between raw ingestion and analytical models. It makes transformations testable, documented, and reproducible — exactly how production data teams work.

---

## Author

**Fadi Kaabi**
MSc Software Engineering — Data Engineering & ML

- GitHub: [fadik972](https://github.com/fadik972)
- LinkedIn: [Fadi Kaabi](https://linkedin.com/in/fadi-kaabi)
- Email: fadikaabi12@gmail.com
