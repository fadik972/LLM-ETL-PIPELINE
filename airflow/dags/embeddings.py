from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import execute_values
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

def get_db_conn():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", 5432),
        dbname=os.environ.get("PIPELINE_DB", "pipeline"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def chunk_text(text: str, chunk_size: int = 500, overlap: int = 50) -> list[str]:
    """Split text into overlapping chunks."""
    if not text or len(text.strip()) == 0:
        return []
    words = text.split()
    if len(words) == 0:
        return []
    chunks = []
    start = 0
    while start < len(words):
        end = min(start + chunk_size, len(words))
        chunk = " ".join(words[start:end])
        chunks.append(chunk)
        if end == len(words):
            break
        start += chunk_size - overlap
    return chunks


def get_embedding(text: str, ollama_url: str, model: str) -> list[float]:
    """Get embedding vector from Ollama."""
    payload = json.dumps({
        "model": model,
        "prompt": text,
    }).encode("utf-8")

    req = urllib.request.Request(
        f"{ollama_url}/api/embeddings",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=60) as resp:
        result = json.loads(resp.read().decode("utf-8"))
        return result["embedding"]


@dag(
    dag_id="embeddings",
    description="Chunk articles and store vector embeddings in pgvector",
    schedule="0 */6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["embeddings", "vectors"],
    max_active_runs=1,
)
def embeddings():

    @task()
    def get_articles_to_embed() -> list[dict]:
        """Get processed articles that don't have embeddings yet."""
        conn = get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT a.id, a.title, a.content
                    FROM raw_articles a
                    WHERE a.is_processed = TRUE
                    AND a.id NOT IN (
                        SELECT DISTINCT article_id
                        FROM document_chunks
                    )
                    ORDER BY a.published_at DESC
                    LIMIT 20
                """)
                rows = cur.fetchall()
                articles = [
                    {"id": row[0], "title": row[1], "content": row[2] or ""}
                    for row in rows
                ]
            log.info("Found %d articles to embed", len(articles))
            return articles
        finally:
            conn.close()

    @task()
    def create_embeddings(articles: list[dict]) -> None:
        """Chunk each article and create embeddings using Ollama."""
        if not articles:
            log.info("No articles to embed")
            return

        ollama_url = os.environ.get("OLLAMA_BASE_URL", "http://172.18.0.1:11434")
        model = os.environ.get("EMBEDDING_MODEL", "nomic-embed-text")

        conn = get_db_conn()
        processed = 0
        failed = 0

        try:
            for article in articles:
                article_id = article["id"]
                title = article["title"]
                content = article["content"]

                # Combine title and content for richer embeddings
                full_text = f"{title}\n\n{content}"
                chunks = chunk_text(full_text, chunk_size=200, overlap=20)

                if not chunks:
                    log.warning("No chunks for article %d", article_id)
                    continue

                log.info(
                    "Embedding article %d: %d chunks",
                    article_id, len(chunks)
                )

                try:
                    rows = []
                    for i, chunk in enumerate(chunks):
                        embedding = get_embedding(chunk, ollama_url, model)
                        rows.append((
                            article_id,
                            i,
                            chunk,
                            embedding,
                            len(chunk.split()),
                        ))

                    with conn.cursor() as cur:
                        execute_values(
                            cur,
                            """
                            INSERT INTO document_chunks
                                (article_id, chunk_index, chunk_text,
                                 embedding, token_count)
                            VALUES %s
                            ON CONFLICT DO NOTHING
                            """,
                            rows,
                            template="(%s, %s, %s, %s::vector, %s)",
                        )
                    conn.commit()
                    processed += 1
                    log.info("Article %d embedded successfully", article_id)

                except Exception as e:
                    conn.rollback()
                    failed += 1
                    log.error(
                        "Failed to embed article %d: %s",
                        article_id, str(e)
                    )
                    continue

        finally:
            conn.close()

        log.info(
            "Embedding complete. Processed: %d, Failed: %d",
            processed, failed
        )

    articles = get_articles_to_embed()
    create_embeddings(articles)


embeddings()
