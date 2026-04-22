from __future__ import annotations

import json
import logging
import os
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

EXTRACTION_PROMPT = """You are a data extraction assistant. Analyze the following news article and extract structured information.

Article Title: {title}
Article Content: {content}

Extract and return ONLY a valid JSON object with this exact structure:
{{
    "entities": [
        {{"type": "company", "value": "company name"}},
        {{"type": "topic", "value": "topic name"}},
        {{"type": "person", "value": "person name"}}
    ],
    "sentiment": "positive" or "negative" or "neutral",
    "sentiment_score": a float between -1.0 and 1.0,
    "summary": "one sentence summary of the article"
}}

Rules:
- Extract up to 5 entities total
- Only include clearly mentioned companies, topics, and people
- sentiment_score: 1.0 = very positive, -1.0 = very negative, 0.0 = neutral
- Return ONLY the JSON object, no other text
"""


def get_db_conn():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", 5432),
        dbname=os.environ.get("PIPELINE_DB", "pipeline"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def call_ollama(title: str, content: str) -> dict:
    """Send article to Ollama and parse the JSON response."""
    import urllib.request
    import urllib.error

    ollama_url = os.environ.get("OLLAMA_BASE_URL", "http://host-gateway:11434")
    prompt = EXTRACTION_PROMPT.format(
        title=title,
        content=content[:1000],  # limit to first 1000 chars to stay within context
    )

    payload = json.dumps({
        "model": os.environ.get("LLM_MODEL", "llama3.2:3b"),
        "prompt": prompt,
        "stream": False,
    }).encode("utf-8")

    req = urllib.request.Request(
        f"{ollama_url}/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=60) as resp:
        result = json.loads(resp.read().decode("utf-8"))
        raw_text = result.get("response", "")

    # Parse the JSON from Ollama's response
    start = raw_text.find("{")
    end = raw_text.rfind("}") + 1
    if start == -1 or end == 0:
        raise ValueError(f"No JSON found in response: {raw_text}")

    return json.loads(raw_text[start:end])


@dag(
    dag_id="llm_extraction",
    description="Extract entities and sentiment from articles using Ollama",
    schedule="30 */6 * * *",  # 30 mins after ingestion
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["llm", "extraction"],
    max_active_runs=1,
)
def llm_extraction():

    @task()
    def get_unprocessed_articles() -> list[dict]:
        """Fetch all articles not yet processed by the LLM."""
        conn = get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, title, content
                    FROM raw_articles
                    WHERE is_processed = FALSE
                    ORDER BY published_at DESC
                    LIMIT 50
                """)
                rows = cur.fetchall()
                articles = [
                    {"id": row[0], "title": row[1], "content": row[2] or ""}
                    for row in rows
                ]
            log.info("Found %d unprocessed articles", len(articles))
            return articles
        finally:
            conn.close()

    @task()
    def extract_and_save(articles: list[dict]) -> None:
        """Send each article to Ollama and save results to DB."""
        if not articles:
            log.info("No articles to process")
            return

        conn = get_db_conn()
        processed = 0
        failed = 0

        try:
            for article in articles:
                article_id = article["id"]
                title = article["title"]
                content = article["content"]

                log.info("Processing article %d: %s", article_id, title[:50])

                try:
                    extracted = call_ollama(title, content)

                    with conn.cursor() as cur:
                        # Save entities
                        entities = extracted.get("entities", [])
                        if entities:
                            entity_rows = [
                                (article_id, e["type"], e["value"])
                                for e in entities
                                if "type" in e and "value" in e
                            ]
                            if entity_rows:
                                execute_values(cur, """
                                    INSERT INTO extracted_entities
                                        (article_id, entity_type, entity_value)
                                    VALUES %s
                                    ON CONFLICT DO NOTHING
                                """, entity_rows)

                        # Save sentiment
                        sentiment = extracted.get("sentiment", "neutral")
                        score = float(extracted.get("sentiment_score", 0.0))
                        summary = extracted.get("summary", "")

                        cur.execute("""
                            INSERT INTO article_sentiment
                                (article_id, sentiment, score, summary)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (article_id) DO UPDATE
                                SET sentiment = EXCLUDED.sentiment,
                                    score = EXCLUDED.score,
                                    summary = EXCLUDED.summary
                        """, (article_id, sentiment, score, summary))

                        # Mark article as processed
                        cur.execute("""
                            UPDATE raw_articles
                            SET is_processed = TRUE
                            WHERE id = %s
                        """, (article_id,))

                    conn.commit()
                    processed += 1
                    log.info("Article %d processed successfully", article_id)

                except Exception as e:
                    conn.rollback()
                    failed += 1
                    log.error("Failed to process article %d: %s", article_id, str(e))
                    continue

        finally:
            conn.close()

        log.info("Extraction complete. Processed: %d, Failed: %d", processed, failed)

    articles = get_unprocessed_articles()
    extract_and_save(articles)


llm_extraction()
