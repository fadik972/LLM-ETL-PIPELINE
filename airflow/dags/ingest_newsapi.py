from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import requests
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "pipeline",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

TOPICS = [
    "artificial intelligence",
    "machine learning",
    "data engineering",
]

@dag(
    dag_id="ingest_newsapi",
    description="Fetch news articles from NewsAPI",
    schedule="0 */6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ingestion", "newsapi"],
    max_active_runs=1,
)
def ingest_newsapi():

    @task()
    def fetch_articles() -> list[dict]:
        api_key = os.environ["NEWSAPI_KEY"]
        articles = []

        for topic in TOPICS:
            log.info("Fetching articles for topic: %s", topic)
            resp = requests.get(
                "https://newsapi.org/v2/everything",
                params={
                    "q": topic,
                    "language": "en",
                    "sortBy": "publishedAt",
                    "pageSize": 10,
                    "apiKey": api_key,
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()

            for article in data.get("articles", []):
                if not article.get("url") or not article.get("title"):
                    continue
                articles.append({
                    "source": "newsapi",
                    "external_id": article["url"],
                    "title": article["title"],
                    "content": article.get("content") or article.get("description") or "",
                    "url": article["url"],
                    "published_at": article.get("publishedAt"),
                })

        log.info("Fetched %d articles total", len(articles))
        return articles

    @task()
    def save_articles(articles: list[dict]) -> None:
        if not articles:
            log.info("No articles to save")
            return

        import psycopg2
        from psycopg2.extras import execute_values

        conn = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=os.environ.get("POSTGRES_PORT", 5432),
            dbname=os.environ.get("PIPELINE_DB", "pipeline"),
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )

        try:
            rows = [
                (
                    a["source"],
                    a["external_id"],
                    a["title"],
                    a.get("content", ""),
                    a.get("url"),
                    a.get("published_at"),
                )
                for a in articles
            ]

            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO raw_articles
                        (source, external_id, title, content, url, published_at)
                    VALUES %s
                    ON CONFLICT (external_id) DO NOTHING
                    """,
                    rows,
                )
            conn.commit()
            log.info("Saved %d articles to database", len(rows))

        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    articles = fetch_articles()
    save_articles(articles)

ingest_newsapi()
