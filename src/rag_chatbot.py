"""
RAG Chatbot using LangChain + Ollama + pgvector
Answers questions about news articles using semantic search.
"""
from __future__ import annotations

import os
import psycopg2
import json
import urllib.request
from typing import List


def get_db_conn():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", 5432),
        dbname=os.environ.get("PIPELINE_DB", "pipeline"),
        user=os.environ.get("POSTGRES_USER", "airflow"),
        password=os.environ.get("POSTGRES_PASSWORD", "airflow"),
    )


def get_embedding(text: str) -> List[float]:
    """Get embedding from Ollama."""
    ollama_url = os.environ.get("OLLAMA_BASE_URL", "http://172.18.0.1:11434")
    payload = json.dumps({
        "model": "nomic-embed-text",
        "prompt": text,
    }).encode("utf-8")

    req = urllib.request.Request(
        f"{ollama_url}/api/embeddings",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=30) as resp:
        result = json.loads(resp.read().decode("utf-8"))
        return result["embedding"]


def search_similar_chunks(query: str, top_k: int = 3) -> List[dict]:
    """Search for similar chunks using pgvector cosine similarity."""
    query_embedding = get_embedding(query)

    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    dc.chunk_text,
                    a.title,
                    a.url,
                    a.published_at,
                    1 - (dc.embedding <=> %s::vector) as similarity
                FROM document_chunks dc
                JOIN raw_articles a ON dc.article_id = a.id
                ORDER BY dc.embedding <=> %s::vector
                LIMIT %s
            """, (query_embedding, query_embedding, top_k))

            rows = cur.fetchall()
            return [
                {
                    "chunk_text": row[0],
                    "title": row[1],
                    "url": row[2],
                    "published_at": row[3],
                    "similarity": float(row[4]),
                }
                for row in rows
            ]
    finally:
        conn.close()


def ask_ollama(question: str, context: str) -> str:
    """Send question + context to Ollama and get an answer."""
    ollama_url = os.environ.get("OLLAMA_BASE_URL", "http://172.18.0.1:11434")

    prompt = f"""You are a helpful news analyst assistant. 
Answer the question based ONLY on the provided news article context below.
If the context doesn't contain enough information, say so clearly.

Context from news articles:
{context}

Question: {question}

Answer:"""

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

    with urllib.request.urlopen(req, timeout=120) as resp:
        result = json.loads(resp.read().decode("utf-8"))
        return result.get("response", "No response generated")


def answer_question(question: str) -> dict:
    """
    Full RAG pipeline:
    1. Convert question to vector
    2. Find similar chunks in pgvector
    3. Send context + question to Ollama
    4. Return answer with sources
    """
    print(f"\nSearching for relevant articles...")
    chunks = search_similar_chunks(question, top_k=3)

    if not chunks:
        return {
            "answer": "No relevant articles found in the database.",
            "sources": [],
        }

    # Build context from chunks
    context = "\n\n".join([
        f"Article: {c['title']}\n{c['chunk_text']}"
        for c in chunks
    ])

    print(f"Found {len(chunks)} relevant chunks. Generating answer...")
    answer = ask_ollama(question, context)

    return {
        "answer": answer,
        "sources": [
            {
                "title": c["title"],
                "url": c["url"],
                "similarity": round(c["similarity"], 3),
            }
            for c in chunks
        ],
    }


if __name__ == "__main__":
    # Test the chatbot
    questions = [
        "What did Apple announce recently?",
        "What are the latest developments in artificial intelligence?",
        "What is happening with interest rates?",
    ]

    for question in questions:
        print(f"\n{'='*60}")
        print(f"Question: {question}")
        print('='*60)

        result = answer_question(question)
        print(f"\nAnswer: {result['answer']}")
        print(f"\nSources:")
        for s in result["sources"]:
            print(f"  - {s['title']} (similarity: {s['similarity']})")
