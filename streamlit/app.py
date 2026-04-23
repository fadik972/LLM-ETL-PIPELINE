import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import json
import urllib.request
import os

st.set_page_config(page_title="LLM ETL Pipeline", page_icon="🔬", layout="wide")

def get_conn():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", 5432),
        dbname=os.environ.get("PIPELINE_DB", "pipeline"),
        user=os.environ.get("POSTGRES_USER", "airflow"),
        password=os.environ.get("POSTGRES_PASSWORD", "airflow"),
    )

def run_query(sql):
    conn = get_conn()
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()

OLLAMA_URL = os.environ.get("OLLAMA_BASE_URL", "http://172.18.0.1:11434")
LLM_MODEL = os.environ.get("LLM_MODEL", "llama3.2:3b")
EMB_MODEL = os.environ.get("EMBEDDING_MODEL", "nomic-embed-text")

def get_embedding(text):
    payload = json.dumps({"model": EMB_MODEL, "prompt": text}).encode()
    req = urllib.request.Request(
        f"{OLLAMA_URL}/api/embeddings",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())["embedding"]

def search_chunks(query, top_k=3):
    emb = get_embedding(query)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT dc.chunk_text, a.title, a.url,
                       1 - (dc.embedding <=> %s::vector) as similarity
                FROM document_chunks dc
                JOIN raw_articles a ON dc.article_id = a.id
                ORDER BY dc.embedding <=> %s::vector
                LIMIT %s
            """, (emb, emb, top_k))
            return [
                {"chunk": r[0], "title": r[1], "url": r[2], "similarity": round(float(r[3]), 3)}
                for r in cur.fetchall()
            ]
    finally:
        conn.close()

def ask_llm(question, context):
    prompt = f"""You are a news analyst. Answer based ONLY on the context below.
If the context lacks information, say so.

Context:
{context}

Question: {question}
Answer:"""
    payload = json.dumps({"model": LLM_MODEL, "prompt": prompt, "stream": False}).encode()
    req = urllib.request.Request(
        f"{OLLAMA_URL}/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read()).get("response", "No response")

st.sidebar.title("LLM ETL Pipeline")
page = st.sidebar.radio("Navigate", ["Dashboard", "Articles", "RAG Chatbot"])

if page == "Dashboard":
    st.title("Pipeline Dashboard")

    col1, col2, col3, col4 = st.columns(4)
    total = run_query("SELECT COUNT(*) as n FROM raw_articles")
    processed = run_query("SELECT COUNT(*) as n FROM raw_articles WHERE is_processed=TRUE")
    entities = run_query("SELECT COUNT(*) as n FROM extracted_entities")
    chunks = run_query("SELECT COUNT(*) as n FROM document_chunks")

    col1.metric("Total articles", int(total["n"][0]))
    col2.metric("Processed", int(processed["n"][0]))
    col3.metric("Entities extracted", int(entities["n"][0]))
    col4.metric("Vector chunks", int(chunks["n"][0]))

    st.divider()

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Top mentioned entities")
        entity_type = st.selectbox("Filter by type", ["All", "company", "person", "topic"])
        query = """
            SELECT entity_type, entity_value, mention_count
            FROM marts.top_entities
            WHERE LOWER(entity_value) != 'none mentioned'
            AND LOWER(entity_value) != 'none'
        """
        if entity_type != "All":
            query += f" AND entity_type = '{entity_type}'"
        query += " ORDER BY mention_count DESC LIMIT 15"
        df_entities = run_query(query)
        if not df_entities.empty:
            fig = px.bar(df_entities, x="mention_count", y="entity_value",
                        color="entity_type", orientation="h", height=450)
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Sentiment breakdown")
        df_sent = run_query("""
            SELECT sentiment, SUM(article_count) as total
            FROM marts.sentiment_summary
            WHERE sentiment IS NOT NULL
            GROUP BY sentiment
        """)
        if not df_sent.empty:
            fig2 = px.pie(df_sent, names="sentiment", values="total",
                         color="sentiment",
                         color_discrete_map={"positive": "#2ecc71", "negative": "#e74c3c", "neutral": "#95a5a6"},
                         height=450)
            st.plotly_chart(fig2, use_container_width=True)

elif page == "Articles":
    st.title("Articles")
    sentiment_filter = st.selectbox("Filter by sentiment", ["All", "positive", "negative", "neutral"])
    query = """
        SELECT title, source, sentiment, score, published_at, url
        FROM marts.article_overview
        WHERE title IS NOT NULL
    """
    if sentiment_filter != "All":
        query += f" AND sentiment = '{sentiment_filter}'"
    query += " ORDER BY published_at DESC LIMIT 50"
    df = run_query(query)
    if not df.empty:
        for _, row in df.iterrows():
            sentiment_color = {"positive": "🟢", "negative": "🔴", "neutral": "⚪"}.get(row.get("sentiment", ""), "⚪")
            title = str(row["title"])
            display_title = f"{sentiment_color} {title[:80]}..." if len(title) > 80 else f"{sentiment_color} {title}"
            with st.expander(display_title):
                col1, col2, col3 = st.columns(3)
                col1.write(f"**Source:** {row['source']}")
                col2.write(f"**Sentiment:** {row.get('sentiment', 'N/A')}")
                col3.write(f"**Score:** {row.get('score', 'N/A')}")
                if row.get("url"):
                    st.markdown(f"[Read full article]({row['url']})")

elif page == "RAG Chatbot":
    st.title("News RAG Chatbot")
    st.caption("Ask questions about the news articles in the database")

    if "messages" not in st.session_state:
        st.session_state.messages = []

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])

    question = st.chat_input("Ask about the news...")

    if question:
        st.session_state.messages.append({"role": "user", "content": question})
        with st.chat_message("user"):
            st.write(question)
        with st.chat_message("assistant"):
            with st.spinner("Searching articles and generating answer..."):
                try:
                    chunks = search_chunks(question, top_k=3)
                    context = "\n\n".join([f"Article: {c['title']}\n{c['chunk']}" for c in chunks])
                    answer = ask_llm(question, context)
                    st.write(answer)
                    with st.expander("Sources"):
                        for c in chunks:
                            st.write(f"**{c['title']}** (similarity: {c['similarity']})")
                            if c["url"]:
                                st.markdown(f"[Link]({c['url']})")
                    st.session_state.messages.append({"role": "assistant", "content": answer})
                except Exception as e:
                    st.error(f"Error: {e}")
