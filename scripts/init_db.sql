-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Create our pipeline database
CREATE DATABASE pipeline;

-- Connect to it
\connect pipeline

-- Enable vector in pipeline db too
CREATE EXTENSION IF NOT EXISTS vector;

-- Raw articles from NewsAPI and arXiv
CREATE TABLE IF NOT EXISTS raw_articles (
    id              SERIAL PRIMARY KEY,
    source          VARCHAR(50)  NOT NULL,
    external_id     VARCHAR(255) UNIQUE NOT NULL,
    title           TEXT         NOT NULL,
    content         TEXT,
    url             TEXT,
    published_at    TIMESTAMPTZ,
    fetched_at      TIMESTAMPTZ  DEFAULT NOW(),
    is_processed    BOOLEAN      DEFAULT FALSE
);

-- Entities extracted by the LLM
CREATE TABLE IF NOT EXISTS extracted_entities (
    id              SERIAL PRIMARY KEY,
    article_id      INT          REFERENCES raw_articles(id) ON DELETE CASCADE,
    entity_type     VARCHAR(50)  NOT NULL,
    entity_value    TEXT         NOT NULL,
    confidence      FLOAT,
    extracted_at    TIMESTAMPTZ  DEFAULT NOW()
);

-- Sentiment extracted by the LLM
CREATE TABLE IF NOT EXISTS article_sentiment (
    id              SERIAL PRIMARY KEY,
    article_id      INT          REFERENCES raw_articles(id) ON DELETE CASCADE UNIQUE,
    sentiment       VARCHAR(20)  NOT NULL,
    score           FLOAT        NOT NULL,
    summary         TEXT,
    extracted_at    TIMESTAMPTZ  DEFAULT NOW()
);

-- Text chunks with vector embeddings
CREATE TABLE IF NOT EXISTS document_chunks (
    id              SERIAL PRIMARY KEY,
    article_id      INT          REFERENCES raw_articles(id) ON DELETE CASCADE,
    chunk_index     INT          NOT NULL,
    chunk_text      TEXT         NOT NULL,
    embedding       VECTOR(768),
    token_count     INT,
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);

-- Fast similarity search index
CREATE INDEX IF NOT EXISTS chunk_embedding_hnsw
    ON document_chunks
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- Useful indexes
CREATE INDEX IF NOT EXISTS idx_articles_source    ON raw_articles (source);
CREATE INDEX IF NOT EXISTS idx_articles_processed ON raw_articles (is_processed);
CREATE INDEX IF NOT EXISTS idx_articles_published ON raw_articles (published_at DESC);
CREATE INDEX IF NOT EXISTS idx_entities_type      ON extracted_entities (entity_type);

-- Schema for dbt transformed models
CREATE SCHEMA IF NOT EXISTS marts;
