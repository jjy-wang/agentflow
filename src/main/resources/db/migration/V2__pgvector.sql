CREATE EXTENSION IF NOT EXISTS vector;

CREATE INDEX idx_memory_embedding ON t_memory
    USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);

CREATE INDEX idx_memory_tenant ON t_memory(tenant_id, created_at DESC);
