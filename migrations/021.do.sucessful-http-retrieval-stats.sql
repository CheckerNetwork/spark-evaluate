ALTER TABLE retrieval_stats
ADD COLUMN IF NOT EXISTS successful_http INT NOT NULL DEFAULT 0