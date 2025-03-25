CREATE TABLE daily_allocator_retrieval_stats (
  day DATE NOT NULL,
  allocator_id TEXT NOT NULL,
  total INTEGER NOT NULL,
  successful INTEGER NOT NULL, 
  successful_http INTEGER NOT NULL,
  PRIMARY KEY (day, allocator_id)
);