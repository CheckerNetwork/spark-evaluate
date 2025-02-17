CREATE TABLE daily_client_retrieval_stats (
  client_id TEXT NOT NULL,
  day DATE NOT NULL,
  total INTEGER NOT NULL,
  successful INTEGER NOT NULL, 
  successful_http INTEGER NOT NULL,
  PRIMARY KEY (day, client_id)
);