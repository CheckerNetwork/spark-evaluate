CREATE TABLE daily_client_retrieval_stats (
  day DATE NOT NULL,
  client_id TEXT NOT NULL,
  total INTEGER NOT NULL,
  successful INTEGER NOT NULL, 
  successful_http INTEGER NOT NULL,
  PRIMARY KEY (day, client_id)
);