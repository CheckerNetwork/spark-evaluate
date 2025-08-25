CREATE TABLE daily_miner_deals_checked (
  day DATE NOT NULL,
  miner_id TEXT NOT NULL,
  payload_cids TEXT[] NOT NULL,
  PRIMARY KEY (day, miner_id)
);