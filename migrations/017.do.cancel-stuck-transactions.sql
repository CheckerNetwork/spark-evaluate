CREATE TABLE transactions_pending (
  hash TEXT NOT NULL PRIMARY KEY,
  timestamp TEXT NOT NULL,
  from TEXT NOT NULL,
  max_priority_fee_per_gas BIGINT NOT NULL,
  nonce NUMBER NOT NULL
);
