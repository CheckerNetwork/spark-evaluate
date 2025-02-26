CREATE TABLE unpublished_provider_retrieval_result_stats_rounds (
  round_index NUMERIC NOT NULL,
  contract_address TEXT NOT NULL,
  evaluated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  spark_evaluate_version TEXT NOT NULL,
  measurement_batches TEXT[] NOT NULL,
  round_details TEXT NOT NULL,
  provider_retrieval_result_stats JSONB NOT NULL,
  PRIMARY KEY (round_index, contract_address)
);
