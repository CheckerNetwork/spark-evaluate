import { Point } from '@influxdata/influxdb-client'

export {
  Point
}


/**
 * Details of a retrieval task as returned by SPARK HTTP API.
 */
export interface RetrievalTask {
  cid: string;
  minerId: string;
  clients?: string[];
}

/**
 * Details of a SPARK round as returned by SPARK HTTP API.
 */
export interface RoundDetails {
  roundId: string; // BigInt serialized as String (JSON does not support BigInt)
  retrievalTasks: RetrievalTask[];
  maxTasksPerNode: number;
  startEpoch: string; // BigInt serialized as String (JSON does not support BigInt)
}

export type RecordTelemetryFn = (
  name: string,
  fn: (point: Point) => void
) => void

export type ConsensusNotFoundReason =
| 'COMMITTEE_TOO_SMALL'
| 'MAJORITY_NOT_FOUND'

export type ConsensusEvaluation =
  | 'MAJORITY_RESULT'
  | 'MINORITY_RESULT'
  | ConsensusNotFoundReason

// When adding a new enum value, remember to update the summary initializer inside `evaluate()`
export type TaskingEvaluation =
  | 'OK'
  | 'TASK_NOT_IN_ROUND'
  | 'TASK_WRONG_NODE'
  | 'DUP_INET_GROUP'
  | 'TOO_MANY_TASKS'

// When adding a new enum value, remember to update the summary initializer inside `reportRetrievalStats()`
export type RetrievalResult =
  | 'OK'
  | 'TIMEOUT'
  | 'CAR_TOO_LARGE'
  | 'UNKNOWN_FETCH_ERROR'
  | 'UNSUPPORTED_MULTIADDR_FORMAT'
  | 'HOSTNAME_DNS_ERROR'
  | 'CONNECTION_REFUSED'
  | 'UNSUPPORTED_CID_HASH_ALGO'
  | 'CONTENT_VERIFICATION_FAILED'
  | 'UNEXPECTED_CAR_BLOCK'
  | 'CANNOT_PARSE_CAR_FILE'
  | 'IPNI_NO_VALID_ADVERTISEMENT'
  | 'IPNI_ERROR_FETCH'
  | `IPNI_ERROR_${number}`
  | `HTTP_${number}`
  | `LASSIE_${number}`
  | 'UNKNOWN_ERROR'
  | ConsensusNotFoundReason


// Data coming from spark-api and spark-publish
export interface RawMeasurement {
  participant_address: string;
  spark_version: string;

  miner_id: string;
  cid: string;
  provider_id: string;
  provider_address: string;
  protocol: string;
  inet_group: string;
  station_id: string;

  start_at: string;
  first_byte_at: string;
  end_at: string;
  finished_at: string;

  status_code: number | undefined | null;
  head_status_code: number | undefined | null;
  timeout: boolean;
  byte_length: number;
  car_too_large: boolean;
  car_checksum: string;
  // See https://github.com/filecoin-station/spark/blob/main/lib/ipni-client.js
  indexer_result:
    | 'OK'
    | 'HTTP_NOT_ADVERTISED'
    | 'NO_VALID_ADVERTISEMENT'
    | 'ERROR_FETCH'
    | `ERROR_${number}`;
}

export type CreatePgClient = () => Promise<import('pg').Client>;
