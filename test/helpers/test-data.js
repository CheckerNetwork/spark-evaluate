import { groupMeasurementsToCommittees } from '../../lib/committee.js'

export const VALID_PARTICIPANT_ADDRESS = '0x000000000000000000000000000000000000dEaD'
export const VALID_STATION_ID = '8800000000000000000000000000000000000000000000000000000000000000000000000000000000000000'
export const VALID_INET_GROUP = 'some-group-id'
export const MEASUREMENT_BATCH = 'bafybeie5rekb2jox77ow64wjjd2bjdsp6d3yeivhzzd234hnbpscfjarv4'
export const ROUND_DETAILS = 'bafybeie5rekb2jox77ow64wjjd2bjdsp6d3yeivhzzd234hnbpscfjarv4'

/** @import { Measurement} from  '../../lib/preprocess.js' */

export const VALID_TASK = {
  cid: 'QmUuEoBdjC8D1PfWZCc7JCSK8nj7TV6HbXWDHYHzZHCVGS',
  minerId: 'f1test',
  clients: ['f1client']
}
Object.freeze(VALID_TASK)

/** @type {Measurement} */
export const VALID_MEASUREMENT = {
  cid: VALID_TASK.cid,
  minerId: VALID_TASK.minerId,
  provider_address: '/dns4/production-ipfs-peer.pinata.cloud/tcp/3000/ws/p2p/Qma8ddFEQWEU8ijWvdxXm3nxU7oHsRtCykAaVz8WUYhiKn',
  providerId: 'PROVIDERID',
  spark_version: '1.10.4',
  protocol: 'bitswap',
  participantAddress: VALID_PARTICIPANT_ADDRESS,
  stationId: VALID_STATION_ID,
  inet_group: VALID_INET_GROUP,
  status_code: 200,
  head_status_code: 200,
  // TODO: these fields are not part of the Measurement object yet
  // timeout: false,
  // car_too_large: false,
  start_at: new Date('2023-11-01T09:00:00.000Z').getTime(),
  first_byte_at: new Date('2023-11-01T09:00:01.000Z').getTime(),
  end_at: new Date('2023-11-01T09:00:02.000Z').getTime(),
  finished_at: new Date('2023-11-01T09:00:10.000Z').getTime(),
  timeout: false,
  byte_length: 1024,
  carChecksum: 'some-car-checksum',
  carTooLarge: false,
  retrievalResult: 'OK',
  indexerResult: 'OK',
  taskingEvaluation: null,
  consensusEvaluation: null
}

// Fraud detection is mutating the measurements parsed from JSON
// To prevent tests from accidentally mutating data used by subsequent tests,
// we freeze this test data object. If we forget to clone this default measurement
// then such test will immediately fail.
Object.freeze(VALID_MEASUREMENT)

/** @type {import('../../lib/typings.js').RoundDetails} */
export const SPARK_ROUND_DETAILS = {
  roundId: '0',
  maxTasksPerNode: 15,
  startEpoch: '4080000',
  retrievalTasks: [VALID_TASK]
}
Object.freeze(SPARK_ROUND_DETAILS)

export const today = () => {
  const d = new Date()
  d.setHours(0)
  d.setMinutes(0)
  d.setSeconds(0)
  d.setMilliseconds(0)
  return d
}

/**
 * @param {Iterable<Measurement>} acceptedMeasurements
 */
export const buildEvaluatedCommitteesFromMeasurements = (acceptedMeasurements) => {
  for (const m of acceptedMeasurements) {
    m.taskingEvaluation = 'OK'
    if (!m.consensusEvaluation) m.consensusEvaluation = 'MAJORITY_RESULT'
  }

  const committees = [...groupMeasurementsToCommittees(acceptedMeasurements).values()]
  for (const c of committees) {
    c.decision = {
      indexMajorityFound: true,
      indexerResult: c.measurements[0].indexerResult,
      retrievalMajorityFound: true,
      retrievalResult: c.measurements[0].retrievalResult
    }
  }
  return committees
}
