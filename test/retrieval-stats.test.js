import assert from 'node:assert'
import createDebug from 'debug'
import { Point } from '../lib/telemetry.js'
import {
  buildRetrievalStats,
  getValueAtPercentile,
  recordCommitteeSizes
} from '../lib/retrieval-stats.js'
import { VALID_MEASUREMENT } from './helpers/test-data.js'
import { assertPointFieldValue, getPointName } from './helpers/assertions.js'
import { groupMeasurementsToCommittees } from '../lib/committee.js'

/** @typedef {import('../lib/preprocess.js').Measurement} Measurement */

const debug = createDebug('test')

describe('retrieval statistics', () => {
  it('reports all stats', async () => {
    /** @type {Measurement[]} */
    const measurements = [
      {
        ...VALID_MEASUREMENT
      },
      {
        ...VALID_MEASUREMENT,
        timeout: true,
        retrievalResult: 'TIMEOUT',
        indexerResult: 'HTTP_NOT_ADVERTISED',

        start_at: new Date('2023-11-01T09:00:00.000Z').getTime(),
        first_byte_at: new Date('2023-11-01T09:00:10.000Z').getTime(),
        end_at: new Date('2023-11-01T09:00:50.000Z').getTime(),
        finished_at: new Date('2023-11-01T09:00:30.000Z').getTime(),
        byte_length: 2048
      },
      {
        ...VALID_MEASUREMENT,
        cid: 'bafyanother',
        carTooLarge: true,
        retrievalResult: 'CAR_TOO_LARGE',
        indexerResult: undefined,
        byte_length: 200 * 1024 * 1024
      },
      {
        ...VALID_MEASUREMENT,
        status_code: 500,
        retrievalResult: 'HTTP_500',
        indexerResult: 'NO_VALID_ADVERTISEMENT',
        participantAddress: '0xcheater',
        inet_group: 'abcd',
        start_at: new Date('2023-11-01T09:00:00.000Z').getTime(),
        first_byte_at: new Date('2023-11-01T09:10:10.000Z').getTime(),
        end_at: new Date('2023-11-01T09:00:20.000Z').getTime(),
        finished_at: new Date('2023-11-01T09:00:30.000Z').getTime(),
        byte_length: 2048,

        // invalid task
        cid: 'bafyinvalid',
        provider_address: '/dns4/production-ipfs-peer.pinata.cloud/tcp/3000/ws/p2p/Qma8ddFEQWEU8ijWvdxXm3nxU7oHsRtCykAaVz8WUYhiKn',
        protocol: 'bitswap'
      }
    ]

    const point = new Point('stats')
    buildRetrievalStats(measurements, point)
    debug('stats', point.fields)

    assertPointFieldValue(point, 'measurements', '4i')
    assertPointFieldValue(point, 'unique_tasks', '3i')
    assertPointFieldValue(point, 'success_rate', '0.25')
    assertPointFieldValue(point, 'participants', '2i')
    assertPointFieldValue(point, 'inet_groups', '2i')
    assertPointFieldValue(point, 'download_bandwidth', '209718272i')

    assertPointFieldValue(point, 'result_rate_OK', '0.25')
    assertPointFieldValue(point, 'result_rate_TIMEOUT', '0.25')
    assertPointFieldValue(point, 'result_rate_CAR_TOO_LARGE', '0.25')
    assertPointFieldValue(point, 'result_rate_HTTP_500', '0.25')

    assertPointFieldValue(point, 'ttfb_min', '1000i')
    assertPointFieldValue(point, 'ttfb_mean', '4000i')
    assertPointFieldValue(point, 'ttfb_p90', '8199i')
    assertPointFieldValue(point, 'ttfb_max', '10000i')

    assertPointFieldValue(point, 'duration_p10', '2000i')
    assertPointFieldValue(point, 'duration_mean', '18500i')
    assertPointFieldValue(point, 'duration_p90', '41000i')

    assertPointFieldValue(point, 'car_size_p10', '1228i')
    assertPointFieldValue(point, 'car_size_mean', '69906090i')
    assertPointFieldValue(point, 'car_size_p90', '167772569i')
    assertPointFieldValue(point, 'car_size_max', '209715200i')

    assertPointFieldValue(point, 'tasks_per_node_p5', '1i')
    assertPointFieldValue(point, 'tasks_per_node_p50', '2i')
    assertPointFieldValue(point, 'tasks_per_node_p95', '2i')

    assertPointFieldValue(point, 'indexer_rate_OK', '0.25')
    assertPointFieldValue(point, 'indexer_rate_UNDEFINED', '0.25')
    assertPointFieldValue(point, 'indexer_rate_HTTP_NOT_ADVERTISED', '0.25')
    assertPointFieldValue(point, 'indexer_rate_NO_VALID_ADVERTISEMENT', '0.25')

    // There are three unique tasks in our measurements, but one of the task does not have
    // any measurement with indexer result. From the two remaining tasks, only one of them
    // has a measurement reporting that HTTP retrieval was advertised to IPNI
    assertPointFieldValue(point, 'rate_of_deals_advertising_http', '0.5')
  })

  it('includes IPNI 5xx errors in breakdowns', async () => {
    /** @type {Measurement[]} */
    const measurements = [
      {
        ...VALID_MEASUREMENT
      },
      {
        ...VALID_MEASUREMENT,
        retrievalResult: 'IPNI_ERROR_504',
        indexerResult: 'ERROR_504'
      }
    ]

    const point = new Point('stats')
    buildRetrievalStats(measurements, point)
    debug('stats', point.fields)

    assertPointFieldValue(point, 'measurements', '2i')

    assertPointFieldValue(point, 'result_rate_OK', '0.5')
    assertPointFieldValue(point, 'result_rate_IPNI_ERROR_504', '0.5')

    assertPointFieldValue(point, 'indexer_rate_OK', '0.5')
    assertPointFieldValue(point, 'indexer_rate_ERROR_504', '0.5')
  })

  it('excludes IPNI 5xx errors from success rates', async () => {
    /** @type {Measurement[]} */
    const measurements = [
      {
        ...VALID_MEASUREMENT,
        protocol: 'http'
      },
      {
        ...VALID_MEASUREMENT,
        protocol: 'http',
        retrievalResult: 'IPNI_ERROR_504',
        indexerResult: 'ERROR_504'
      }
    ]

    const point = new Point('stats')
    buildRetrievalStats(measurements, point)
    debug('stats', point.fields)

    assertPointFieldValue(point, 'measurements', '2i')
    assertPointFieldValue(point, 'total_for_success_rates', '1i')
    assertPointFieldValue(point, 'success_rate', '1')
    assertPointFieldValue(point, 'success_rate_http', '1')
  })

  it('handles when all measurements reported IPNI 5xx', async () => {
    /** @type {Measurement[]} */
    const measurements = [
      {
        ...VALID_MEASUREMENT,
        retrievalResult: 'IPNI_ERROR_504',
        indexerResult: 'ERROR_504'
      }
    ]

    const point = new Point('stats')
    buildRetrievalStats(measurements, point)
    debug('stats', point.fields)

    assertPointFieldValue(point, 'measurements', '1i')
    assertPointFieldValue(point, 'total_for_success_rates', '0i')
    assertPointFieldValue(point, 'success_rate', '0')
    assertPointFieldValue(point, 'success_rate_http', '0')
  })

  it('handles first_byte_at set to unix epoch', () => {
    const measurements = [
      {
        ...VALID_MEASUREMENT,
        start_at: new Date('2023-11-01T09:00:00.000Z').getTime(),
        first_byte_at: new Date('1970-01-01T00:00:00.000Z').getTime()
      }
    ]
    const point = new Point('stats')
    buildRetrievalStats(measurements, point)
    debug('stats', point.fields)
    assertPointFieldValue(point, 'ttfb_min', undefined)
    assertPointFieldValue(point, 'ttfb_mean', undefined)
    assertPointFieldValue(point, 'ttfb_p90', undefined)
  })

  it('handles end_at set to unix epoch', () => {
    const measurements = [
      {
        ...VALID_MEASUREMENT,
        start_at: new Date('2023-11-01T09:00:00.000Z').getTime(),
        end_at: new Date('1970-01-01T00:00:00.000Z').getTime()
      }
    ]
    const point = new Point('stats')
    buildRetrievalStats(measurements, point)
    debug('stats', point.fields)
    assertPointFieldValue(point, 'duration_p10', undefined)
    assertPointFieldValue(point, 'duration_mean', undefined)
    assertPointFieldValue(point, 'duration_p90', undefined)
  })

  it('includes minerId in task definition', () => {
    const measurements = [
      {
        ...VALID_MEASUREMENT,
        minerId: 'f1one'
      },
      {
        ...VALID_MEASUREMENT,
        minerId: 'f1two'
      }
    ]
    const point = new Point('stats')
    buildRetrievalStats(measurements, point)
    debug('stats', point.fields)
    assertPointFieldValue(point, 'unique_tasks', '2i')
  })

  it('records histogram of "score per inet group"', async () => {
    /** @type {Measurement[]} */
    const measurements = [
      // inet group 1 - score=2
      {
        ...VALID_MEASUREMENT,
        taskingEvaluation: 'OK'
      },
      {
        ...VALID_MEASUREMENT,
        cid: 'bafyanother',
        retrievalResult: 'TIMEOUT',
        taskingEvaluation: 'OK'
      },
      {
        ...VALID_MEASUREMENT,
        taskingEvaluation: 'DUP_INET_GROUP'
      },
      // inet group 2 - score=3
      {
        ...VALID_MEASUREMENT,
        inet_group: 'ig2',
        taskingEvaluation: 'OK'
      },
      {
        ...VALID_MEASUREMENT,
        inet_group: 'ig2',
        cid: 'bafyanother',
        taskingEvaluation: 'OK'
      },
      {
        ...VALID_MEASUREMENT,
        inet_group: 'ig2',
        cid: 'bafythree',
        retrievalResult: 'TIMEOUT',
        taskingEvaluation: 'OK'
      },
      // inet group 3 - score=1
      {
        ...VALID_MEASUREMENT,
        inet_group: 'ig3',
        taskingEvaluation: 'OK'
      },
      // measurements not in majority are excluded
      {
        ...VALID_MEASUREMENT,
        inet_group: 'ig3',
        taskingEvaluation: 'OK',
        consensusEvaluation: 'MINORITY_RESULT'
      }
    ]
    for (const m of measurements) {
      if (!m.consensusEvaluation && m.taskingEvaluation === 'OK') m.consensusEvaluation = 'MAJORITY_RESULT'
    }

    const point = new Point('stats')
    buildRetrievalStats(measurements, point)
    debug('stats', point.fields)

    assertPointFieldValue(point, 'nano_score_per_inet_group_min', '166666667i' /* =1/6 */)
    assertPointFieldValue(point, 'nano_score_per_inet_group_p50', '333333333i' /* =2/6 */)
    assertPointFieldValue(point, 'nano_score_per_inet_group_max', '500000000i' /* =3/6 */)
  })

  it('records successful http rate', async () => {
    /** @type {Measurement[]} */
    const measurements = [
      {
        // Standard measurement, no http protocol used
        ...VALID_MEASUREMENT,
        protocol: 'graphsync'
      },
      {
        // A successful http measurement
        ...VALID_MEASUREMENT,
        protocol: 'http'
      },
      {
        ...VALID_MEASUREMENT,
        protocol: 'http',
        retrievalResult: 'HTTP_500'
      },
      {
        ...VALID_MEASUREMENT,
        // Should not be picked up, as the retrieval timed out
        retrievalResult: 'TIMEOUT',
        protocol: 'http'
      }
    ]

    const point = new Point('stats')
    buildRetrievalStats(measurements, point)
    debug('stats', point.fields)

    assertPointFieldValue(point, 'success_rate', '0.5')
    // Only one of the successful measurements used http
    assertPointFieldValue(point, 'success_rate_http', '0.25')
  })

  it('records network retrieval success rate', async () => {
    /** @type {Measurement[]} */
    const measurements = [
      {
        ...VALID_MEASUREMENT,
        alternativeProviderRetrievalResult: 'OK'
      },
      {
        ...VALID_MEASUREMENT,
        alternativeProviderRetrievalResult: 'HTTP_500'
      }
    ]

    const point = new Point('stats')
    buildRetrievalStats(measurements, point)
    debug('stats', point.fields)

    assertPointFieldValue(point, 'network_success_rate', '0.5')
  })
})

describe('getValueAtPercentile', () => {
  it('interpolates the values', () => {
    assert.strictEqual(
      getValueAtPercentile([10, 20, 30], 0.9),
      28
    )
  })
})

describe('recordCommitteeSizes', () => {
  it('reports unique subnets', async () => {
    const measurements = [
      // task 1
      {
        ...VALID_MEASUREMENT,
        inet_group: 'ig1'
      },
      {
        ...VALID_MEASUREMENT,
        participantAddress: '0xanother',
        // duplicate measurement in the same subnet, should be ignored
        inet_group: 'ig1'
      },
      {
        ...VALID_MEASUREMENT,
        inet_group: 'ig2'
      },
      {
        ...VALID_MEASUREMENT,
        inet_group: 'ig3'
      },

      // task 2
      {
        ...VALID_MEASUREMENT,
        cid: 'bafyanother'
      }
    ]
    measurements.forEach(m => { m.taskingEvaluation = 'OK' })

    const point = new Point('committees')
    const committees = groupMeasurementsToCommittees(measurements).values()
    recordCommitteeSizes(committees, point)
    debug(getPointName(point), point.fields)

    assertPointFieldValue(point, 'subnets_min', '1i')
    assertPointFieldValue(point, 'subnets_mean', '2i') // (3+1)/2 rounded down
    assertPointFieldValue(point, 'subnets_p50', '2i') // (3+1)/2 rounded down
    assertPointFieldValue(point, 'subnets_max', '3i')
  })

  it('reports unique participants', async () => {
    const measurements = [
      // task 1
      {
        ...VALID_MEASUREMENT,
        participantAddress: '0xone'
      },
      {
        ...VALID_MEASUREMENT,
        inet_group: 'ig1',
        // duplicate measurement by the same participant, should be ignored
        participantAddress: '0xone'
      },
      {
        ...VALID_MEASUREMENT,
        participantAddress: '0xtwo'
      },
      {
        ...VALID_MEASUREMENT,
        participantAddress: '0xthree'
      },

      // task 2
      {
        ...VALID_MEASUREMENT,
        cid: 'bafyanother'
      }
    ]
    measurements.forEach(m => { m.taskingEvaluation = 'OK' })

    const point = new Point('committees')
    const committees = groupMeasurementsToCommittees(measurements).values()
    recordCommitteeSizes(committees, point)
    debug(getPointName(point), point.fields)

    assertPointFieldValue(point, 'participants_min', '1i')
    assertPointFieldValue(point, 'participants_mean', '2i') // (3+1)/2 rounded down
    assertPointFieldValue(point, 'participants_p50', '2i') // (3+1)/2 rounded down
    assertPointFieldValue(point, 'participants_max', '3i')
  })

  it('reports unique nodes', async () => {
    const measurements = [
      // task 1
      {
        ...VALID_MEASUREMENT,
        inet_group: 'ig1',
        participantAddress: '0xone'
      },
      {
        ...VALID_MEASUREMENT,
        // duplicate measurement by the same participant in the same subnet, should be ignored
        inet_group: 'ig1',
        participantAddress: '0xone'
      },
      {
        ...VALID_MEASUREMENT,
        // same participant address but different subnet
        inet_group: 'ig2',
        participantAddress: '0xone'
      },
      {
        ...VALID_MEASUREMENT
      },

      // task 2
      {
        ...VALID_MEASUREMENT,
        cid: 'bafyanother'
      }
    ]
    measurements.forEach(m => { m.taskingEvaluation = 'OK' })

    const point = new Point('committees')
    const committees = groupMeasurementsToCommittees(measurements).values()
    recordCommitteeSizes(committees, point)
    debug(getPointName(point), point.fields)

    assertPointFieldValue(point, 'nodes_min', '1i')
    assertPointFieldValue(point, 'nodes_mean', '2i') // (3+1)/2 rounded down
    assertPointFieldValue(point, 'nodes_p50', '2i') // (3+1)/2 rounded down
    assertPointFieldValue(point, 'nodes_max', '3i')
  })

  it('reports number of all measurements', async () => {
    /** @type {Measurement[]} */
    const measurements = [
      // task 1
      {
        ...VALID_MEASUREMENT
      },
      {
        ...VALID_MEASUREMENT

      },
      {
        ...VALID_MEASUREMENT
      },

      // task 2
      {
        ...VALID_MEASUREMENT,
        cid: 'bafyanother'
      }
    ]
    for (const m of measurements) {
      m.taskingEvaluation = 'OK'
      m.consensusEvaluation = 'MAJORITY_RESULT'
    }

    const point = new Point('committees')
    const committees = groupMeasurementsToCommittees(measurements).values()
    measurements[0].consensusEvaluation = 'MINORITY_RESULT'

    recordCommitteeSizes(committees, point)
    debug(getPointName(point), point.fields)

    assertPointFieldValue(point, 'measurements_min', '1i')
    assertPointFieldValue(point, 'measurements_mean', '2i') // (3+1)/2 rounded down
    assertPointFieldValue(point, 'measurements_p50', '2i') // (3+1)/2 rounded down
    assertPointFieldValue(point, 'measurements_max', '3i')

    assertPointFieldValue(point, 'majority_ratios_percents_min', '66i') // 2/3 rounded down
    assertPointFieldValue(point, 'majority_ratios_percents_max', '100i')
  })
})
