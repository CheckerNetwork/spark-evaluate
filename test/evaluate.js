import { MAX_SCORE, evaluate, runFraudDetection } from '../lib/evaluate.js'
import { Point } from '../lib/telemetry.js'
import assert from 'node:assert'
import createDebug from 'debug'
import { SPARK_ROUND_DETAILS, VALID_MEASUREMENT, VALID_TASK, today } from './helpers/test-data.js'
import { assertPointFieldValue, assertRecordedTelemetryPoint } from './helpers/assertions.js'
import { RoundData } from '../lib/round.js'
import { DATABASE_URL } from '../lib/config.js'
import pg from 'pg'
import { beforeEach } from 'mocha'
import { migrateWithPgClient } from '../lib/migrate.js'

/** @import {RoundDetails} from '../lib/typings.js' */
/** @import {Measurement} from '../lib/preprocess.js' */

const debug = createDebug('test')
const logger = { log: debug, error: debug }

const telemetry = []
const recordTelemetry = (measurementName, fn) => {
  const point = new Point(measurementName)
  fn(point)
  debug('recordTelemetry(%s): %o', measurementName, point.fields)
  telemetry.push(point)
}
beforeEach(() => telemetry.splice(0))

const createPgClient = async () => {
  const pgClient = new pg.Client({ connectionString: DATABASE_URL })
  await pgClient.connect()
  return pgClient
}

describe('evaluate', async function () {
  this.timeout(5000)

  let pgClient
  before(async () => {
    pgClient = await createPgClient()
    await migrateWithPgClient(pgClient)
  })

  beforeEach(async () => {
    await pgClient.query('DELETE FROM retrieval_stats')
    await pgClient.query('DELETE FROM unpublished_provider_retrieval_result_stats_rounds')
  })

  after(async () => {
    await pgClient.end()
  })

  it('evaluates measurements', async () => {
    const round = new RoundData(0n)
    for (let i = 0; i < 10; i++) {
      round.measurements.push({ ...VALID_MEASUREMENT })
    }
    /** @returns {Promise<RoundDetails>} */
    const fetchRoundDetails = async () => ({ ...SPARK_ROUND_DETAILS, retrievalTasks: [VALID_TASK] })
    const setScoresCalls = []
    const setScores = async (participantAddresses, scores) => {
      setScoresCalls.push({ participantAddresses, scores })
    }
    const ieContract = {
      async getAddress () {
        return '0x811765AccE724cD5582984cb35f5dE02d587CA12'
      }
    }
    await evaluate({
      round,
      roundIndex: 0n,
      requiredCommitteeSize: 1,
      ieContract,
      setScores,
      fetchRoundDetails,
      recordTelemetry,
      createPgClient,
      logger,
      prepareProviderRetrievalResultStats: async () => {}
    })
    assert.strictEqual(setScoresCalls.length, 1)
    assert.deepStrictEqual(setScoresCalls[0].participantAddresses, [VALID_MEASUREMENT.participantAddress])
    assert.strictEqual(setScoresCalls[0].scores.length, 1)
    assert.strictEqual(
      setScoresCalls[0].scores[0],
      1000000000000000n
    )

    const point = telemetry.find(p => p.name === 'evaluate')
    assert(!!point,
      `No telemetry point "evaluate" was recorded. Actual points: ${JSON.stringify(telemetry.map(p => p.name))}`)
    assertPointFieldValue(point, 'total_nodes', '1i')
    // TODO: assert more point fields

    const { rows: publicStats } = await pgClient.query('SELECT * FROM retrieval_stats')
    assert.deepStrictEqual(publicStats, [{
      day: today(),
      miner_id: VALID_TASK.minerId,
      total: 1,
      successful: 1,
      // None of the measurements use http
      successful_http: 0,
      successful_http_head: 0
    }])
  })
  it('handles empty rounds', async () => {
    const round = new RoundData(0n)
    const setScoresCalls = []
    const setScores = async (participantAddresses, scores) => {
      setScoresCalls.push({ participantAddresses, scores })
    }
    const ieContract = {
      async getAddress () {
        return '0x811765AccE724cD5582984cb35f5dE02d587CA12'
      }
    }
    /** @returns {Promise<RoundDetails>} */
    const fetchRoundDetails = async () => ({ ...SPARK_ROUND_DETAILS, retrievalTasks: [VALID_TASK] })
    await evaluate({
      round,
      roundIndex: 0n,
      requiredCommitteeSize: 1,
      ieContract,
      setScores,
      fetchRoundDetails,
      recordTelemetry,
      createPgClient,
      logger,
      prepareProviderRetrievalResultStats: async () => {}
    })
    assert.strictEqual(setScoresCalls.length, 1)
    assert.deepStrictEqual(setScoresCalls[0].participantAddresses, [
      '0x000000000000000000000000000000000000dEaD'
    ])
    assert.deepStrictEqual(setScoresCalls[0].scores, [
      MAX_SCORE
    ])

    let point = telemetry.find(p => p.name === 'evaluate')
    assert(!!point,
      `No telemetry point "evaluate" was recorded. Actual points: ${JSON.stringify(telemetry.map(p => p.name))}`)

    // TODO: assert point fields

    point = telemetry.find(p => p.name === 'retrieval_stats_honest')
    assert(!!point,
          `No telemetry point "retrieval_stats_honest" was recorded. Actual points: ${JSON.stringify(telemetry.map(p => p.name))}`)
    assertPointFieldValue(point, 'measurements', '0i')
    assertPointFieldValue(point, 'unique_tasks', '0i')
    // no more fields are set for empty rounds
    assert.deepStrictEqual(Object.keys(point.fields), [
      'round_index',
      'measurements',
      'unique_tasks'
    ])
  })
  it('handles unknown rounds', async () => {
    const round = new RoundData(0n)
    const setScoresCalls = []
    const setScores = async (participantAddresses, scores) => {
      setScoresCalls.push({ participantAddresses, scores })
    }
    const ieContract = {
      async getAddress () {
        return '0x811765AccE724cD5582984cb35f5dE02d587CA12'
      }
    }
    /** @returns {Promise<RoundDetails>} */
    const fetchRoundDetails = async () => ({ ...SPARK_ROUND_DETAILS, retrievalTasks: [VALID_TASK] })
    await evaluate({
      round,
      roundIndex: 0n,
      ieContract,
      setScores,
      requiredCommitteeSize: 1,
      fetchRoundDetails,
      recordTelemetry,
      createPgClient,
      logger,
      prepareProviderRetrievalResultStats: async () => {}
    })
    assert.strictEqual(setScoresCalls.length, 1)
    assert.deepStrictEqual(setScoresCalls[0].participantAddresses, [
      '0x000000000000000000000000000000000000dEaD'
    ])
    assert.deepStrictEqual(setScoresCalls[0].scores, [
      MAX_SCORE
    ])
  })
  it('calculates reward shares', async () => {
    const round = new RoundData(0n)
    for (let i = 0; i < 5; i++) {
      round.measurements.push({ ...VALID_MEASUREMENT, participantAddress: '0x123' })
      round.measurements.push({ ...VALID_MEASUREMENT, participantAddress: '0x234', inet_group: 'group2' })
      round.measurements.push({
        ...VALID_MEASUREMENT,
        inet_group: 'group3',
        // invalid task
        cid: 'bafyreicnokmhmrnlp2wjhyk2haep4tqxiptwfrp2rrs7rzq7uk766chqvq',
        provider_address: '/dns4/production-ipfs-peer.pinata.cloud/tcp/3000/ws/p2p/Qma8ddFEQWEU8ijWvdxXm3nxU7oHsRtCykAaVz8WUYhiKn',
        protocol: 'bitswap'
      })
    }
    const setScoresCalls = []
    const setScores = async (participantAddresses, scores) => {
      setScoresCalls.push({ participantAddresses, scores })
    }
    const ieContract = {
      async getAddress () {
        return '0x811765AccE724cD5582984cb35f5dE02d587CA12'
      }
    }
    /** @returns {Promise<RoundDetails>} */
    const fetchRoundDetails = async () => ({ ...SPARK_ROUND_DETAILS, retrievalTasks: [VALID_TASK] })
    await evaluate({
      round,
      roundIndex: 0n,
      requiredCommitteeSize: 1,
      ieContract,
      setScores,
      recordTelemetry,
      fetchRoundDetails,
      createPgClient,
      logger,
      prepareProviderRetrievalResultStats: async () => {}
    })
    assert.strictEqual(setScoresCalls.length, 1)
    assert.deepStrictEqual(setScoresCalls[0].participantAddresses.sort(), ['0x123', '0x234'])
    const sum = (
      setScoresCalls[0].scores[0] +
      setScoresCalls[0].scores[1]
    ).toString()
    assert(
      ['1000000000000000', '999999999999999'].includes(sum),
      `Sum of scores not close enough. Got ${sum}`
    )
    assert.strictEqual(setScoresCalls[0].scores.length, 2)

    const point = assertRecordedTelemetryPoint(telemetry, 'evaluate')
    assert(!!point,
      `No telemetry point "evaluate" was recorded. Actual points: ${JSON.stringify(telemetry.map(p => p.name))}`)

    assertPointFieldValue(point, 'total_nodes', '3i')
  })

  it('rewards majority measurements only', async () => {
    const round = new RoundData(0n)
    // Majority
    round.measurements.push({ ...VALID_MEASUREMENT, participantAddress: '0x10' })
    round.measurements.push({ ...VALID_MEASUREMENT, participantAddress: '0x20', inet_group: 'group2' })
    // Minority
    round.measurements.push({ ...VALID_MEASUREMENT, participantAddress: '0x30', inet_group: 'group3', retrievalResult: 'TIMEOUT' })

    const setScoresCalls = []
    const setScores = async (participantAddresses, scores) => {
      setScoresCalls.push({ participantAddresses, scores })
    }
    const ieContract = {
      async getAddress () {
        return '0x811765AccE724cD5582984cb35f5dE02d587CA12'
      }
    }
    /** @returns {Promise<RoundDetails>} */
    const fetchRoundDetails = async () => ({ ...SPARK_ROUND_DETAILS, retrievalTasks: [VALID_TASK] })
    await evaluate({
      round,
      roundIndex: 0n,
      requiredCommitteeSize: 1,
      ieContract,
      setScores,
      recordTelemetry,
      fetchRoundDetails,
      createPgClient,
      logger,
      prepareProviderRetrievalResultStats: async () => {}
    })

    assert.strictEqual(setScoresCalls.length, 1)
    assert.deepStrictEqual(setScoresCalls[0].participantAddresses.sort(), ['0x10', '0x20'])
    assert.deepStrictEqual(setScoresCalls[0].scores, [MAX_SCORE / 2n, MAX_SCORE / 2n])
  })

  it('adds a dummy entry to ensure scores add up exactly to MAX_SCORE', async () => {
    const round = new RoundData(0n)
    round.measurements.push({ ...VALID_MEASUREMENT, participantAddress: '0x123', inet_group: 'ig1' })
    round.measurements.push({ ...VALID_MEASUREMENT, participantAddress: '0x234', inet_group: 'ig2' })
    round.measurements.push({ ...VALID_MEASUREMENT, participantAddress: '0x456', inet_group: 'ig3' })

    const setScoresCalls = []
    const setScores = async (participantAddresses, scores) => {
      setScoresCalls.push({ participantAddresses, scores })
    }
    const ieContract = {
      async getAddress () {
        return '0x811765AccE724cD5582984cb35f5dE02d587CA12'
      }
    }
    const logger = { log: debug, error: debug }
    /** @returns {Promise<RoundDetails>} */
    const fetchRoundDetails = async () => ({ ...SPARK_ROUND_DETAILS, retrievalTasks: [VALID_TASK] })
    await evaluate({
      round,
      roundIndex: 0n,
      requiredCommitteeSize: 1,
      ieContract,
      setScores,
      recordTelemetry,
      fetchRoundDetails,
      createPgClient,
      logger,
      prepareProviderRetrievalResultStats: async () => {}
    })
    assert.strictEqual(setScoresCalls.length, 1)
    const { scores, participantAddresses } = setScoresCalls[0]
    assert.strictEqual(scores.length, 4)
    const sum = scores.reduce((prev, score) => (prev ?? 0) + score)
    assert.strictEqual(sum, MAX_SCORE)
    assert.strictEqual(participantAddresses.sort()[0], '0x000000000000000000000000000000000000dEaD')
  })

  it('reports retrieval stats - honest & all', async () => {
    const round = new RoundData(0n)
    for (let i = 0; i < 5; i++) {
      round.measurements.push({ ...VALID_MEASUREMENT })
      round.measurements.push({
        ...VALID_MEASUREMENT,
        inet_group: 'group3',
        // invalid task
        cid: 'bafyreicnokmhmrnlp2wjhyk2haep4tqxiptwfrp2rrs7rzq7uk766chqvq',
        provider_address: '/dns4/production-ipfs-peer.pinata.cloud/tcp/3000/ws/p2p/Qma8ddFEQWEU8ijWvdxXm3nxU7oHsRtCykAaVz8WUYhiKn',
        protocol: 'bitswap',
        retrievalResult: 'TIMEOUT'
      })
    }
    const setScoresCalls = []
    const setScores = async (participantAddresses, scores) => {
      setScoresCalls.push({ participantAddresses, scores })
    }
    const ieContract = {
      async getAddress () {
        return '0x811765AccE724cD5582984cb35f5dE02d587CA12'
      }
    }
    /** @returns {Promise<RoundDetails>} */
    const fetchRoundDetails = async () => ({ ...SPARK_ROUND_DETAILS, retrievalTasks: [VALID_TASK] })
    await evaluate({
      round,
      roundIndex: 0n,
      requiredCommitteeSize: 1,
      ieContract,
      setScores,
      recordTelemetry,
      fetchRoundDetails,
      createPgClient,
      logger,
      prepareProviderRetrievalResultStats: async () => {}
    })

    let point = telemetry.find(p => p.name === 'retrieval_stats_honest')
    assert(!!point,
      `No telemetry point "retrieval_stats_honest" was recorded. Actual points: ${JSON.stringify(telemetry.map(p => p.name))}`)
    assertPointFieldValue(point, 'measurements', '1i')
    assertPointFieldValue(point, 'unique_tasks', '1i')
    assertPointFieldValue(point, 'success_rate', '1')
    // The default protocol is not http. While the overall success rate is 1, we expect the http success rate to be 0.
    assertPointFieldValue(point, 'success_rate_http', '0')

    point = telemetry.find(p => p.name === 'retrieval_stats_all')
    assert(!!point,
      `No telemetry point "retrieval_stats_all" was recorded. Actual points: ${JSON.stringify(telemetry.map(p => p.name))}`)
    assertPointFieldValue(point, 'measurements', '10i')
    assertPointFieldValue(point, 'unique_tasks', '2i')
    assertPointFieldValue(point, 'success_rate', '0.5')
    assertPointFieldValue(point, 'success_rate_http', '0')
  })

  it('prepares provider retrieval result stats', async () => {
    const round = new RoundData(0n)
    for (let i = 0; i < 5; i++) {
      round.measurements.push({ ...VALID_MEASUREMENT })
    }
    const setScores = async () => {}
    const prepareProviderRetrievalResultStatsCalls = []
    const prepareProviderRetrievalResultStats = async (round, committees) => {
      prepareProviderRetrievalResultStatsCalls.push({ round, committees })
    }
    const ieContract = {
      async getAddress () {
        return '0x811765AccE724cD5582984cb35f5dE02d587CA12'
      }
    }
    /** @returns {Promise<RoundDetails>} */
    const roundDetails = { ...SPARK_ROUND_DETAILS, retrievalTasks: [VALID_TASK] }
    const fetchRoundDetails = async () => roundDetails
    await evaluate({
      round,
      roundIndex: 0n,
      requiredCommitteeSize: 1,
      ieContract,
      setScores,
      recordTelemetry,
      fetchRoundDetails,
      createPgClient,
      logger,
      prepareProviderRetrievalResultStats
    })

    assert.strictEqual(prepareProviderRetrievalResultStatsCalls.length, 1)
    const call = prepareProviderRetrievalResultStatsCalls[0]
    assert(call.round instanceof RoundData)
    assert.strictEqual(call.round.details, roundDetails)
    assert(Array.isArray(call.committees))
    assert.strictEqual(call.committees.length, 1)
  })
})

describe('fraud detection', function () {
  this.timeout(5000)

  it('checks if measurements are for a valid task', async () => {
    /** @type {RoundDetails} */
    const sparkRoundDetails = {
      ...SPARK_ROUND_DETAILS,
      retrievalTasks: [
        {
          cid: 'QmUuEoBdjC8D1PfWZCc7JCSK8nj7TV6HbXWDHYHzZHCVGS',
          minerId: 'f1test'
        }
      ]
    }

    const measurements = [
      {
        ...VALID_MEASUREMENT,
        // valid task
        cid: 'QmUuEoBdjC8D1PfWZCc7JCSK8nj7TV6HbXWDHYHzZHCVGS',
        minerId: 'f1test'
      },
      {
        ...VALID_MEASUREMENT,
        // invalid task - wrong CID
        cid: 'bafyreicnokmhmrnlp2wjhyk2haep4tqxiptwfrp2rrs7rzq7uk766chqvq',
        minerId: 'f1test'
      },
      {
        ...VALID_MEASUREMENT,
        // invalid task - wrong minerId
        cid: 'QmUuEoBdjC8D1PfWZCc7JCSK8nj7TV6HbXWDHYHzZHCVGS',
        minerId: 'f1bad'
      }
    ]

    await runFraudDetection({
      roundIndex: 1n,
      measurements,
      sparkRoundDetails,
      requiredCommitteeSize: 1,
      logger
    })
    assert.deepStrictEqual(
      measurements.map(m => m.taskingEvaluation),
      ['OK', 'TASK_NOT_IN_ROUND', 'TASK_NOT_IN_ROUND']
    )
  })

  it('rejects redundant measurements from the same inet group', async () => {
    /** @type {RoundDetails} */
    const sparkRoundDetails = { ...SPARK_ROUND_DETAILS, retrievalTasks: [VALID_TASK] }
    const measurements = [
      { ...VALID_MEASUREMENT },
      { ...VALID_MEASUREMENT }
    ]

    await runFraudDetection({
      roundIndex: 1n,
      measurements,
      sparkRoundDetails,
      requiredCommitteeSize: 1,
      logger
    })
    assert.deepStrictEqual(
      measurements.map(m => m.taskingEvaluation),
      ['OK', 'DUP_INET_GROUP']
    )
  })

  it('picks different inet-group member to reward for each task', async () => {
    // We have two participants in the same inet group
    // They both complete the same valid tasks
    // Ideally, our algorithm should assign same reward to each one
    /** @type {RoundDetails} */
    const sparkRoundDetails = {
      ...SPARK_ROUND_DETAILS,
      retrievalTasks: [
        { ...VALID_TASK, cid: 'cid1', minerId: 'f1first' },
        { ...VALID_TASK, cid: 'cid2', minerId: 'f1first' },
        { ...VALID_TASK, cid: 'cid1', minerId: 'f1second' },
        { ...VALID_TASK, cid: 'cid2', minerId: 'f1second' }
      ]
    }
    // hard-coded to get deterministic results
    // the values are crafted to get distribute rewards among pa2 and pa3
    const timestamps = {
      pa1: {
        cid1: new Date('2023-11-01T09:00:01.000Z').getTime(),
        cid2: new Date('2023-11-01T09:00:21.000Z').getTime()
      },
      pa2: {
        cid1: new Date('2023-11-01T09:00:04.000Z').getTime(),
        cid2: new Date('2023-11-01T09:00:22.000Z').getTime()
      }
    }
    const measurements = []
    for (const participantAddress of Object.keys(timestamps)) {
      for (const task of sparkRoundDetails.retrievalTasks) {
        measurements.push({
          ...VALID_MEASUREMENT,
          ...task,
          participantAddress,
          // eslint-disable-next-line camelcase
          finished_at: timestamps[participantAddress][task.cid]
        })
      }
    }

    await runFraudDetection({
      roundIndex: 1n,
      measurements,
      sparkRoundDetails,
      requiredCommitteeSize: 1,
      logger
    })
    assert.deepStrictEqual(
      measurements.map(m => `${m.participantAddress}::${m.taskingEvaluation}`),
      [
        'pa1::OK',
        'pa1::DUP_INET_GROUP',
        'pa1::OK',
        'pa1::DUP_INET_GROUP',
        'pa2::DUP_INET_GROUP',
        'pa2::OK',
        'pa2::DUP_INET_GROUP',
        'pa2::OK'
      ]
    )
  })

  it('rewards at most `maxTasksPerNode` measurements in each inet group', async () => {
    // Consider the following situation:
    // A single operator runs many Station instances in the same inet_group, assigning each
    // instance a unique participant address, and picking the tasks in such way that each instance
    // reports measurements for different tasks. With 1000 tasks per round and 15 tasks per
    // instance, it’s possible to operate 66 instances that each receives the maximum possible score
    // for each round.

    /** @type {RoundDetails} */
    const sparkRoundDetails = {
      ...SPARK_ROUND_DETAILS,
      maxTasksPerNode: 2,
      retrievalTasks: [
        { ...VALID_TASK, cid: 'cid1', minerId: 'f1first' },
        { ...VALID_TASK, cid: 'cid2', minerId: 'f1first' },
        { ...VALID_TASK, cid: 'cid1', minerId: 'f1second' },
        { ...VALID_TASK, cid: 'cid2', minerId: 'f1second' }
      ]
    }

    const measurements = [
      // the first participant completes tasks #1 and #4
      { ...VALID_MEASUREMENT, ...sparkRoundDetails.retrievalTasks[0], participantAddress: 'pa1' },
      { ...VALID_MEASUREMENT, ...sparkRoundDetails.retrievalTasks[3], participantAddress: 'pa1' },
      // the second participant completes tasks #2 and #3
      { ...VALID_MEASUREMENT, ...sparkRoundDetails.retrievalTasks[1], participantAddress: 'pa2' },
      { ...VALID_MEASUREMENT, ...sparkRoundDetails.retrievalTasks[2], participantAddress: 'pa2' }
    ]

    // Ensure `finished_at` values are unique for each measurement, it's an assumption we rely on
    const start = Date.now()
    measurements.forEach((m, ix) => { m.finished_at = start + ix * 1_000 })

    await runFraudDetection({
      roundIndex: 1n,
      measurements,
      sparkRoundDetails,
      requiredCommitteeSize: 1,
      logger
    })

    assert.strictEqual(
      measurements.filter(m => m.taskingEvaluation === 'OK').length,
      2 // maxTasksPerNode
    )
  })

  it('calculates aggregate stats of participant group-winning rate', async () => {
    // Let's create three different tasks and three participants where two share the same inet group.
    // All three participants measure all three tasks.
    /** @type {RoundDetails} */
    const sparkRoundDetails = {
      ...SPARK_ROUND_DETAILS,
      retrievalTasks: [
        { ...VALID_TASK, cid: 'cid1' },
        { ...VALID_TASK, cid: 'cid2' },
        { ...VALID_TASK, cid: 'cid3' }
      ]
    }

    const participantSubnets = {
      pa1: 'ig1',
      pa2: 'ig2',
      pa3: 'ig2' // same as above!
    }
    // hard-coded to get deterministic results
    // the values are crafted to distribute rewards between pa2 and pa3
    const timestamps = {
      pa1: {
        cid1: new Date('2023-11-01T09:00:01.000Z').getTime(),
        cid2: new Date('2023-11-01T09:00:21.000Z').getTime(),
        cid3: new Date('2023-11-01T09:00:41.000Z').getTime()
      },
      pa2: {
        cid1: new Date('2023-11-01T09:00:04.000Z').getTime(),
        cid2: new Date('2023-11-01T09:00:22.000Z').getTime(),
        cid3: new Date('2023-11-01T09:00:42.000Z').getTime()
      },
      pa3: {
        cid1: new Date('2023-11-01T09:00:03.000Z').getTime(),
        cid2: new Date('2023-11-01T09:00:23.000Z').getTime(),
        cid3: new Date('2023-11-01T09:03:43.000Z').getTime()
      }
    }

    /** @type {import('../lib/preprocess.js').Measurement[]} */
    const measurements = []

    // eslint-disable-next-line camelcase
    for (const [participantAddress, inet_group] of Object.entries(participantSubnets)) {
      for (const task of sparkRoundDetails.retrievalTasks) {
        measurements.push({
          ...VALID_MEASUREMENT,
          ...task,
          participantAddress,
          // eslint-disable-next-line camelcase
          inet_group,
          finished_at: timestamps[participantAddress][task.cid]
        })
      }
    }

    await runFraudDetection({
      roundIndex: 1n,
      measurements,
      sparkRoundDetails,
      requiredCommitteeSize: 1,
      logger
    })

    assert.deepStrictEqual(
      measurements.map(m => `${m.participantAddress}::${m.taskingEvaluation}`),
      [
        'pa1::OK',
        'pa1::OK',
        'pa1::OK',

        'pa2::DUP_INET_GROUP',
        'pa2::OK',
        'pa2::DUP_INET_GROUP',

        'pa3::OK',
        'pa3::DUP_INET_GROUP',
        'pa3::OK'
      ]
    )
  })

  it('rejects measurements above maxTasksPerNode', async () => {
    /** @type {RoundDetails} */
    const sparkRoundDetails = {
      ...SPARK_ROUND_DETAILS,
      retrievalTasks: [
        {
          ...VALID_TASK,
          cid: 'cid1'
        }, {
          ...VALID_TASK,
          cid: 'cid2'
        }, {
          ...VALID_TASK,
          cid: 'cid3'
        }
      ],
      maxTasksPerNode: 1
    }
    const measurements = [
      {
        ...VALID_MEASUREMENT,
        cid: 'cid1',
        inet_group: 'group1'
      },
      {
        ...VALID_MEASUREMENT,
        // This Station ID is carefully chosen so that this station has a different set
        // of allowed tasks than the default VALID_STATION_ID
        stationId: 'another-station-123',
        cid: 'cid3',
        inet_group: 'group1'
      },
      {
        ...VALID_MEASUREMENT,
        cid: 'cid1',
        inet_group: 'group2'
      },
      {
        ...VALID_MEASUREMENT,
        cid: 'cid3'
      }
    ]

    await runFraudDetection({
      roundIndex: 1n,
      measurements,
      sparkRoundDetails,
      requiredCommitteeSize: 1,
      logger
    })

    assert.deepStrictEqual(
      measurements.map(m => m.taskingEvaluation),
      [
        'OK',
        'TOO_MANY_TASKS',
        // The second measurement was submitted from a different subnet (inet group).
        // This usually happens when a single participant runs two station instances in different
        // networks. That's a valid behavior, the measurement is accepted and rewarded.
        'OK',
        'TASK_WRONG_NODE'
      ]
    )
  })

  it('rejects tasks not allowed by the tasking algorithm', async () => {
    /** @type {RoundDetails} */
    const sparkRoundDetails = {
      ...SPARK_ROUND_DETAILS,
      maxTasksPerNode: 4,
      startEpoch: '4080000',
      retrievalTasks: [
        { cid: 'bafyone', minerId: 'f010' },
        { cid: 'bafyone', minerId: 'f020' },
        { cid: 'bafyone', minerId: 'f030' },
        { cid: 'bafyone', minerId: 'f040' },

        { cid: 'bafytwo', minerId: 'f010' },
        { cid: 'bafytwo', minerId: 'f020' },
        { cid: 'bafytwo', minerId: 'f030' },
        { cid: 'bafytwo', minerId: 'f040' }
      ]
    }

    const stationId = 'some-fixed-station-id'

    /** @type {Measurement[]} */
    const measurements = sparkRoundDetails.retrievalTasks.map(task => ({
      ...VALID_MEASUREMENT,
      ...task,
      stationId
    }))

    await runFraudDetection({
      roundIndex: 1n,
      measurements,
      sparkRoundDetails,
      requiredCommitteeSize: 1,
      logger
    })

    assert.deepStrictEqual(measurements.map(m => `${m.cid}::${m.minerId}::${m.taskingEvaluation}`), [
      'bafyone::f010::TASK_WRONG_NODE',
      'bafyone::f020::OK',
      'bafyone::f030::OK',
      'bafyone::f040::OK',

      'bafytwo::f010::TASK_WRONG_NODE',
      'bafytwo::f020::OK',
      'bafytwo::f030::TASK_WRONG_NODE',
      'bafytwo::f040::TASK_WRONG_NODE'
    ])
  })
})
