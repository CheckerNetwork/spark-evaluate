<<<<<<< HEAD
import { MAX_SCORE, evaluate } from '../lib/evaluate.js'
=======
import { evaluate } from '../lib/evaluate.js'
>>>>>>> 354bf2c119dd0f3f05dc62721aba90b50d1bb698
import { Point } from '../lib/telemetry.js'
import assert from 'node:assert'
import createDebug from 'debug'
import { SPARK_ROUND_DETAILS, VALID_MEASUREMENT, VALID_TASK, today } from './helpers/test-data.js'
import { assertPointFieldValue } from './helpers/assertions.js'
import { RoundData } from '../lib/round.js'
import { DATABASE_URL } from '../lib/config.js'
import pg from 'pg'
import { beforeEach } from 'mocha'
import { migrateWithPgClient } from '../lib/migrate.js'

/** @import {RoundDetails} from '../lib/typings.js' */

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
      fetchRoundDetails,
      recordTelemetry,
      createPgClient,
      logger,
      prepareProviderRetrievalResultStats: async () => {}
    })

    const point = telemetry.find(p => p.name === 'evaluate')
    assert(!!point,
      `No telemetry point "evaluate" was recorded. Actual points: ${JSON.stringify(telemetry.map(p => p.name))}`)
    assertPointFieldValue(point, 'total_nodes', '1i')
    // TODO: assert more point fields

    const { rows: publicStats } = await pgClient.query('SELECT * FROM retrieval_stats')
    assert.deepStrictEqual(publicStats, [{
      day: today(),
      miner_id: VALID_TASK.minerId,
      total: 10,
      successful: 10,
      // None of the measurements use http
      successful_http: 0,
      successful_http_head: 0
    }])
  })
  it('handles empty rounds', async () => {
    const round = new RoundData(0n)
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
      fetchRoundDetails,
      recordTelemetry,
      createPgClient,
      logger,
      prepareProviderRetrievalResultStats: async () => {}
    })
    let point = telemetry.find(p => p.name === 'evaluate')
    assert(!!point,
      `No telemetry point "evaluate" was recorded. Actual points: ${JSON.stringify(telemetry.map(p => p.name))}`)

    // TODO: assert point fields

    point = telemetry.find(p => p.name === 'retrieval_stats_all')
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
      requiredCommitteeSize: 1,
      fetchRoundDetails,
      recordTelemetry,
      createPgClient,
      logger,
      prepareProviderRetrievalResultStats: async () => {}
    })
  })

  it('reports retrieval stats', async () => {
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
      recordTelemetry,
      fetchRoundDetails,
      createPgClient,
      logger,
      prepareProviderRetrievalResultStats: async () => {}
    })

    const point = telemetry.find(p => p.name === 'retrieval_stats_all')
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
