import assert from 'node:assert'
import pg from 'pg'

import { DATABASE_URL } from '../lib/config.js'
import { migrateWithPgClient } from '../lib/migrate.js'
import { buildEvaluatedCommitteesFromMeasurements, VALID_MEASUREMENT } from './helpers/test-data.js'
import { updateDailyAllocatorRetrievalStats, updateDailyClientRetrievalStats, updatePublicStats } from '../lib/public-stats.js'
import { beforeEach } from 'mocha'
import { groupMeasurementsToCommittees } from '../lib/committee.js'

/** @typedef {import('../lib/preprocess.js').Measurement} Measurement */

const createPgClient = async () => {
  const pgClient = new pg.Client({ connectionString: DATABASE_URL })
  await pgClient.connect()
  return pgClient
}

describe('public-stats', () => {
  let pgClient
  before(async () => {
    pgClient = await createPgClient()
    await migrateWithPgClient(pgClient)
  })

  let today
  beforeEach(async () => {
    await pgClient.query('DELETE FROM retrieval_stats')
    await pgClient.query('DELETE FROM indexer_query_stats')
    await pgClient.query('DELETE FROM daily_deals')
    await pgClient.query('DELETE FROM retrieval_timings')
    await pgClient.query('DELETE FROM daily_client_retrieval_stats')
    await pgClient.query('DELETE FROM daily_allocator_retrieval_stats')

    // Run all tests inside a transaction to ensure `now()` always returns the same value
    // See https://dba.stackexchange.com/a/63549/125312
    // This avoids subtle race conditions when the tests are executed around midnight.
    await pgClient.query('BEGIN TRANSACTION')

    today = await getCurrentDate()
  })

  afterEach(async () => {
    await pgClient.query('END TRANSACTION')
  })

  after(async () => {
    await pgClient.end()
  })

  describe('retrieval_stats', () => {
    it('creates or updates the row for today - one miner only', async () => {
      /** @type {Measurement[]} */
      const honestMeasurements = [
        { ...VALID_MEASUREMENT, cid: 'cidone', retrievalResult: 'OK' },
        { ...VALID_MEASUREMENT, cid: 'cidtwo', retrievalResult: 'TIMEOUT' }
      ]
      const allMeasurements = honestMeasurements
      let committees = buildEvaluatedCommitteesFromMeasurements(honestMeasurements)

      await updatePublicStats({
        createPgClient,
        committees,
        allMeasurements,
        findDealClients: (_minerId, _cid) => ['f0client'],
        findDealAllocators: (_minerId, _cid) => ['f0allocator']
      })

      const { rows: created } = await pgClient.query(
        'SELECT day::TEXT, total, successful FROM retrieval_stats'
      )
      assert.deepStrictEqual(created, [
        { day: today, total: 2, successful: 1 }
      ])

      honestMeasurements.push({ ...VALID_MEASUREMENT, retrievalResult: 'UNKNOWN_ERROR' })
      committees = buildEvaluatedCommitteesFromMeasurements(honestMeasurements)
      await updatePublicStats({
        createPgClient,
        committees,
        allMeasurements,
        findDealClients: (_minerId, _cid) => ['f0client'],
        findDealAllocators: (_minerId, _cid) => ['f0allocator']
      })

      const { rows: updated } = await pgClient.query(
        'SELECT day::TEXT, total, successful FROM retrieval_stats'
      )
      assert.deepStrictEqual(updated, [
        { day: today, total: 2 + 3, successful: 1 + 1 }
      ])
    })
    it('calculates successful http retrievals correctly', async () => {
      /** @type {Measurement[]} */
      const honestMeasurements = [
        { ...VALID_MEASUREMENT, protocol: 'http', retrievalResult: 'OK', head_status_code: 200 },
        { ...VALID_MEASUREMENT, protocol: 'graphsync', retrievalResult: 'OK' },
        { ...VALID_MEASUREMENT, protocol: 'http', retrievalResult: 'HTTP_500' },
        { ...VALID_MEASUREMENT, protocol: 'graphsync', retrievalResult: 'LASSIE_500' }
      ]
      const allMeasurements = honestMeasurements
      let committees = buildEvaluatedCommitteesFromMeasurements(honestMeasurements)

      await updatePublicStats({
        createPgClient,
        committees,
        allMeasurements,
        findDealClients: (_minerId, _cid) => ['f0client'],
        findDealAllocators: (_minerId, _cid) => ['f0allocator']
      })

      const { rows: created } = await pgClient.query(
        'SELECT day::TEXT, total, successful, successful_http, successful_http_head FROM retrieval_stats'
      )
      assert.deepStrictEqual(created, [
        { day: today, total: 4, successful: 2, successful_http: 1, successful_http_head: 1 }
      ])

      // Let's add another successful http retrieval to make sure the updating process works as expected
      honestMeasurements.push({ ...VALID_MEASUREMENT, retrievalResult: 'OK', protocol: 'http', head_status_code: 200 })
      committees = buildEvaluatedCommitteesFromMeasurements(honestMeasurements)
      await updatePublicStats({
        createPgClient,
        committees,
        allMeasurements,
        findDealClients: (_minerId, _cid) => ['f0client'],
        findDealAllocators: (_minerId, _cid) => ['f0allocator']
      })

      const { rows: updated } = await pgClient.query(
        'SELECT day::TEXT, total, successful, successful_http, successful_http_head FROM retrieval_stats'
      )
      assert.deepStrictEqual(updated, [
        { day: today, total: 4 + 5, successful: 2 + 3, successful_http: 1 + 2, successful_http_head: 1 + 2 }
      ])
    })

    it('creates or updates the row for today - multiple miners', async () => {
      /** @type {Measurement[]} */
      const honestMeasurements = [
        { ...VALID_MEASUREMENT, minerId: 'f1first', retrievalResult: 'OK' },
        { ...VALID_MEASUREMENT, minerId: 'f1first', retrievalResult: 'TIMEOUT' },
        { ...VALID_MEASUREMENT, minerId: 'f1second', retrievalResult: 'OK' }
      ]
      const allMeasurements = honestMeasurements
      let committees = buildEvaluatedCommitteesFromMeasurements(honestMeasurements)

      await updatePublicStats({

        createPgClient,
        committees,
        allMeasurements,
        findDealClients: (_minerId, _cid) => ['f0client'],
        findDealAllocators: (_minerId, _cid) => ['f0allocator']
      })
      const { rows: created } = await pgClient.query(
        'SELECT day::TEXT, miner_id, total, successful FROM retrieval_stats'
      )
      assert.deepStrictEqual(created, [
        { day: today, miner_id: 'f1first', total: 2, successful: 1 },
        { day: today, miner_id: 'f1second', total: 1, successful: 1 }
      ])

      honestMeasurements.push({ ...VALID_MEASUREMENT, minerId: 'f1first', retrievalResult: 'UNKNOWN_ERROR' })
      honestMeasurements.push({ ...VALID_MEASUREMENT, minerId: 'f1second', retrievalResult: 'UNKNOWN_ERROR' })
      committees = buildEvaluatedCommitteesFromMeasurements(honestMeasurements)

      await updatePublicStats({
        createPgClient,
        committees,
        allMeasurements,
        findDealClients: (_minerId, _cid) => ['f0client'],
        findDealAllocators: (_minerId, _cid) => ['f0allocator']
      })

      const { rows: updated } = await pgClient.query(
        'SELECT day::TEXT, miner_id, total, successful FROM retrieval_stats'
      )
      assert.deepStrictEqual(updated, [
        { day: today, miner_id: 'f1first', total: 2 + 3, successful: 1 + 1 },
        { day: today, miner_id: 'f1second', total: 1 + 2, successful: 1 + 1 }
      ])
    })

    it('includes minority results', async () => {
      /** @type {Measurement[]} */
      const honestMeasurements = [
        { ...VALID_MEASUREMENT, retrievalResult: 'OK' },
        { ...VALID_MEASUREMENT, retrievalResult: 'OK' },
        { ...VALID_MEASUREMENT, retrievalResult: 'TIMEOUT' }
      ]
      for (const m of honestMeasurements) m.taskingEvaluation = 'OK'
      const allMeasurements = honestMeasurements
      const committees = [...groupMeasurementsToCommittees(honestMeasurements).values()]
      assert.strictEqual(committees.length, 1)
      committees[0].evaluate({ requiredCommitteeSize: 3 })
      assert.deepStrictEqual(allMeasurements.map(m => m.taskingEvaluation), [
        'OK',
        'OK',
        'OK'
      ])
      assert.deepStrictEqual(allMeasurements.map(m => m.consensusEvaluation), [
        'MAJORITY_RESULT',
        'MAJORITY_RESULT',
        'MINORITY_RESULT'
      ])
      // The last measurement is rejected because it's a minority result
      honestMeasurements.splice(2)

      await updatePublicStats({
        createPgClient,
        committees,
        allMeasurements,
        findDealClients: (_minerId, _cid) => ['f0client'],
        findDealAllocators: (_minerId, _cid) => ['f0allocator']
      })

      const { rows: created } = await pgClient.query(
        'SELECT day::TEXT, total, successful, successful_http FROM retrieval_stats'
      )
      assert.deepStrictEqual(created, [
        { day: today, total: 3, successful: 2, successful_http: 0 }
      ])
    })

    it('excludes IPNI 5xx results', async () => {
      /** @type {Measurement[]} */
      const allMeasurements = [
        { ...VALID_MEASUREMENT, retrievalResult: 'OK' },
        { ...VALID_MEASUREMENT, retrievalResult: 'OK' },
        { ...VALID_MEASUREMENT, retrievalResult: 'OK' },
        { ...VALID_MEASUREMENT, retrievalResult: 'IPNI_ERROR_504' },
        { ...VALID_MEASUREMENT, retrievalResult: 'TIMEOUT' }
      ]
      for (const m of allMeasurements) m.taskingEvaluation = 'OK'
      const committees = [...groupMeasurementsToCommittees(allMeasurements).values()]
      assert.strictEqual(committees.length, 1)
      committees[0].evaluate({ requiredCommitteeSize: 3 })
      assert.deepStrictEqual(allMeasurements.map(m => m.consensusEvaluation), [
        'MAJORITY_RESULT',
        'MAJORITY_RESULT',
        'MAJORITY_RESULT',
        'MINORITY_RESULT',
        'MINORITY_RESULT'
      ])

      await updatePublicStats({
        createPgClient,
        committees,
        allMeasurements,
        findDealClients: (_minerId, _cid) => ['f0client'],
        findDealAllocators: (_minerId, _cid) => ['f0allocator']
      })

      const { rows: created } = await pgClient.query(
        'SELECT day::TEXT, total, successful FROM retrieval_stats'
      )
      // 5 measurements were recorded in total, but only 4 are counted
      assert.deepStrictEqual(created, [
        { day: today, total: 4, successful: 3 }
      ])
    })
  })

  describe('indexer_query_stats', () => {
    it('creates or updates the row for today', async () => {
      /** @type {Measurement[]} */
      const honestMeasurements = [
        { ...VALID_MEASUREMENT, indexerResult: 'OK' },
        { ...VALID_MEASUREMENT, cid: 'bafy2', indexerResult: 'HTTP_NOT_ADVERTISED' },
        { ...VALID_MEASUREMENT, cid: 'bafy3', indexerResult: 'ERROR_404' }
      ]
      const allMeasurements = honestMeasurements
      let committees = buildEvaluatedCommitteesFromMeasurements(honestMeasurements)

      await updatePublicStats({
        createPgClient,
        committees,
        allMeasurements,
        findDealClients: (_minerId, _cid) => ['f0client'],
        findDealAllocators: (_minerId, _cid) => ['f0allocator']
      })

      const { rows: created } = await pgClient.query(
        'SELECT day::TEXT, deals_tested, deals_advertising_http FROM indexer_query_stats'
      )
      assert.deepStrictEqual(created, [
        { day: today, deals_tested: 3, deals_advertising_http: 1 }
      ])

      // Notice: this measurement is for the same task as honestMeasurements[0], therefore it's
      // effectively ignored as the other measurement was successful.
      honestMeasurements.push({ ...VALID_MEASUREMENT, indexerResult: 'ERROR_FETCH' })
      // This is a measurement for a new task.
      honestMeasurements.push({ ...VALID_MEASUREMENT, cid: 'bafy4', indexerResult: 'ERROR_FETCH' })
      committees = buildEvaluatedCommitteesFromMeasurements(honestMeasurements)

      await updatePublicStats({
        createPgClient,
        committees,
        allMeasurements,
        findDealClients: (_minerId, _cid) => ['f0client'],
        findDealAllocators: (_minerId, _cid) => ['f0allocator']
      })

      const { rows: updated } = await pgClient.query(
        'SELECT day::TEXT, deals_tested, deals_advertising_http FROM indexer_query_stats'
      )
      assert.deepStrictEqual(updated, [
        { day: today, deals_tested: 3 + 4, deals_advertising_http: 1 + 1 }
      ])
    })
  })

  describe('daily_deals', () => {
    it('creates or updates the row for today', async () => {
      /** @type {Measurement[]} */
      const honestMeasurements = [
        { ...VALID_MEASUREMENT },
        // HTTP_NOT_ADVERTISED means the deal is indexed
        { ...VALID_MEASUREMENT, cid: 'bafy2', indexerResult: 'HTTP_NOT_ADVERTISED', retrievalResult: 'HTTP_502' },
        { ...VALID_MEASUREMENT, cid: 'bafy3', indexerResult: 'ERROR_404', retrievalResult: 'IPNI_ERROR_404' },
        { ...VALID_MEASUREMENT, cid: 'bafy4', status_code: 502, retrievalResult: 'HTTP_502' }
      ]
      const allMeasurements = honestMeasurements
      let committees = buildEvaluatedCommitteesFromMeasurements(honestMeasurements)

      await updatePublicStats({
        createPgClient,
        committees,
        allMeasurements,
        findDealClients: (_minerId, _cid) => ['f0client'],
        findDealAllocators: (_minerId, _cid) => ['f0allocator']
      })

      const { rows: created } = await pgClient.query(
        'SELECT day::TEXT, tested, indexed, retrievable FROM daily_deals'
      )
      assert.deepStrictEqual(created, [
        { day: today, tested: 4, indexed: 3, retrievable: 1 }
      ])

      // Notice: this measurement is for the same task as honestMeasurements[0], therefore it's
      // effectively ignored as the other measurement was successful.
      honestMeasurements.push({ ...VALID_MEASUREMENT, status_code: 502 })
      // These are measurements for a new task.
      honestMeasurements.push({ ...VALID_MEASUREMENT, cid: 'bafy5', indexerResult: 'OK', status_code: 502, retrievalResult: 'HTTP_502' })
      honestMeasurements.push({ ...VALID_MEASUREMENT, cid: 'bafy5', indexerResult: 'ERROR_FETCH', retrievalResult: 'IPNI_ERROR_FETCH' })
      committees = buildEvaluatedCommitteesFromMeasurements(honestMeasurements)

      await updatePublicStats({
        createPgClient,
        committees,
        allMeasurements,
        findDealClients: (_minerId, _cid) => ['f0client'],
        findDealAllocators: (_minerId, _cid) => ['f0allocator']
      })

      const { rows: updated } = await pgClient.query(
        'SELECT day::TEXT, miner_id, tested, indexed, retrievable FROM daily_deals'
      )
      assert.deepStrictEqual(updated, [{
        day: today,
        miner_id: VALID_MEASUREMENT.minerId,
        tested: 2 * 4 + 1 /* added bafy5 */,
        indexed: 2 * 3 + 1 /* bafy5 is indexed */,
        retrievable: 2 * 1 + 0 /* bafy5 not retrievable */
      }])
    })

    it('records client_id by creating one row per client', async () => {
      const findDealClients = (_minerId, _cid) => ['f0clientA', 'f0clientB']
      const findDealAllocators = (_minerId, _cid) => ['f0allocator']

      // Create new records
      {
        /** @type {Measurement[]} */
        const honestMeasurements = [
          { ...VALID_MEASUREMENT }

        ]
        const allMeasurements = honestMeasurements
        const committees = buildEvaluatedCommitteesFromMeasurements(honestMeasurements)

        await updatePublicStats({
          createPgClient,
          committees,
          allMeasurements,
          findDealClients,
          findDealAllocators
        })

        const { rows: created } = await pgClient.query(
          'SELECT day::TEXT, miner_id, client_id, tested, indexed, retrievable FROM daily_deals'
        )
        assert.deepStrictEqual(created, [
          { day: today, miner_id: VALID_MEASUREMENT.minerId, client_id: 'f0clientA', tested: 1, indexed: 1, retrievable: 1 },
          { day: today, miner_id: VALID_MEASUREMENT.minerId, client_id: 'f0clientB', tested: 1, indexed: 1, retrievable: 1 }
        ])
      }

      // Update existing records
      {
        /** @type {Measurement[]} */
        const honestMeasurements = [
          { ...VALID_MEASUREMENT, cid: 'bafy5', indexerResult: 'ERROR_FETCH', retrievalResult: 'IPNI_ERROR_FETCH' }
        ]
        const allMeasurements = honestMeasurements
        const committees = buildEvaluatedCommitteesFromMeasurements(honestMeasurements)

        await updatePublicStats({
          createPgClient,
          committees,
          allMeasurements,
          findDealClients,
          findDealAllocators
        })

        const { rows: updated } = await pgClient.query(
          'SELECT day::TEXT, miner_id, client_id, tested, indexed, retrievable FROM daily_deals'
        )
        assert.deepStrictEqual(updated, [
          { day: today, miner_id: VALID_MEASUREMENT.minerId, client_id: 'f0clientA', tested: 2, indexed: 1, retrievable: 1 },
          { day: today, miner_id: VALID_MEASUREMENT.minerId, client_id: 'f0clientB', tested: 2, indexed: 1, retrievable: 1 }
        ])
      }
    })

    it('records index_majority_found, indexed, indexed_http', async () => {
      const findDealClients = (_minerId, _cid) => ['f0client']
      const findDealAllocators = (_minerId, _cid) => ['f0allocator']
      // Create new record(s)
      {
        /** @type {Measurement[]} */
        const honestMeasurements = [
          // a majority is found, indexerResult = OK
          { ...VALID_MEASUREMENT, indexerResult: 'OK' },
          { ...VALID_MEASUREMENT, indexerResult: 'OK' },
          { ...VALID_MEASUREMENT, indexerResult: 'ERROR_404' },

          // a majority is found, indexerResult = HTTP_NOT_ADVERTISED
          { ...VALID_MEASUREMENT, cid: 'bafy2', indexerResult: 'HTTP_NOT_ADVERTISED' },
          { ...VALID_MEASUREMENT, cid: 'bafy2', indexerResult: 'HTTP_NOT_ADVERTISED' },
          { ...VALID_MEASUREMENT, cid: 'bafy2', indexerResult: 'ERROR_404' },

          // a majority is found, indexerResult = ERROR_404
          { ...VALID_MEASUREMENT, cid: 'bafy3', indexerResult: 'OK' },
          { ...VALID_MEASUREMENT, cid: 'bafy3', indexerResult: 'ERROR_404' },
          { ...VALID_MEASUREMENT, cid: 'bafy3', indexerResult: 'ERROR_404' },

          // committee is too small
          { ...VALID_MEASUREMENT, cid: 'bafy4', indexerResult: 'OK' },

          // no majority was found
          { ...VALID_MEASUREMENT, cid: 'bafy5', indexerResult: 'OK' },
          { ...VALID_MEASUREMENT, cid: 'bafy5', indexerResult: 'NO_VALID_ADVERTISEMENT' },
          { ...VALID_MEASUREMENT, cid: 'bafy5', indexerResult: 'ERROR_404' }
        ]
        honestMeasurements.forEach(m => { m.taskingEvaluation = 'OK' })
        const allMeasurements = honestMeasurements
        const committees = [...groupMeasurementsToCommittees(honestMeasurements).values()]
        committees.forEach(c => c.evaluate({ requiredCommitteeSize: 3 }))

        await updatePublicStats({
          createPgClient,
          committees,
          allMeasurements,
          findDealClients,
          findDealAllocators
        })

        const { rows: created } = await pgClient.query(
          'SELECT day::TEXT, tested, index_majority_found, indexed, indexed_http FROM daily_deals'
        )
        assert.deepStrictEqual(created, [
          { day: today, tested: 5, index_majority_found: 3, indexed: 2, indexed_http: 1 }
        ])
      }

      // Update existing record(s)
      {
        /** @type {Measurement[]} */
        const honestMeasurements = [
          // a majority is found, indexerResult = OK
          { ...VALID_MEASUREMENT, indexerResult: 'OK' },

          // a majority is found, indexerResult = HTTP_NOT_ADVERTISED
          { ...VALID_MEASUREMENT, cid: 'bafy2', indexerResult: 'HTTP_NOT_ADVERTISED' },

          // a majority is found, indexerResult = ERROR_404
          { ...VALID_MEASUREMENT, cid: 'bafy3', indexerResult: 'ERROR_404' },

          // committee is too small
          { ...VALID_MEASUREMENT, cid: 'bafy4', indexerResult: 'OK' }
        ]
        const allMeasurements = honestMeasurements
        const committees = buildEvaluatedCommitteesFromMeasurements(honestMeasurements)
        Object.assign(committees.find(c => c.retrievalTask.cid === 'bafy4').decision, {
          indexMajorityFound: false,
          indexerResult: 'COMMITTEE_TOO_SMALL'
        })

        await updatePublicStats({
          createPgClient,
          committees,
          allMeasurements,
          findDealClients,
          findDealAllocators
        })

        const { rows: created } = await pgClient.query(
          'SELECT day::TEXT, tested, index_majority_found, indexed, indexed_http FROM daily_deals'
        )
        assert.deepStrictEqual(created, [
          { day: today, tested: 5 + 4, index_majority_found: 3 + 3, indexed: 2 + 2, indexed_http: 1 + 1 }
        ])
      }
    })

    it('records retrieval_majority_found, retrievable', async () => {
      const findDealClients = (_minerId, _cid) => ['f0client']
      const findDealAllocators = (_minerId, _cid) => ['f0allocator']
      // Create new record(s)
      {
        /** @type {Measurement[]} */
        const honestMeasurements = [
          // a majority is found, retrievalResult = OK
          { ...VALID_MEASUREMENT, retrievalResult: 'OK' },
          { ...VALID_MEASUREMENT, retrievalResult: 'OK' },
          { ...VALID_MEASUREMENT, retrievalResult: 'HTTP_404' },

          // a majority is found, retrievalResult = ERROR_404
          { ...VALID_MEASUREMENT, cid: 'bafy3', retrievalResult: 'OK' },
          { ...VALID_MEASUREMENT, cid: 'bafy3', retrievalResult: 'HTTP_404' },
          { ...VALID_MEASUREMENT, cid: 'bafy3', retrievalResult: 'HTTP_404' },

          // committee is too small
          { ...VALID_MEASUREMENT, cid: 'bafy4', retrievalResult: 'OK' },

          // no majority was found
          { ...VALID_MEASUREMENT, cid: 'bafy5', retrievalResult: 'OK' },
          { ...VALID_MEASUREMENT, cid: 'bafy5', retrievalResult: 'HTTP_404' },
          { ...VALID_MEASUREMENT, cid: 'bafy5', retrievalResult: 'HTTP_502' }
        ]
        honestMeasurements.forEach(m => { m.taskingEvaluation = 'OK' })
        const allMeasurements = honestMeasurements
        const committees = [...groupMeasurementsToCommittees(honestMeasurements).values()]
        committees.forEach(c => c.evaluate({ requiredCommitteeSize: 3 }))

        await updatePublicStats({
          createPgClient,
          committees,
          allMeasurements,
          findDealClients,
          findDealAllocators
        })

        const { rows: created } = await pgClient.query(
          'SELECT day::TEXT, tested, retrieval_majority_found, retrievable FROM daily_deals'
        )
        assert.deepStrictEqual(created, [
          { day: today, tested: 4, retrieval_majority_found: 2, retrievable: 1 }
        ])
      }

      // Update existing record(s)
      {
        /** @type {Measurement[]} */
        const honestMeasurements = [
          // a majority is found, retrievalResult = OK
          { ...VALID_MEASUREMENT, retrievalResult: 'OK', consensusEvaluation: 'MAJORITY_RESULT' },

          // a majority is found, retrievalResult = ERROR_404
          { ...VALID_MEASUREMENT, cid: 'bafy3', retrievalResult: 'HTTP_404', consensusEvaluation: 'MAJORITY_RESULT' },

          // committee is too small
          { ...VALID_MEASUREMENT, cid: 'bafy4', retrievalResult: 'OK', consensusEvaluation: 'COMMITTEE_TOO_SMALL' }
        ]
        const allMeasurements = honestMeasurements
        const committees = buildEvaluatedCommitteesFromMeasurements(honestMeasurements)
        Object.assign(committees.find(c => c.retrievalTask.cid === 'bafy4').decision, {
          retrievalMajorityFound: false,
          retrievalResult: 'COMMITTEE_TOO_SMALL'
        })

        await updatePublicStats({
          createPgClient,
          committees,
          allMeasurements,
          findDealClients,
          findDealAllocators
        })
        const { rows: created } = await pgClient.query(
          'SELECT day::TEXT, tested, retrieval_majority_found, retrievable FROM daily_deals'
        )
        assert.deepStrictEqual(created, [
          { day: today, tested: 4 + 3, retrieval_majority_found: 2 + 2, retrievable: 1 + 1 }
        ])
      }
    })

    it('handles a task not linked to any clients', async () => {
      const findDealClients = (_minerId, _cid) => undefined
      const findDealAllocators = (_minerId, _cid) => ['f0allocator']

      /** @type {Measurement[]} */
      const honestMeasurements = [
        { ...VALID_MEASUREMENT }

      ]
      const allMeasurements = honestMeasurements
      const committees = buildEvaluatedCommitteesFromMeasurements(honestMeasurements)

      await updatePublicStats({
        createPgClient,
        committees,
        allMeasurements,
        findDealClients,
        findDealAllocators
      })

      const { rows: created } = await pgClient.query(
        'SELECT day::TEXT, miner_id, client_id FROM daily_deals'
      )
      assert.deepStrictEqual(created, [])
    })
  })

  describe('retrieval_times', () => {
    it('creates or updates rows for today', async () => {
      /** @type {Measurement[]} */
      let acceptedMeasurements = [
        givenTimeToFirstByte({ ...VALID_MEASUREMENT, cid: 'cidone', minerId: 'f1first', retrievalResult: 'OK' }, 1000),
        givenTimeToFirstByte({ ...VALID_MEASUREMENT, cid: 'cidone', minerId: 'f1first', retrievalResult: 'OK' }, 3000),
        givenTimeToFirstByte({ ...VALID_MEASUREMENT, cid: 'cidone', minerId: 'f1second', retrievalResult: 'OK' }, 2000),
        givenTimeToFirstByte({ ...VALID_MEASUREMENT, cid: 'cidone', minerId: 'f1second', retrievalResult: 'OK' }, 1000)
      ]

      /** @type {Measurement[]} */
      const rejectedMeasurements = [
        givenTimeToFirstByte({ ...VALID_MEASUREMENT, cid: 'cidone', minerId: 'f1first', retrievalResult: 'UNKNOWN_ERROR' }, 100),
        givenTimeToFirstByte({ ...VALID_MEASUREMENT, cid: 'cidone', minerId: 'f1first', retrievalResult: 'UNKNOWN_ERROR' }, 200),
        givenTimeToFirstByte({ ...VALID_MEASUREMENT, cid: 'cidone', minerId: 'f1first', retrievalResult: 'UNKNOWN_ERROR' }, 300),
        givenTimeToFirstByte({ ...VALID_MEASUREMENT, cid: 'cidone', minerId: 'f1second', retrievalResult: 'UNKNOWN_ERROR' }, 300),
        givenTimeToFirstByte({ ...VALID_MEASUREMENT, cid: 'cidone', minerId: 'f1second', retrievalResult: 'UNKNOWN_ERROR' }, 200),
        givenTimeToFirstByte({ ...VALID_MEASUREMENT, cid: 'cidone', minerId: 'f1second', retrievalResult: 'UNKNOWN_ERROR' }, 100)
      ]

      let allMeasurements = [...acceptedMeasurements, ...rejectedMeasurements]
      let committees = buildEvaluatedCommitteesFromMeasurements(acceptedMeasurements)

      await updatePublicStats({
        createPgClient,
        committees,
        allMeasurements,
        findDealClients: (_minerId, _cid) => ['f0client'],
        findDealAllocators: (_minerId, _cid) => ['f0allocator']
      })
      const { rows: created } = await pgClient.query(
        'SELECT day::TEXT, miner_id, ttfb_p50 FROM retrieval_timings ORDER BY miner_id'
      )
      assert.deepStrictEqual(created, [
        { day: today, miner_id: 'f1first', ttfb_p50: [2000] },
        { day: today, miner_id: 'f1second', ttfb_p50: [1500] }
      ])

      acceptedMeasurements = [
        givenTimeToFirstByte({ ...VALID_MEASUREMENT, cid: 'cidone', minerId: 'f1first', retrievalResult: 'OK' }, 3000),
        givenTimeToFirstByte({ ...VALID_MEASUREMENT, cid: 'cidone', minerId: 'f1first', retrievalResult: 'OK' }, 5000),
        givenTimeToFirstByte({ ...VALID_MEASUREMENT, cid: 'cidone', minerId: 'f1first', retrievalResult: 'OK' }, 1000)
      ]
      allMeasurements = [...acceptedMeasurements, ...rejectedMeasurements]
      committees = buildEvaluatedCommitteesFromMeasurements(acceptedMeasurements)
      await updatePublicStats({
        createPgClient,
        committees,
        allMeasurements,
        findDealClients: (_minerId, _cid) => ['f0client'],
        findDealAllocators: (_minerId, _cid) => ['f0allocator']
      })
      const { rows: updated } = await pgClient.query(
        'SELECT day::TEXT, miner_id, ttfb_p50 FROM retrieval_timings ORDER BY miner_id'
      )
      assert.deepStrictEqual(updated, [
        { day: today, miner_id: 'f1first', ttfb_p50: [2000, 3000] },
        { day: today, miner_id: 'f1second', ttfb_p50: [1500] }
      ])
    })
  })

  describe('updateDailyClientRetrievalStats', () => {
    it('aggregates per client stats', async () => {
      // We create multiple measurements with different miner ids and thus key ids
      // We also want to test multiple different number of measurements for a given combination of (cid,minerId)
      /** @type {Measurement[]} */
      const allMeasurements = [
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f0test' },
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f0test' },

        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f1test' },
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f1test' },
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f1test' }
      ]

      // Separate the measurements into two groups, one for the client f0 and the other for f1
      const findDealClients = (minerId, _cid) => {
        switch (minerId) {
          case 'f0test':
            return ['f0client']
          case 'f1test':
            return ['f1client']
          default:
            throw new Error('Unexpected minerId')
        }
      }
      const committees = buildEvaluatedCommitteesFromMeasurements(allMeasurements)
      const { rows: created } = await pgClient.query(
        'SELECT * FROM daily_client_retrieval_stats'
      )
      assert.deepStrictEqual(created, [])
      await updateDailyClientRetrievalStats(
        pgClient,
        committees,
        findDealClients
      )
      const { rows } = await pgClient.query(
          `SELECT
               day::TEXT,
               client_id,
               total,
               successful,
               successful_http
            FROM daily_client_retrieval_stats
            ORDER BY client_id`)

      assert.deepStrictEqual(rows, [
        { day: today, client_id: 'f0client', total: 2, successful: 2, successful_http: 2 },
        { day: today, client_id: 'f1client', total: 3, successful: 3, successful_http: 3 }
      ])
    })
    it('aggregates overlapping per client stats', async () => {
      /** @type {Measurement[]} */
      const allMeasurements = [
        // minerId f01test stores deals for both client f0 and f1
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f01test' },
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f01test' },

        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f1test' },
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f1test' },
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f1test' }
      ]

      // Separate the measurements into two groups, one for the client f0 and f1 and the other only for f1
      const findDealClients = (minerId, cid) => {
        // Check that the CID is passed correctly
        assert.strictEqual(cid, VALID_MEASUREMENT.cid)
        switch (minerId) {
          case 'f01test':
            return ['f0client', 'f1client']
          case 'f1test':
            return ['f1client']
          default:
            throw new Error('Unexpected minerId')
        }
      }
      const committees = buildEvaluatedCommitteesFromMeasurements(allMeasurements)
      await updateDailyClientRetrievalStats(
        pgClient,
        committees,
        findDealClients
      )
      const { rows } = await pgClient.query(
          `SELECT
               day::TEXT,
               client_id,
               total,
               successful,
               successful_http
            FROM daily_client_retrieval_stats
            ORDER BY client_id`)

      assert.deepStrictEqual(rows, [
        { day: today, client_id: 'f0client', total: 2, successful: 2, successful_http: 2 },
        { day: today, client_id: 'f1client', total: 5, successful: 5, successful_http: 5 }
      ])
    })
    it('skips clients that have not match for a given miner_id,piece_cid combination', async () => {
      /** @type {Measurement[]} */
      const allMeasurements = [
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f0test' },
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f1test' }
      ]

      const findDealClients = (_minerId, _cid) => undefined

      const committees = buildEvaluatedCommitteesFromMeasurements(allMeasurements)
      // We test the warning output by changing the default warn function
      const originalWarn = console.warn
      let warnCalled = false
      console.warn = function (message) {
        warnCalled = true
        assert(message.includes('no deal clients found. Excluding the task from daily per-client stats.'))
      }
      await updateDailyClientRetrievalStats(
        pgClient,
        committees,
        findDealClients
      )
      // Reset warning function
      console.warn = originalWarn

      // Warning function should have been called
      assert(warnCalled)
      const { rows: stats } = await pgClient.query(
        'SELECT day::TEXT,client_id,total,successful,successful_http FROM daily_client_retrieval_stats'
      )
      assert.strictEqual(stats.length, 0, `No stats should be recorded: ${JSON.stringify(stats)}`)
    })
    it('updates existing clients rsr scores on conflicting client_id,day pairs', async () => {
      /** @type {Measurement[]} */
      const allMeasurements = [
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f0test' }
      ]

      const findDealClients = (_minerId, _cid) => ['f0client']

      let committees = buildEvaluatedCommitteesFromMeasurements(allMeasurements)
      await updateDailyClientRetrievalStats(
        pgClient,
        committees,
        findDealClients
      )
      let { rows: stats } = await pgClient.query(
        'SELECT day::TEXT,client_id,total,successful,successful_http FROM daily_client_retrieval_stats'
      )
      assert.strictEqual(stats.length, 1)
      assert.deepStrictEqual(stats, [
        { day: today, client_id: 'f0client', total: 1, successful: 1, successful_http: 1 }
      ])

      // We now create another round of measurements for the same client and day
      allMeasurements.push(
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f0test' },
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f0test' }
      )
      committees = buildEvaluatedCommitteesFromMeasurements(allMeasurements)
      await updateDailyClientRetrievalStats(
        pgClient,
        committees,
        findDealClients
      )
      stats = (await pgClient.query(
        'SELECT day::TEXT,client_id,total,successful,successful_http FROM daily_client_retrieval_stats'
      )).rows
      assert.strictEqual(stats.length, 1)
      assert.deepStrictEqual(stats, [
        { day: today, client_id: 'f0client', total: 4, successful: 4, successful_http: 4 }
      ])
    })
    it('correctly handles different protocols and retrieval results', async () => {
      // We create multiple measurements that have different combinations of protocols and retrieval results
      /** @type {Measurement[]} */
      const allMeasurements = [
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f0test' },
        { ...VALID_MEASUREMENT, protocol: 'NOT_HTTP', minerId: 'f0test' },
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f0test', retrievalResult: 'HTTP_404' }
      ]
      const findDealClients = (_minerId, _cid) => ['f0client']

      const committees = buildEvaluatedCommitteesFromMeasurements(allMeasurements)
      await updateDailyClientRetrievalStats(
        pgClient,
        committees,
        findDealClients
      )
      const { rows: stats } = await pgClient.query(
        'SELECT day::TEXT,client_id,total,successful,successful_http FROM daily_client_retrieval_stats'
      )
      // Http protocols should be counted as successful and successful_http while other protocols should only be counted as successful
      // Measurements with retrievalResult HTTP_404 should only be counted to the total number of results
      assert.deepStrictEqual(stats, [
        { day: today, client_id: 'f0client', total: 3, successful: 2, successful_http: 1 }
      ])
    })

    it('excludes IPNI 5xx results', async () => {
      /** @type {Measurement[]} */
      const allMeasurements = [
        { ...VALID_MEASUREMENT, retrievalResult: 'OK' },
        { ...VALID_MEASUREMENT, retrievalResult: 'OK' },
        { ...VALID_MEASUREMENT, retrievalResult: 'OK' },
        { ...VALID_MEASUREMENT, retrievalResult: 'IPNI_ERROR_504' },
        { ...VALID_MEASUREMENT, retrievalResult: 'TIMEOUT' }
      ]
      for (const m of allMeasurements) m.taskingEvaluation = 'OK'
      const committees = [...groupMeasurementsToCommittees(allMeasurements).values()]
      assert.strictEqual(committees.length, 1)
      committees[0].evaluate({ requiredCommitteeSize: 3 })
      assert.deepStrictEqual(allMeasurements.map(m => m.consensusEvaluation), [
        'MAJORITY_RESULT',
        'MAJORITY_RESULT',
        'MAJORITY_RESULT',
        'MINORITY_RESULT',
        'MINORITY_RESULT'
      ])

      await updatePublicStats({
        createPgClient,
        committees,
        allMeasurements,
        findDealClients: (_minerId, _cid) => ['f0client'],
        findDealAllocators: (_minerId, _cid) => ['f0allocator']
      })

      const { rows: created } = await pgClient.query(
        'SELECT day::TEXT, total, successful FROM retrieval_stats'
      )
      assert.deepStrictEqual(created, [
        { day: today, total: 4, successful: 3 }
      ])

      const { rows: stats } = await pgClient.query(
        'SELECT day::TEXT, client_id, total, successful FROM daily_client_retrieval_stats'
      )
      assert.deepStrictEqual(stats, [
        // 5 measurements were recorded in total, but only 4 are counted
        { day: today, client_id: 'f0client', total: 4, successful: 3 }
      ])
    })
  })

  describe('updateDailyAllocatorRetrievalStats', () => {
    it('aggregates per allocator stats', async () => {
      // We create multiple measurements with different miner ids and thus key ids
      // We also want to test multiple different number of measurements for a given combination of (cid,minerId)
      /** @type {Measurement[]} */
      const allMeasurements = [
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f0test' },
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f0test' },

        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f1test' },
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f1test' },
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f1test' }
      ]

      // Separate the measurements into two groups, one for the allocator f0 and the other for f1
      const findDealAllocators = (minerId, _cid) => {
        switch (minerId) {
          case 'f0test':
            return ['f0allocator']
          case 'f1test':
            return ['f1allocator']
          default:
            throw new Error('Unexpected minerId')
        }
      }
      const committees = buildEvaluatedCommitteesFromMeasurements(allMeasurements)
      const { rows: created } = await pgClient.query(
        'SELECT * FROM daily_allocator_retrieval_stats'
      )
      assert.deepStrictEqual(created, [])
      await updateDailyAllocatorRetrievalStats(
        pgClient,
        committees,
        findDealAllocators
      )
      const { rows } = await pgClient.query(
            `SELECT
                 day::TEXT,
                 allocator_id,
                 total,
                 successful,
                 successful_http
              FROM daily_allocator_retrieval_stats
              ORDER BY allocator_id`)

      assert.deepStrictEqual(rows, [
        { day: today, allocator_id: 'f0allocator', total: 2, successful: 2, successful_http: 2 },
        { day: today, allocator_id: 'f1allocator', total: 3, successful: 3, successful_http: 3 }
      ])
    })
    it('aggregates overlapping per allocator stats', async () => {
      /** @type {Measurement[]} */
      const allMeasurements = [
        // minerId f01test stores deals for both allocator f0 and f1
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f01test' },
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f01test' },

        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f1test' },
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f1test' },
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f1test' }
      ]

      // Separate the measurements into two groups, one for the allocator f0 and f1 and the other only for f1
      const findDealAllocators = (minerId, cid) => {
        // Check that the CID is passed correctly
        assert.strictEqual(cid, VALID_MEASUREMENT.cid)
        switch (minerId) {
          case 'f01test':
            return ['f0allocator', 'f1allocator']
          case 'f1test':
            return ['f1allocator']
          default:
            throw new Error('Unexpected minerId')
        }
      }
      const committees = buildEvaluatedCommitteesFromMeasurements(allMeasurements)
      await updateDailyAllocatorRetrievalStats(
        pgClient,
        committees,
        findDealAllocators
      )
      const { rows } = await pgClient.query(
            `SELECT
                 day::TEXT,
                 allocator_id,
                 total,
                 successful,
                 successful_http
              FROM daily_allocator_retrieval_stats
              ORDER BY allocator_id`)

      assert.deepStrictEqual(rows, [
        { day: today, allocator_id: 'f0allocator', total: 2, successful: 2, successful_http: 2 },
        { day: today, allocator_id: 'f1allocator', total: 5, successful: 5, successful_http: 5 }
      ])
    })
    it('skips allocators that have no match for a given miner_id,piece_cid combination', async () => {
      /** @type {Measurement[]} */
      const allMeasurements = [
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f0test' },
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f1test' }
      ]

      const findDealAllocators = (_minerId, _cid) => undefined

      const committees = buildEvaluatedCommitteesFromMeasurements(allMeasurements)
      // We test the warning output by changing the default warn function
      const originalWarn = console.warn
      let warnCalled = false
      console.warn = function (message) {
        warnCalled = true
        assert(message.includes('no deal allocators found. Excluding the task from daily per-allocator stats.'))
      }
      await updateDailyAllocatorRetrievalStats(
        pgClient,
        committees,
        findDealAllocators
      )
      // Reset warning function
      console.warn = originalWarn

      // Warning function should have been called
      assert(warnCalled)
      const { rows: stats } = await pgClient.query(
        'SELECT day::TEXT,allocator_id,total,successful,successful_http FROM daily_allocator_retrieval_stats'
      )
      assert.strictEqual(stats.length, 0, `No stats should be recorded: ${JSON.stringify(stats)}`)
    })
    it('updates existing allocators rsr scores on conflicting allocator_id,day pairs', async () => {
      /** @type {Measurement[]} */
      const allMeasurements = [
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f0test' }
      ]

      const findDealAllocators = (_minerId, _cid) => ['f0allocator']

      let committees = buildEvaluatedCommitteesFromMeasurements(allMeasurements)
      await updateDailyAllocatorRetrievalStats(
        pgClient,
        committees,
        findDealAllocators
      )
      let { rows: stats } = await pgClient.query(
        'SELECT day::TEXT,allocator_id,total,successful,successful_http FROM daily_allocator_retrieval_stats'
      )
      assert.strictEqual(stats.length, 1)
      assert.deepStrictEqual(stats, [
        { day: today, allocator_id: 'f0allocator', total: 1, successful: 1, successful_http: 1 }
      ])

      // We now create another round of measurements for the same allocator and day
      allMeasurements.push(
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f0test' },
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f0test' }
      )
      committees = buildEvaluatedCommitteesFromMeasurements(allMeasurements)
      await updateDailyAllocatorRetrievalStats(
        pgClient,
        committees,
        findDealAllocators
      )
      stats = (await pgClient.query(
        'SELECT day::TEXT,allocator_id,total,successful,successful_http FROM daily_allocator_retrieval_stats'
      )).rows
      assert.strictEqual(stats.length, 1)
      assert.deepStrictEqual(stats, [
        { day: today, allocator_id: 'f0allocator', total: 4, successful: 4, successful_http: 4 }
      ])
    })
    it('correctly handles different protocols and retrieval results', async () => {
      // We create multiple measurements that have different combinations of protocols and retrieval results
      /** @type {Measurement[]} */
      const allMeasurements = [
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f0test' },
        { ...VALID_MEASUREMENT, protocol: 'NOT_HTTP', minerId: 'f0test' },
        { ...VALID_MEASUREMENT, protocol: 'http', minerId: 'f0test', retrievalResult: 'HTTP_404' }
      ]
      const findDealAllocators = (_minerId, _cid) => ['f0allocator']

      const committees = buildEvaluatedCommitteesFromMeasurements(allMeasurements)
      await updateDailyAllocatorRetrievalStats(
        pgClient,
        committees,
        findDealAllocators
      )
      const { rows: stats } = await pgClient.query(
        'SELECT day::TEXT,allocator_id,total,successful,successful_http FROM daily_allocator_retrieval_stats'
      )
      // Http protocols should be counted as successful and successful_http while other protocols should only be counted as successful
      // Measurements with retrievalResult HTTP_404 should only be counted to the total number of results
      assert.deepStrictEqual(stats, [
        { day: today, allocator_id: 'f0allocator', total: 3, successful: 2, successful_http: 1 }
      ])
    })

    it('excludes IPNI 5xx results', async () => {
      /** @type {Measurement[]} */
      const allMeasurements = [
        { ...VALID_MEASUREMENT, retrievalResult: 'OK' },
        { ...VALID_MEASUREMENT, retrievalResult: 'OK' },
        { ...VALID_MEASUREMENT, retrievalResult: 'OK' },
        { ...VALID_MEASUREMENT, retrievalResult: 'IPNI_ERROR_504' },
        { ...VALID_MEASUREMENT, retrievalResult: 'TIMEOUT' }
      ]
      for (const m of allMeasurements) m.taskingEvaluation = 'OK'
      const committees = [...groupMeasurementsToCommittees(allMeasurements).values()]
      assert.strictEqual(committees.length, 1)
      committees[0].evaluate({ requiredCommitteeSize: 3 })
      assert.deepStrictEqual(allMeasurements.map(m => m.consensusEvaluation), [
        'MAJORITY_RESULT',
        'MAJORITY_RESULT',
        'MAJORITY_RESULT',
        'MINORITY_RESULT',
        'MINORITY_RESULT'
      ])

      await updatePublicStats({
        createPgClient,
        committees,
        allMeasurements,
        findDealClients: (_minerId, _cid) => ['f0client'],
        findDealAllocators: (_minerId, _cid) => ['f0allocator']
      })

      const { rows: created } = await pgClient.query(
        'SELECT day::TEXT, total, successful FROM retrieval_stats'
      )
      assert.deepStrictEqual(created, [
        { day: today, total: 4, successful: 3 }
      ])

      const { rows: stats } = await pgClient.query(
        'SELECT day::TEXT, allocator_id, total, successful FROM daily_allocator_retrieval_stats'
      )
      assert.deepStrictEqual(stats, [
        // 5 measurements were recorded in total, but only 4 are counted
        { day: today, allocator_id: 'f0allocator', total: 4, successful: 3 }
      ])
    })
  })

  const getCurrentDate = async () => {
    const { rows: [{ today }] } = await pgClient.query('SELECT now()::DATE::TEXT as today')
    return today
  }

  /**
   *
   * @param {Measurement} measurement
   * @param {number} timeToFirstByte  Time in milliseconds
   * @returns
   */
  function givenTimeToFirstByte (measurement, timeToFirstByte) {
    measurement.first_byte_at = measurement.start_at + timeToFirstByte
    return measurement
  }
})
