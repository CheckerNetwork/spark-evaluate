import assert from 'node:assert'
import pg from 'pg'

import { DATABASE_URL } from '../lib/config.js'
import { migrateWithPgClient } from '../lib/migrate.js'
import { buildEvaluatedCommitteesFromMeasurements, VALID_MEASUREMENT } from './helpers/test-data.js'
import { updatePublicStats } from '../lib/public-stats.js'
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
        findDealClients: (_minerId, _cid) => ['f0client']
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
        findDealClients: (_minerId, _cid) => ['f0client']
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
        { ...VALID_MEASUREMENT, protocol: 'http', retrievalResult: 'OK' },
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
        findDealClients: (_minerId, _cid) => ['f0client']
      })

      const { rows: created } = await pgClient.query(
        'SELECT day::TEXT, total, successful, successful_http FROM retrieval_stats'
      )
      assert.deepStrictEqual(created, [
        { day: today, total: 4, successful: 2, successful_http: 1 }
      ])

      // Let's add another successful http retrieval to make sure the updating process works as expected
      honestMeasurements.push({ ...VALID_MEASUREMENT, retrievalResult: 'OK', protocol: 'http' })
      committees = buildEvaluatedCommitteesFromMeasurements(honestMeasurements)
      await updatePublicStats({
        createPgClient,
        committees,
        allMeasurements,
        findDealClients: (_minerId, _cid) => ['f0client']
      })

      const { rows: updated } = await pgClient.query(
        'SELECT day::TEXT, total, successful, successful_http FROM retrieval_stats'
      )
      assert.deepStrictEqual(updated, [
        { day: today, total: 4 + 5, successful: 2 + 3, successful_http: 1 + 2 }
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
        findDealClients: (_minerId, _cid) => ['f0client']
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
        findDealClients: (_minerId, _cid) => ['f0client']
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
        findDealClients: (_minerId, _cid) => ['f0client']
      })

      const { rows: created } = await pgClient.query(
        'SELECT day::TEXT, total, successful, successful_http FROM retrieval_stats'
      )
      assert.deepStrictEqual(created, [
        { day: today, total: 3, successful: 2, successful_http: 0 }
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
        findDealClients: (_minerId, _cid) => ['f0client']
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
        findDealClients: (_minerId, _cid) => ['f0client']
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
        findDealClients: (_minerId, _cid) => ['f0client']
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
        findDealClients: (_minerId, _cid) => ['f0client']
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
          findDealClients
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
          findDealClients
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
          findDealClients
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
          findDealClients
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
          findDealClients
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
          findDealClients
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
        findDealClients
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
        findDealClients: (_minerId, _cid) => ['f0client']
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
        findDealClients: (_minerId, _cid) => ['f0client']
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
