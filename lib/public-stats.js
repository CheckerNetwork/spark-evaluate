import * as Sentry from '@sentry/node'
import createDebug from 'debug'
import * as providerRetrievalResultStats from './provider-retrieval-result-stats.js'
import { updatePlatformStats } from './platform-stats.js'
import { getTaskId, getValueAtPercentile } from './retrieval-stats.js'

/** @import pg from 'pg' */
/** @import { Committee } from './committee.js' */

const debug = createDebug('spark:public-stats')

/**
 * @param {object} args
 * @param {import('./typings.js').CreatePgClient} args.createPgClient
 * @param {Iterable<Committee>} args.committees
 * @param {import('./preprocess.js').Measurement[]} args.allMeasurements
 * @param {(minerId: string, cid: string) => (string[] | undefined)} args.findDealClients
 */
export const updatePublicStats = async ({ createPgClient, committees, allMeasurements, findDealClients }) => {
  const stats = providerRetrievalResultStats.build(committees)
  const pgClient = await createPgClient()
  try {
    for (const [minerId, retrievalResultStats] of stats.entries()) {
      await updateRetrievalStats(pgClient, minerId, retrievalResultStats)
    }
    await updateIndexerQueryStats(pgClient, committees)
    await updateDailyDealsStats(pgClient, committees, findDealClients)
    await updatePlatformStats(pgClient, allMeasurements)
    await updateRetrievalTimings(pgClient, committees)
  } finally {
    await pgClient.end()
  }
}

/**
 * @param {pg.Client} pgClient
 * @param {string} minerId
 * @param {object} stats
 * @param {number} stats.total
 * @param {number} stats.successful
 * @param {number} stats.successfulHttp
 */
const updateRetrievalStats = async (pgClient, minerId, { total, successful, successfulHttp }) => {
  debug('Updating public retrieval stats for miner %s: total += %s successful += %s, successful_http += %s', minerId, total, successful, successfulHttp)
  await pgClient.query(`
    INSERT INTO retrieval_stats
      (day, miner_id, total, successful, successful_http)
    VALUES
      (now(), $1, $2, $3, $4)
    ON CONFLICT(day, miner_id) DO UPDATE SET
      total = retrieval_stats.total + $2,
      successful = retrieval_stats.successful + $3,
      successful_http = retrieval_stats.successful_http + $4
  `, [
    minerId,
    total,
    successful,
    successfulHttp
  ])
}

/**
 * @param {pg.Client} pgClient
 * @param {Iterable<Committee>} committees
 */
const updateIndexerQueryStats = async (pgClient, committees) => {
  /** @type {Set<string>} */
  const dealsWithHttpAdvertisement = new Set()
  /** @type {Set<string>} */
  const dealsWithIndexerResults = new Set()

  for (const c of committees) {
    const dealId = getTaskId(c.retrievalTask)
    const decision = c.decision
    if (!decision) continue
    if (decision.indexerResult) dealsWithIndexerResults.add(dealId)
    if (decision.indexerResult === 'OK') dealsWithHttpAdvertisement.add(dealId)
  }

  const tested = dealsWithIndexerResults.size
  const advertisingHttp = dealsWithHttpAdvertisement.size
  debug('Updating public stats - indexer queries: deals_tested += %s deals_advertising_http += %s', tested, advertisingHttp)
  await pgClient.query(`
    INSERT INTO indexer_query_stats
      (day, deals_tested, deals_advertising_http)
    VALUES
      (now(), $1, $2)
    ON CONFLICT(day) DO UPDATE SET
    deals_tested = indexer_query_stats.deals_tested + $1,
    deals_advertising_http = indexer_query_stats.deals_advertising_http + $2
  `, [
    tested,
    advertisingHttp
  ])
}

/**
 * @param {pg.Client} pgClient
 * @param {Iterable<Committee>} committees
 * @param {(minerId: string, cid: string) => (string[] | undefined)} findDealClients
 */
const updateDailyDealsStats = async (pgClient, committees, findDealClients) => {
  /** @type {Map<string, Map<string, {
   * tested: number;
   * index_majority_found: number;
   * retrieval_majority_found: number;
   * indexed:  number;
   * indexed_http: number;
   * retrievable: number;
   * }>>} */
  const minerClientDealStats = new Map()
  for (const c of committees) {
    const { minerId, cid } = c.retrievalTask
    const clients = findDealClients(minerId, cid)
    if (!clients || !clients.length) {
      console.warn(`Invalid retrieval task (${minerId}, ${cid}): no deal clients found. Excluding the task from daily per-deal stats.`)
      Sentry.captureException(new Error('Invalid retrieval task: no deal clients found.'), {
        extra: {
          minerId,
          cid
        }
      })
      continue
    }

    let clientDealStats = minerClientDealStats.get(minerId)
    if (!clientDealStats) {
      clientDealStats = new Map()
      minerClientDealStats.set(minerId, clientDealStats)
    }

    for (const clientId of clients) {
      let stats = clientDealStats.get(clientId)
      if (!stats) {
        stats = {
          tested: 0,
          index_majority_found: 0,
          retrieval_majority_found: 0,
          indexed: 0,
          indexed_http: 0,
          retrievable: 0
        }
        clientDealStats.set(clientId, stats)
      }

      stats.tested++

      const decision = c.decision
      if (!decision) continue

      if (decision.indexMajorityFound) {
        stats.index_majority_found++
      }

      if (decision.indexerResult === 'OK' || decision.indexerResult === 'HTTP_NOT_ADVERTISED') {
        stats.indexed++
      }

      if (decision.indexerResult === 'OK') {
        stats.indexed_http++
      }

      if (decision.retrievalMajorityFound) {
        stats.retrieval_majority_found++
      }

      if (decision.retrievalResult === 'OK') {
        stats.retrievable++
      }
    }
  }

  // Convert the nested map to an array for the query
  const flatStats = Array.from(minerClientDealStats.entries()).flatMap(
    ([minerId, clientDealStats]) => Array.from(clientDealStats.entries()).flatMap(
      ([clientId, stats]) => ({ minerId, clientId, ...stats })
    )
  )

  if (debug.enabled) {
    debug(
      'Updating public stats - daily deals: tested += %s index_majority_found += %s indexed += %s retrieval_majority_found += %s retrievable += %s',
      flatStats.reduce((sum, stat) => sum + stat.tested, 0),
      flatStats.reduce((sum, stat) => sum + stat.index_majority_found, 0),
      flatStats.reduce((sum, stat) => sum + stat.indexed, 0),
      flatStats.reduce((sum, stat) => sum + stat.retrieval_majority_found, 0),
      flatStats.reduce((sum, stat) => sum + stat.retrievable, 0)
    )
  }

  await pgClient.query(`
    INSERT INTO daily_deals (
      day,
      miner_id,
      client_id,
      tested,
      index_majority_found,
      indexed,
      indexed_http,
      retrieval_majority_found,
      retrievable
    ) VALUES (
      now(),
      unnest($1::text[]),
      unnest($2::text[]),
      unnest($3::int[]),
      unnest($4::int[]),
      unnest($5::int[]),
      unnest($6::int[]),
      unnest($7::int[]),
      unnest($8::int[])
    )
    ON CONFLICT(day, miner_id, client_id) DO UPDATE SET
      tested = daily_deals.tested + EXCLUDED.tested,
      index_majority_found = daily_deals.index_majority_found + EXCLUDED.index_majority_found,
      indexed = daily_deals.indexed + EXCLUDED.indexed,
      indexed_http = daily_deals.indexed_http + EXCLUDED.indexed_http,
      retrieval_majority_found = daily_deals.retrieval_majority_found + EXCLUDED.retrieval_majority_found,
      retrievable = daily_deals.retrievable + EXCLUDED.retrievable
  `, [
    flatStats.map(stat => stat.minerId),
    flatStats.map(stat => stat.clientId),
    flatStats.map(stat => stat.tested),
    flatStats.map(stat => stat.index_majority_found),
    flatStats.map(stat => stat.indexed),
    flatStats.map(stat => stat.indexed_http),
    flatStats.map(stat => stat.retrieval_majority_found),
    flatStats.map(stat => stat.retrievable)
  ])
}

/**
 * @param {pg.Client} pgClient
 * @param {Iterable<Committee>} committees
 */
const updateRetrievalTimings = async (pgClient, committees) => {
  /** @type {Map<string, number[]>} */
  const retrievalTimings = new Map()
  for (const c of committees) {
    if (!c.decision || !c.decision.retrievalMajorityFound || c.decision.retrievalResult !== 'OK') continue
    const { minerId } = c.retrievalTask
    const ttfbMeasurments = []
    for (const m of c.measurements) {
      if (m.taskingEvaluation !== 'OK' || m.consensusEvaluation !== 'MAJORITY_RESULT') continue
      // FIXME: assert first_byte_at and start_at during preprocessing
      // See https://github.com/filecoin-station/spark-evaluate/issues/447
      if (!m.first_byte_at || !m.start_at || m.start_at > m.first_byte_at) continue
      const ttfbMeasurment = m.first_byte_at - m.start_at
      if (isNaN(ttfbMeasurment)) continue
      ttfbMeasurments.push(ttfbMeasurment)
    }

    if (!retrievalTimings.has(minerId)) {
      retrievalTimings.set(minerId, [])
    }

    const ttfb = Math.ceil(getValueAtPercentile(ttfbMeasurments, 0.5))
    retrievalTimings.get(minerId).push(ttfb)
  }

  // eslint-disable-next-line camelcase
  const rows = Array.from(retrievalTimings.entries()).flatMap(([miner_id, ttfb_p50]) => ({ miner_id, ttfb_p50 }))

  await pgClient.query(`
    INSERT INTO retrieval_timings (day, miner_id, ttfb_p50)
    SELECT now(), miner_id, ttfb_p50 FROM jsonb_to_recordset($1::jsonb) AS t (miner_id text, ttfb_p50 int[])
    ON CONFLICT (day, miner_id)
    DO UPDATE SET
      ttfb_p50 = array_cat(
        retrieval_timings.ttfb_p50,
        EXCLUDED.ttfb_p50
      )
  `, [
    JSON.stringify(rows)
  ])
}
