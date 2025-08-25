import createDebug from 'debug'
import * as providerRetrievalResultStats from './provider-retrieval-result-stats.js'
import { updatePlatformStats } from './platform-stats.js'
import { getTaskId, getValueAtPercentile } from './retrieval-stats.js'

/** @import pg from 'pg' */
/** @import { Committee } from './committee.js' */
/** @import { Measurement } from './preprocess.js' */
const debug = createDebug('spark:public-stats')

/**
 * @param {object} args
 * @param {import('./typings.js').CreatePgClient} args.createPgClient
 * @param {Iterable<Committee>} args.committees
 * @param {import('./preprocess.js').Measurement[]} args.allMeasurements
 * @param {(minerId: string, cid: string) => (string[] | undefined)} args.findDealClients
 * @param {(minerId: string, cid: string) => (string[] | undefined)} args.findDealAllocators
 */
export const updatePublicStats = async ({ createPgClient, committees, allMeasurements, findDealClients, findDealAllocators }) => {
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
    await updateDailyClientRetrievalStats(pgClient, committees, findDealClients)
    await updateDailyAllocatorRetrievalStats(pgClient, committees, findDealAllocators)
    await updateDailyMinerDealsChecked(pgClient, committees)
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
 * @param {number} stats.successfulHttpHead
 */
const updateRetrievalStats = async (pgClient, minerId, { total, successful, successfulHttp, successfulHttpHead }) => {
  debug('Updating public retrieval stats for miner %s: total += %s successful += %s, successful_http += %s, successful_http_head += %s', minerId, total, successful, successfulHttp, successfulHttpHead)
  await pgClient.query(`
    INSERT INTO retrieval_stats
      (day, miner_id, total, successful, successful_http, successful_http_head)
    VALUES
      (now(), $1, $2, $3, $4, $5)
    ON CONFLICT(day, miner_id) DO UPDATE SET
      total = retrieval_stats.total + $2,
      successful = retrieval_stats.successful + $3,
      successful_http = retrieval_stats.successful_http + $4,
      successful_http_head = retrieval_stats.successful_http_head + $5
  `, [
    minerId,
    total,
    successful,
    successfulHttp,
    successfulHttpHead
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
    const ttfbMeasurements = []
    for (const m of c.measurements) {
      if (m.retrievalResult !== 'OK' || m.taskingEvaluation !== 'OK' || m.consensusEvaluation !== 'MAJORITY_RESULT') continue

      const ttfbMeasurment = m.first_byte_at - m.start_at
      ttfbMeasurements.push(ttfbMeasurment)
    }

    if (!retrievalTimings.has(minerId)) {
      retrievalTimings.set(minerId, [])
    }

    const ttfb = Math.ceil(getValueAtPercentile(ttfbMeasurements, 0.5))
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

/**
 * @param {pg.Client} pgClient
 * @param {Iterable<Committee>} committees
 * @param {(minerId: string, cid: string) => (string[] | undefined)} findDealClients
 */
export const updateDailyClientRetrievalStats = async (pgClient, committees, findDealClients) => {
  const stats = buildPerPartyStats(committees, findDealClients, 'client')

  await pgClient.query(`
    INSERT INTO daily_client_retrieval_stats (
      day,
      client_id,
      total,
      successful,
      successful_http
    ) VALUES (
      now(),
      unnest($1::text[]),
      unnest($2::int[]),
      unnest($3::int[]),
      unnest($4::int[])
    )
    ON CONFLICT (day, client_id) DO UPDATE SET
      total = daily_client_retrieval_stats.total + EXCLUDED.total,
      successful = daily_client_retrieval_stats.successful + EXCLUDED.successful,
      successful_http = daily_client_retrieval_stats.successful_http + EXCLUDED.successful_http
  `, [
    stats.map(stat => stat.partyId),
    stats.map(stat => stat.retrievalStats.total),
    stats.map(stat => stat.retrievalStats.successful),
    stats.map(stat => stat.retrievalStats.successfulHttp)
  ])
}

/**
 * @param {pg.Client} pgClient
 * @param {Iterable<Committee>} committees
 * @param {(minerId: string, cid: string) => (string[] | undefined)} findDealAllocators
 */
export const updateDailyAllocatorRetrievalStats = async (pgClient, committees, findDealAllocators) => {
  const stats = buildPerPartyStats(committees, findDealAllocators, 'allocator')

  await pgClient.query(`
    INSERT INTO daily_allocator_retrieval_stats (
      day,
      allocator_id,
      total,
      successful,
      successful_http
    ) VALUES (
      now(),
      unnest($1::text[]),
      unnest($2::int[]),
      unnest($3::int[]),
      unnest($4::int[])
    )
    ON CONFLICT (day, allocator_id) DO UPDATE SET
      total = daily_allocator_retrieval_stats.total + EXCLUDED.total,
      successful = daily_allocator_retrieval_stats.successful + EXCLUDED.successful,
      successful_http = daily_allocator_retrieval_stats.successful_http + EXCLUDED.successful_http
  `, [
    stats.map(stat => stat.partyId),
    stats.map(stat => stat.retrievalStats.total),
    stats.map(stat => stat.retrievalStats.successful),
    stats.map(stat => stat.retrievalStats.successfulHttp)
  ])
}
/**
* @param {Iterable<Committee>} committees
* @param {(minerId: string, cid: string) => (string[] | undefined)} perDealParty
* @param {string} partyName
* @returns {object[]}
*/
function buildPerPartyStats (committees, perDealParty, partyName) {
  /** @type {Map<string, { total, successful, successfulHttp }>} */
  const retrievalStatsPerParty = new Map()
  /** @type {Map<string, Measurement[]>} */
  const measurementsPerParty = new Map()
  for (const c of committees) {
    const { minerId, cid } = c.retrievalTask
    const parties = perDealParty(minerId, cid)
    if (!parties || !parties.length) {
      console.warn(`Invalid retrieval task (${minerId}, ${cid}): no deal ${partyName}s found. Excluding the task from daily per-${partyName} stats.`)
      continue
    }

    // Each of the parties in the given list has a deal with the miner and payload CID of this committee's retrieval task
    // Each of the measurements in the committee is associated with every single one of these parties and should be accounted for in the stats
    for (const partyId of parties) {
      if (!partyId) continue
      let value = measurementsPerParty.get(partyId)
      if (!value) {
        value = []
        measurementsPerParty.set(partyId, value)
      }
      value.push(...c.measurements)
    }
  }

  // We iterate over all the measurements that are associated with each party and update the stats accordingly
  for (const [partyId, measurements] of measurementsPerParty.entries()) {
    if (!retrievalStatsPerParty.has(partyId)) {
      retrievalStatsPerParty.set(partyId, { total: 0, successful: 0, successfulHttp: 0 })
    }
    const retrievalStats = retrievalStatsPerParty.get(partyId)
    for (const m of measurements) {
      // Ignore measurements reporting IPNI service outage
      if (m.retrievalResult.match(/^IPNI_ERROR_5\d\d$/)) continue

      retrievalStats.total++
      if (m.retrievalResult === 'OK') {
        retrievalStats.successful++
        if (m.protocol && m.protocol === 'http') { retrievalStats.successfulHttp++ }
      }
    }
  }

  // We need to flatten the map of retrieval stats per partyId to an array of objects for the sql query
  const flatStats = Array.from(retrievalStatsPerParty.entries()).flatMap(
    ([partyId, retrievalStats]) => ({ partyId, retrievalStats })
  )
  return flatStats
}

/**
 * @param {pg.Client} pgClient
 * @param {Iterable<Committee>} committees
 */
const updateDailyMinerDealsChecked = async (pgClient, committees) => {
  /** @type {Map<string, Set<string>>} */
  const minerPayloadCids = new Map()

  for (const c of committees) {
    const { minerId, cid } = c.retrievalTask

    let payloadCids = minerPayloadCids.get(minerId)
    if (!payloadCids) {
      payloadCids = new Set()
      minerPayloadCids.set(minerId, payloadCids)
    }

    payloadCids.add(cid)
  }

  // Convert the map to arrays for the query
  const minerIds = []
  const payloadCidsArrays = []

  for (const [minerId, payloadCids] of minerPayloadCids.entries()) {
    minerIds.push(minerId)
    payloadCidsArrays.push(Array.from(payloadCids))
  }

  if (debug.enabled) {
    debug(
      'Updating public stats - daily miner deals checked: miners count = %s, total payload CIDs = %s',
      minerIds.length,
      payloadCidsArrays.reduce((sum, cids) => sum + cids.length, 0)
    )
  }

  if (minerIds.length === 0) {
    debug('No miner deals to record')
    return
  }

  await pgClient.query(`
    INSERT INTO daily_miner_deals_checked (
      day,
      miner_id,
      payload_cids
    ) VALUES (
      now(),
      unnest($1::text[]),
      unnest($2::text[][])
    )
    ON CONFLICT(day, miner_id) DO UPDATE SET
      payload_cids = array(
        SELECT DISTINCT unnest(
          array_cat(daily_miner_deals_checked.payload_cids, EXCLUDED.payload_cids)
        )
      )
  `, [
    minerIds,
    payloadCidsArrays
  ])
}
