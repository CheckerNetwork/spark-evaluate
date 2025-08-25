import assert from 'node:assert'
import createDebug from 'debug'
import pRetry from 'p-retry'

const debug = createDebug('spark:platform-stats')

/**
 * @param {import('pg').Client} pgClient
 * @param {import('./preprocess.js').Measurement[]} allMeasurements
 */
export const updatePlatformStats = async (pgClient, allMeasurements) => {
  const participantsMap = await mapParticipantsToIds(pgClient, new Set(allMeasurements.map(m => m.participantAddress)))
  await updateDailyParticipants(pgClient, Array.from(participantsMap.values()))
  await updateStationsAndParticipants(pgClient, allMeasurements, participantsMap)
}

/**
 * @param {import('pg').Client} pgClient
 * @param {number[]} participantIds
 */
export const updateDailyParticipants = async (pgClient, participantIds) => {
  debug('Updating daily participants, count=%s', participantIds.length)
  await pgClient.query(`
    INSERT INTO daily_participants (day, participant_id)
    SELECT now() as day, UNNEST($1::INT[]) AS participant_id
    ON CONFLICT DO NOTHING
  `, [
    participantIds
  ])
}

/**
 * @param {import('pg').Client} pgClient
 * @param {import('./preprocess.js').Measurement[]} measurements
 * @param {Map<string, number>} participantsMap
 * @param {object} options
 * @param {Date} [options.day]
 */
export const updateStationsAndParticipants = async (
  pgClient,
  measurements,
  participantsMap,
  { day = new Date() } = {}
) => {
  /** @type {Map<number, Map<string, {accepted: number, total: number}>>} */
  const participantsStationsStats = new Map()

  /** @type {Map<number, Set<string>>} */
  const subnets = new Map()

  for (const m of measurements) {
    if (m.stationId == null) continue
    const participantId = participantsMap.get(m.participantAddress)
    if (participantId == null) continue

    let participant = participantsStationsStats.get(participantId)
    if (!participant) {
      participant = new Map()
      participantsStationsStats.set(participantId, participant)
    }

    let stationStats = participant.get(m.stationId)
    if (!stationStats) {
      stationStats = { accepted: 0, total: 0 }
      participant.set(m.stationId, stationStats)
    }

    stationStats.total++
    if (m.taskingEvaluation === 'OK' && m.consensusEvaluation === 'MAJORITY_RESULT') stationStats.accepted++

    let subnetsSet = subnets.get(participantId)
    if (!subnetsSet) {
      subnetsSet = new Set()
      subnets.set(participantId, subnetsSet)
    }

    subnetsSet.add(m.inet_group)
  }

  // Convert the nested map structure to arrays for the query
  const flattenedStats = Array.from(participantsStationsStats.entries()).flatMap(([participantId, stationMap]) =>
    Array.from(stationMap.entries()).map(([stationId, stationStats]) => ({
      participantId,
      stationId,
      ...stationStats
    }))
  )

  // Convert the subnets map to an array for the query
  const participantsSubnets = Array.from(subnets.entries()).flatMap(([participantId, subnetSet]) =>
    Array.from(subnetSet).map(subnet => ({ participantId, subnet }))
  )

  debug('Updating recent station details, station_participant_count=%s, participant_subnet_count=%s', flattenedStats.length, participantsSubnets.length)

  try {
    await pgClient.query('BEGIN')

    await pgClient.query(`
      INSERT INTO recent_station_details (
        day,
        station_id,
        participant_id,
        accepted_measurement_count,
        total_measurement_count
      )
      VALUES (
        $1::DATE,
        unnest($2::text[]),
        unnest($3::int[]),
        unnest($4::int[]),
        unnest($5::int[])
      )
      ON CONFLICT (day, station_id, participant_id) DO UPDATE
      SET accepted_measurement_count = recent_station_details.accepted_measurement_count
                                        + EXCLUDED.accepted_measurement_count,
          total_measurement_count = recent_station_details.total_measurement_count
                                        + EXCLUDED.total_measurement_count
    `, [
      day,
      flattenedStats.map(s => s.stationId),
      flattenedStats.map(s => s.participantId),
      flattenedStats.map(s => s.accepted),
      flattenedStats.map(s => s.total)
    ])

    await pgClient.query(`
      INSERT INTO recent_active_stations (day, station_id)
      SELECT $1::DATE, unnest($2::text[])
      ON CONFLICT (day, station_id) DO NOTHING
    `, [
      day,
      flattenedStats.map(s => s.stationId)
    ])

    await pgClient.query(`
      INSERT INTO recent_participant_subnets (day, participant_id, subnet)
      SELECT $1::DATE, unnest($2::int[]), unnest($3::text[])
      ON CONFLICT (day, participant_id, subnet) DO NOTHING
    `, [
      day,
      participantsSubnets.map(ps => ps.participantId),
      participantsSubnets.map(ps => ps.subnet)
    ])

    await pgClient.query('COMMIT')
  } catch (err) {
    await pgClient.query('ROLLBACK')
    throw err
  }
}

/**
 * @param {import('pg').Client} pgClient
 * @param {Set<string>} participantsSet
 * @returns {Promise<Map<string, number>>} A map of participant addresses to ids.
 */
export const mapParticipantsToIds = async (pgClient, participantsSet) => {
  debug('Mapping participants to id, count=%s', participantsSet.size)

  /** @type {Map<string, number>} */
  const participantsMap = new Map()

  // TODO: We can further optimise performance of this function by using
  // an in-memory LRU cache. Our network has currently ~2k participants,
  // we need ~50 bytes for each (address, id) pair, that's only ~100KB of data.

  // TODO: passing the entire list of participants as a single query parameter
  // will probably not scale beyond several thousands of addresses. We will
  // need to rework the queries to split large arrays into smaller batches.

  // In most rounds, we have already seen most of the participant addresses
  // If we use "INSERT...ON CONFLICT", then PG increments id counter even for
  // existing addresses where we end up skipping the insert. This could quickly
  // exhaust the space of all 32bit integers.
  // Solution: query the table for know records before running the insert.
  //
  // Caveat: In my testing, this query was not able to leverage the (unique)
  // index on participants.participant_address and performed a full table scan
  // after the array grew past ~10 items. If this becomes a problem, we can
  // introduce the LRU cache mentioned above.
  const { rows: found } = await pgClient.query(
    'SELECT * FROM participants WHERE participant_address = ANY($1::TEXT[])',
    [Array.from(participantsSet.values())]
  )
  debug('Known participants count=%s', found.length)

  // eslint-disable-next-line camelcase
  for (const { id, participant_address } of found) {
    participantsMap.set(participant_address, id)
    participantsSet.delete(participant_address)
  }

  debug('New participant addresses count=%s', participantsSet.size)

  // Register the new addresses. Use "INSERT...ON CONFLICT" to handle the race condition
  // where another client may have registered these addresses between our previous
  // SELECT query and the next INSERT query.
  const newAddresses = Array.from(participantsSet.values())
  debug('Registering new participant addresses, count=%s', newAddresses.length)
  const { rows: created } = await pgClient.query(`
    INSERT INTO participants (participant_address)
    SELECT UNNEST($1::TEXT[]) AS participant_address
    ON CONFLICT(participant_address) DO UPDATE
      -- this no-op update is needed to populate "RETURNING id, participant_address"
      SET participant_address = EXCLUDED.participant_address
    RETURNING id, participant_address
  `, [
    newAddresses
  ])

  assert.strictEqual(created.length, newAddresses.length)
  for (const { id, participant_address: participantAddress } of created) {
    participantsMap.set(participantAddress, id)
  }

  return participantsMap
}

/**
 * @param {import('./typings.js').CreatePgClient} createPgClient
 * @param {object} options
 * @param {Array<Function>} [options.functionsToRun]
 */
export const refreshDatabase = async (
  createPgClient,
  {
    functionsToRun = [
      updateTopMeasurementParticipants,
      aggregateAndCleanUpRecentData,
      updateMonthlyActiveStationCount
    ]
  } = {}
) => {
  const pgClient = await createPgClient()

  for (const func of functionsToRun) {
    try {
      await func(pgClient)
    } catch (err) {
      console.error(`Error running function ${func.name}:`, err)
    }
  }

  await pgClient.end()
}

/**
 * @param {import('pg').Client} pgClient
 */
export const updateTopMeasurementParticipants = async (pgClient) => {
  try {
    await pRetry(async () => {
      await pgClient.query('REFRESH MATERIALIZED VIEW top_measurement_participants_yesterday_mv')
    }, {
      retries: 3,
      onFailedAttempt: ({ error }) => {
        console.warn(`Attempt to refresh materialized view failed: ${error.message}. Retrying...`)
      }
    })
  } catch (err) {
    console.error('Error refreshing top measurement participants', err)
  }
}

/**
 * @param {import('pg').Client} pgClient
 */
export const aggregateAndCleanUpRecentData = async (pgClient) => {
  try {
    await pgClient.query('BEGIN')

    await pgClient.query(`
      WITH recent_station_details_summary AS (
        SELECT
          day,
          SUM(accepted_measurement_count) AS accepted_measurement_count,
          SUM(total_measurement_count) AS total_measurement_count,
          COUNT(DISTINCT station_id) AS station_count,
          COUNT(DISTINCT participant_id) AS participant_address_count
        FROM
          recent_station_details
        WHERE
          day <= CURRENT_DATE - INTERVAL '2 days'
        GROUP BY
          day
      ),
      recent_participant_subnets_summary AS (
        SELECT
          day,
          COUNT(DISTINCT subnet) AS inet_group_count
        FROM
          recent_participant_subnets
        WHERE
          day <= CURRENT_DATE - INTERVAL '2 days'
        GROUP BY
          day
      )
      INSERT INTO daily_platform_stats (
        day,
        accepted_measurement_count,
        total_measurement_count,
        station_count,
        participant_address_count,
        inet_group_count
      )
      SELECT
        rsd.day,
        rsd.accepted_measurement_count,
        rsd.total_measurement_count,
        rsd.station_count,
        rsd.participant_address_count,
        rps.inet_group_count
      FROM
        recent_station_details_summary rsd
      JOIN
        recent_participant_subnets_summary rps ON rsd.day = rps.day
    `)

    // Delete aggregated data from recent_station_details and recent_participant_subnets
    await pgClient.query(`
      DELETE FROM recent_station_details WHERE day <= CURRENT_DATE - INTERVAL '2 days'
    `)
    await pgClient.query(`
      DELETE FROM recent_participant_subnets WHERE day <= CURRENT_DATE - INTERVAL '2 days'
    `)

    await pgClient.query('COMMIT')
  } catch (err) {
    await pgClient.query('ROLLBACK')
    throw err
  }
}

/**
 * @param {import('pg').Client} pgClient
 */
export const updateMonthlyActiveStationCount = async (pgClient) => {
  const result = await pgClient.query(`
    SELECT 1 FROM monthly_active_station_count
    WHERE month = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
  `)

  if (result.rowCount > 0) return

  try {
    await pgClient.query('BEGIN')
    await pgClient.query(`
      INSERT INTO monthly_active_station_count (month, station_count)
      SELECT
        DATE_TRUNC('month', day) AS month,
        COUNT(DISTINCT station_id) AS station_count
      FROM recent_active_stations
      WHERE day >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
        AND day < DATE_TRUNC('month', CURRENT_DATE)
      GROUP BY DATE_TRUNC('month', day)
    `)

    await pgClient.query(`
      DELETE FROM recent_active_stations
      WHERE day >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
        AND day < DATE_TRUNC('month', CURRENT_DATE);
    `)

    await pgClient.query('COMMIT')
  } catch (err) {
    await pgClient.query('ROLLBACK')
    throw err
  }
}
