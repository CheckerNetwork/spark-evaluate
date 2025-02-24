import * as Sentry from '@sentry/node'
import timers from 'node:timers/promises'
import pRetry from 'p-retry'
import { createDagJsonCar } from './car.js'
import { CID } from 'multiformats'

const ONE_HOUR = 60 * 60 * 1000

const withPgClient = fn => async ({ createPgClient, ...args }) => {
  const pgClient = await createPgClient()
  try {
    return await fn({ pgClient, ...args })
  } finally {
    await pgClient.end()
  }
}

export const build = committees => {
  /** @type {Map<string, {total: number, successful: number, successfulHttp:number, successfulHttpHead: number}>} */
  const providerRetrievalResultStats = new Map()
  for (const c of committees) {
    // IMPORTANT: include minority results in the calculation
    for (const m of c.measurements) {
      const minerId = m.minerId
      const retrievalStats = providerRetrievalResultStats.get(minerId) ?? { total: 0, successful: 0, successfulHttp: 0, successfulHttpHead: 0 }
      retrievalStats.total++
      if (m.retrievalResult === 'OK') {
        retrievalStats.successful++
        if (m.protocol && m.protocol === 'http') {
          retrievalStats.successfulHttp++
          if (typeof m.head_status_code === 'number' && m.head_status_code >= 200 && m.head_status_code < 300) {
            retrievalStats.successfulHttpHead++
          }
        }
      }
      providerRetrievalResultStats.set(minerId, retrievalStats)
    }
  }
  return providerRetrievalResultStats
}

export const publishRoundDetails = async ({ storachaClient, round }) => {
  const { car, cid } = await createDagJsonCar(round.details)
  await pRetry(() => storachaClient.uploadCAR(car))
  return cid
}

export const prepare = withPgClient(async ({ storachaClient, pgClient, round, committees, sparkEvaluateVersion, ieContractAddress }) => {
  const roundDetailsCid = await publishRoundDetails({ storachaClient, round })
  await pgClient.query(`
    INSERT INTO unpublished_provider_retrieval_result_stats_rounds
    (round_index, contract_address, spark_evaluate_version, measurement_batches, round_details, provider_retrieval_result_stats)
    VALUES
    ($1, $2, $3, $4, $5, $6)
  `, [
    round.index,
    ieContractAddress,
    sparkEvaluateVersion,
    round.measurementBatches,
    roundDetailsCid.toString(),
    Object.fromEntries(build(committees).entries())
  ])
})

export const publish = withPgClient(async ({ pgClient, storachaClient, rsrContract }) => {
  const { rows } = await pgClient.query(`
    WITH oldest_publishable_round AS (
      SELECT *
      FROM unpublished_provider_retrieval_result_stats_rounds
      WHERE evaluated_at < now()::date
      ORDER BY evaluated_at
      LIMIT 1
    )
    SELECT *, evaluated_at::DATE::TEXT as date
    FROM unpublished_provider_retrieval_result_stats_rounds
    WHERE
      (SELECT evaluated_at::date FROM oldest_publishable_round) <= evaluated_at
      AND evaluated_at < (SELECT evaluated_at::date FROM oldest_publishable_round) + interval '1 day'
      AND contract_address = (SELECT contract_address FROM oldest_publishable_round)
    ORDER BY round_index
  `)
  if (rows.length === 0) {
    return
  }

  const providerRetrievalResultStats = new Map()
  for (const row of rows) {
    for (const [providerId, { successful, total }] of Object.entries(row.provider_retrieval_result_stats)) {
      if (!providerRetrievalResultStats.has(providerId)) {
        providerRetrievalResultStats.set(providerId, { successful: 0, total: 0 })
      }
      providerRetrievalResultStats.get(providerId).successful += successful
      providerRetrievalResultStats.get(providerId).total += total
    }
  }

  const { cid, car } = await createDagJsonCar({
    date: rows[0].date,
    meta: {
      rounds: rows.map(row => ({
        index: row.round_index,
        contractAddress: row.contract_address,
        sparkEvaluateVersion: {
          gitCommit: row.spark_evaluate_version
        },
        measurementBatches: row.measurement_batches.map(cid => CID.parse(cid)),
        details: CID.parse(row.round_details)
      }))
    },
    providerRetrievalResultStats: [...providerRetrievalResultStats.entries()]
      .map(([providerId, { successful, total }]) => ({ providerId, successful, total }))
  })
  await pRetry(() => storachaClient.uploadCAR(car))
  console.log(`https://${cid}.ipfs.w3s.link`)

  const tx = await pRetry(() => rsrContract.addProviderRetrievalResultStats(cid.toString()))
  console.log(tx.hash)
  await tx.wait()

  await pgClient.query(`
    DELETE FROM unpublished_provider_retrieval_result_stats_rounds
    WHERE
      round_index <= $1
      AND contract_address = $2
  `, [
    rows.at(-1).round_index,
    rows[0].contract_address
  ])
})

export const runPublishLoop = async ({ createPgClient, storachaClient, rsrContract }) => {
  while (true) {
    try {
      await publish({ createPgClient, storachaClient, rsrContract })
    } catch (err) {
      console.error(err)
      Sentry.captureException(err)
    } finally {
      await timers.setTimeout(ONE_HOUR)
    }
  }
}
