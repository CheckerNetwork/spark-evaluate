import * as Sentry from '@sentry/node'
import createDebug from 'debug'
import { updatePublicStats } from './public-stats.js'
import { buildRetrievalStats, recordCommitteeSizes } from './retrieval-stats.js'
import { groupMeasurementsToCommittees } from './committee.js'
import pRetry from 'p-retry'

/** @import {Measurement} from './preprocess.js' */

const debug = createDebug('spark:evaluate')

export const REQUIRED_COMMITTEE_SIZE = 1

/**
 * @param {object} args
 * @param {import('./round.js').RoundData} args.round
 * @param {bigint} args.roundIndex
 * @param {number} [args.requiredCommitteeSize]
 * @param {any} args.ieContract
 * @param {import('./spark-api.js').fetchRoundDetails} args.fetchRoundDetails,
 * @param {import('./typings.js').RecordTelemetryFn} args.recordTelemetry
 * @param {import('./typings.js').CreatePgClient} [args.createPgClient]
 * @param {Pick<Console, 'log' | 'error'>} args.logger
 * @param {(round: import('./round.js').RoundData, committees: Iterable<import('./committee.js').Committee>) => Promise<void>} args.prepareProviderRetrievalResultStats
 */
export const evaluate = async ({
  round,
  roundIndex,
  requiredCommitteeSize,
  ieContract,
  fetchRoundDetails,
  recordTelemetry,
  createPgClient,
  logger,
  prepareProviderRetrievalResultStats
}) => {
  requiredCommitteeSize ??= REQUIRED_COMMITTEE_SIZE

  // Get measurements
  /** @type {Measurement[]} */
  const measurements = round.measurements || []

  // Detect fraud
  const sparkRoundDetails = await fetchRoundDetails(await ieContract.getAddress(), roundIndex, recordTelemetry)
  round.details = sparkRoundDetails
  debug('ROUND DETAILS for round=%s', roundIndex, sparkRoundDetails)

  // PERFORMANCE: Avoid duplicating the array of measurements because there are
  // hundreds of thousands of them. All the function groupMeasurementsToCommittees
  // needs is to iterate over the accepted measurements once.
  const iterateAcceptedMeasurements = function * () {
    for (const m of measurements) {
      // Mark all measurements as accepted by default.
      m.taskingEvaluation = 'OK'
      yield m
    }
  }

  const evaluationCommittees = groupMeasurementsToCommittees(iterateAcceptedMeasurements())
  for (const c of evaluationCommittees.values()) {
    c.evaluate({ requiredCommitteeSize })
  }

  const committees = Array.from(evaluationCommittees.values())
  logger.log(
    'EVALUATE ROUND %s: Evaluated %s measurements.\n',
    roundIndex,
    measurements.length
  )

  // Calculate aggregates per evaluation outcome
  /** @type {Partial<Record<import('./typings.js').TaskingEvaluation,  number>>} */
  logger.log(
    'EVALUATE ROUND %s: Evaluated %s measurements.\n',
    roundIndex,
    measurements.length
  )

  // Telemetry and stats (no rewards)
  recordTelemetry('evaluate', point => {
    point.intField('round_index', roundIndex)
    point.intField('total_measurements', measurements.length)
    point.intField('total_nodes', countUniqueNodes(measurements))
  })

  const ignoredErrors = []

  // Both retrieval_stats_honest and retrieval_stats_all report the same stats,
  // but we keep retrieval_stats_honest for backwards compatibility.
  try {
    recordTelemetry('retrieval_stats_honest', (point) => {
      point.intField('round_index', roundIndex)
      buildRetrievalStats(measurements, point)
    })
  } catch (err) {
    console.error('Cannot record retrieval stats (honest).', err)
    ignoredErrors.push(err)
    Sentry.captureException(err)
  }

  try {
    recordTelemetry('retrieval_stats_all', (point) => {
      point.intField('round_index', roundIndex)
      buildRetrievalStats(measurements, point)
    })
  } catch (err) {
    console.error('Cannot record retrieval stats (all).', err)
    ignoredErrors.push(err)
    Sentry.captureException(err)
  }

  try {
    recordTelemetry('committees', (point) => {
      point.intField('round_index', roundIndex)
      point.intField('committees_all', committees.length)
      point.intField('committees_too_small',
        committees
          .filter(c => c.decision?.retrievalResult === 'COMMITTEE_TOO_SMALL')
          .length
      )
      recordCommitteeSizes(committees, point)
    })
  } catch (err) {
    console.error('Cannot record committees.', err)
    ignoredErrors.push(err)
    Sentry.captureException(err)
  }

  if (createPgClient) {
    try {
      await updatePublicStats({
        createPgClient,
        committees,
        allMeasurements: measurements,
        findDealClients: (minerId, cid) => sparkRoundDetails.retrievalTasks
          .find(t => t.cid === cid && t.minerId === minerId)?.clients,
        findDealAllocators: (minerId, cid) => sparkRoundDetails.retrievalTasks
          .find(t => t.cid === cid && t.minerId === minerId)?.allocators
      })
    } catch (err) {
      console.error('Cannot update public stats.', err)
      ignoredErrors.push(err)
      Sentry.captureException(err)
    }
  }

  try {
    await pRetry(
      () => prepareProviderRetrievalResultStats(round, committees),
      {
        async onFailedAttempt (error) {
          console.warn(error)
          console.warn(
            'Preparing provider retrieval result stats failed. Retrying...'
          )
        }
      }
    )
  } catch (err) {
    console.error('Cannot prepare provider retrieval result stats.', err)
    ignoredErrors.push(err)
    Sentry.captureException(err)
  }

  return { ignoredErrors }
}

/**
 * @param {Measurement[]} measurements
 */
const countUniqueNodes = (measurements) => {
  const nodes = new Set()
  for (const m of measurements) {
    const key = `${m.inet_group}::${m.participantAddress}`
    nodes.add(key)
  }
  return nodes.size
}
