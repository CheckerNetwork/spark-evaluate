import * as Sentry from '@sentry/node'
import createDebug from 'debug'
import { updatePublicStats } from './public-stats.js'
import { buildRetrievalStats, recordCommitteeSizes } from './retrieval-stats.js'
import { groupMeasurementsToCommittees } from './committee.js'
import pRetry from 'p-retry'

/** @import {Measurement} from './preprocess.js' */

const debug = createDebug('spark:evaluate')

export const MAX_SCORE = 1_000_000_000_000_000n
export const REQUIRED_COMMITTEE_SIZE = 30

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

  const sparkRoundDetails = await fetchRoundDetails(await ieContract.getAddress(), roundIndex, recordTelemetry)
  round.details = sparkRoundDetails
  // Omit the roundDetails object from the format string to get nicer formatting
  debug('ROUND DETAILS for round=%s', roundIndex, sparkRoundDetails)

  const taskCommittees = groupMeasurementsToCommittees(measurements)
  for (const c of taskCommittees.values()) {
    c.evaluate({ requiredCommitteeSize })
  }
  const committees = Array.from(taskCommittees.values())

  // Calculate aggregates per evaluation outcome
  // This is used for logging and telemetry
  /** @type {Partial<Record<import('./typings.js').TaskingEvaluation,  number>>} */
  const evaluationOutcomes = {
    OK: 0,
    TASK_NOT_IN_ROUND: 0,
    DUP_INET_GROUP: 0,
    TOO_MANY_TASKS: 0
  }
  for (const m of measurements) {
    evaluationOutcomes[m.taskingEvaluation] = (evaluationOutcomes[m.taskingEvaluation] ?? 0) + 1
  }
  logger.log(
    'EVALUATE ROUND %s: Evaluated %s measurements.\n%o',
    roundIndex,
    measurements.length,
    evaluationOutcomes
  )

  recordTelemetry('evaluate', point => {
    point.intField('round_index', roundIndex)
    point.intField('total_measurements', measurements.length)
    point.intField('total_nodes', countUniqueNodes(measurements))
    for (const [type, count] of Object.entries(evaluationOutcomes)) {
      point.intField(`measurements_${type}`, count)
    }
  })

  const ignoredErrors = []
  const approvedMeasurements = measurements.filter(
    m => m.taskingEvaluation === 'OK'
  )
  try {
    recordTelemetry('retrieval_stats_honest', (point) => {
      point.intField('round_index', roundIndex)
      buildRetrievalStats(approvedMeasurements, point)
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
  // Deduplication removed: count all measurements, not just unique ones
  return measurements.length
}
