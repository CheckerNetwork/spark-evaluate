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
 * @param {(participants: string[], values: bigint[]) => Promise<void>} args.setScores
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
  setScores,
  fetchRoundDetails,
  recordTelemetry,
  createPgClient,
  logger,
  prepareProviderRetrievalResultStats
}) => {
  requiredCommitteeSize ??= REQUIRED_COMMITTEE_SIZE

  const sparkRoundDetails = await fetchRoundDetails(await ieContract.getAddress(), roundIndex, recordTelemetry)
  round.details = sparkRoundDetails
  // Omit the roundDetails object from the format string to get nicer formatting
  debug('ROUND DETAILS for round=%s', roundIndex, sparkRoundDetails)

  // Get measurements
  /** @type {Measurement[]} */
  const measurements = round.measurements || []

  // Fraud detection removed: all measurements are accepted
  const evaluationCommittees = groupMeasurementsToCommittees(measurements)
  for (const committee of evaluationCommittees.values()) {
    committee.evaluate({ requiredCommitteeSize })
  }
  const committees = Array.from(evaluationCommittees.values())
  const measurementsToReward = measurements


  // Calculate reward shares
  const participants = {}
  let sum = 0n
  for (const measurement of measurementsToReward) {
    if (!participants[measurement.participantAddress]) {
      participants[measurement.participantAddress] = 0n
    }
    participants[measurement.participantAddress] += 1n
  }
  for (const [participantAddress, participantTotal] of Object.entries(participants)) {
    const score = participantTotal *
      MAX_SCORE /
      BigInt(measurementsToReward.length)
    participants[participantAddress] = score
    sum += score
  }

  if (sum < MAX_SCORE) {
    const delta = MAX_SCORE - sum
    const score = (participants['0x000000000000000000000000000000000000dEaD'] ?? 0n) + delta
    participants['0x000000000000000000000000000000000000dEaD'] = score
    logger.log('EVALUATE ROUND %s: added %s as rounding to MAX_SCORE', roundIndex, delta)
  }

  // Calculate aggregates per evaluation outcome
  // This is used for logging and telemetry
  /** @type {Partial<Record<import('./typings.js').TaskingEvaluation,  number>>} */
  logger.log(
    'EVALUATE ROUND %s: Evaluated %s measurements, rewarding %s entries.',
    roundIndex,
    measurements.length,
    measurementsToReward.length
  )

  // Fraud detection duration and timings removed

  // Submit scores to IE

  const totalScore = Object.values(participants).reduce((sum, val) => sum + val, 0n)
  logger.log(
    'EVALUATE ROUND %s: Setting scores; number of participants: %s, total score: %s',
    roundIndex,
    Object.keys(participants).length,
    totalScore === 1000000000000000n ? '100%' : totalScore
  )

  const start = Date.now()
  try {
    await setScores(Object.keys(participants), Object.values(participants))
    logger.log('EVALUATE ROUND %s: POST /scores', roundIndex)
  } catch (err) {
    logger.error('CANNOT SUBMIT SCORES FOR ROUND %s: %s',
      roundIndex,
      err
    )
    Sentry.captureException(err, { extra: { roundIndex } })
  }
  const setScoresDuration = Date.now() - start

  recordTelemetry('evaluate', point => {
    point.intField('round_index', roundIndex)
    point.intField('total_participants', Object.keys(participants).length)
    point.intField('total_measurements', measurements.length)
    point.intField('total_nodes', countUniqueNodes(measurements))
    point.intField('honest_measurements', measurementsToReward.length)
    point.intField('set_scores_duration_ms', setScoresDuration)
    // Fraud detection timings removed
    // Evaluation outcomes removed
  })

  const ignoredErrors = []
  const approvedMeasurements = measurements
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
  const nodes = new Set()
  for (const m of measurements) {
    const key = `${m.inet_group}::${m.participantAddress}`
    nodes.add(key)
  }
  return nodes.size
}
