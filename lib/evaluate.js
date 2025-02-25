import * as Sentry from '@sentry/node'
import createDebug from 'debug'
import assert from 'node:assert'
import { getRandomnessForSparkRound } from './drand-client.js'
import { updatePublicStats } from './public-stats.js'
import { buildRetrievalStats, getTaskId, recordCommitteeSizes } from './retrieval-stats.js'
import { getTasksAllowedForStations } from './tasker.js'
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

  // Get measurements
  /** @type {Measurement[]} */
  const measurements = round.measurements || []

  // Detect fraud

  const sparkRoundDetails = await fetchRoundDetails(await ieContract.getAddress(), roundIndex, recordTelemetry)
  round.details = sparkRoundDetails
  // Omit the roundDetails object from the format string to get nicer formatting
  debug('ROUND DETAILS for round=%s', roundIndex, sparkRoundDetails)

  const started = Date.now()

  const { committees, timings: fraudDetectionTimings } = await runFraudDetection({
    roundIndex,
    measurements,
    sparkRoundDetails,
    requiredCommitteeSize,
    logger
  })
  const measurementsToReward = measurements.filter(
    m => m.taskingEvaluation === 'OK' && m.consensusEvaluation === 'MAJORITY_RESULT'
  )

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
    'EVALUATE ROUND %s: Evaluated %s measurements, rewarding %s entries.\n%o',
    roundIndex,
    measurements.length,
    measurementsToReward.length,
    evaluationOutcomes
  )

  const fraudDetectionDuration = Date.now() - started

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
    point.intField('fraud_detection_duration_ms', fraudDetectionDuration)
    for (const { influxField, duration } of fraudDetectionTimings) {
      point.intField(`fraud_detection_timings_${influxField}_ms`, duration)
    }

    for (const [type, count] of Object.entries(evaluationOutcomes)) {
      point.intField(`measurements_${type}`, count)
    }
  })

  const ignoredErrors = []

  try {
    recordTelemetry('retrieval_stats_honest', (point) => {
      point.intField('round_index', roundIndex)
      // FIXME: Include non-majority measurements in these stats
      // See https://github.com/filecoin-station/spark-evaluate/issues/446
      buildRetrievalStats(measurementsToReward, point)
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
          .find(t => t.cid === cid && t.minerId === minerId)?.clients
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
 * @param {object} args
 * @param {bigint} args.roundIndex
 * @param {Measurement[]} args.measurements
 * @param {import('./typings.js').RoundDetails} args.sparkRoundDetails
 * @param {number} args.requiredCommitteeSize
 * @param {Pick<Console, 'log' | 'error'>} args.logger
 * @returns {Promise<{committees: import('./committee.js').Committee[], timings: {influxField: string, duration: number}[]}>}
 */
export const runFraudDetection = async ({
  roundIndex,
  measurements,
  sparkRoundDetails,
  requiredCommitteeSize,
  logger
}) => {
  const timings = {
    taskBuilding: {
      influxField: 'task_building',
      start: null,
      end: null
    },
    taskingEvaluation: {
      influxField: 'tasking_evaluation',
      start: null,
      end: null
    },
    inetGroupsEvaluation: {
      influxField: 'inet_groups_evaluation',
      start: null,
      end: null
    },
    majorityEvaluation: {
      influxField: 'majority_evaluation',
      start: null,
      end: null
    }
  }

  const randomness = await pRetry(
    () => getRandomnessForSparkRound(Number(sparkRoundDetails.startEpoch)),
    {
      async onFailedAttempt (error) {
        console.warn(error)
        console.warn('Drand request failed. Retrying...')
      }
    }
  )

  timings.taskBuilding.start = new Date()
  const tasksAllowedForStations = await getTasksAllowedForStations(
    sparkRoundDetails,
    randomness,
    measurements
  )
  timings.taskBuilding.end = new Date()

  logger.log(
    'EVALUATE ROUND %s: built per-node task lists in %sms [Tasks=%s;TN=%s;Nodes=%s]',
    roundIndex,
    timings.taskBuilding.end - timings.taskBuilding.start,
    sparkRoundDetails.retrievalTasks.length,
    sparkRoundDetails.maxTasksPerNode,
    tasksAllowedForStations.size
  )

  //
  // 1. Filter out measurements not belonging to any valid task in this round
  //    or missing some of the required fields like `inet_group`
  //
  timings.taskingEvaluation.start = new Date()
  for (const m of measurements) {
    // sanity checks to get nicer errors if we forget to set required fields in unit tests
    assert(typeof m.inet_group === 'string', 'missing inet_group')
    assert(typeof m.finished_at === 'number', 'missing finished_at')

    const isValidTask = sparkRoundDetails.retrievalTasks.some(
      t => t.cid === m.cid && t.minerId === m.minerId
    )
    if (!isValidTask) {
      m.taskingEvaluation = 'TASK_NOT_IN_ROUND'
      continue
    }

    const isValidTaskForNode = tasksAllowedForStations.get(m.stationId).some(
      t => t.cid === m.cid && t.minerId === m.minerId
    )
    if (!isValidTaskForNode) {
      m.taskingEvaluation = 'TASK_WRONG_NODE'
    }
  }
  timings.taskingEvaluation.end = new Date()

  //
  // 2. Accept only maxTasksPerNode measurements from each inet group
  //
  timings.inetGroupsEvaluation.start = new Date()
  /** @type {Map<string, Measurement[]>} */
  const inetGroups = new Map()
  for (const m of measurements) {
    if (m.taskingEvaluation) continue

    const key = m.inet_group
    let group = inetGroups.get(key)
    if (!group) {
      group = []
      inetGroups.set(key, group)
    }

    group.push(m)
  }

  const getHash = async (/** @type {Measurement} */ m) => {
    const bytes = await globalThis.crypto.subtle.digest('SHA-256', Buffer.from(new Date(m.finished_at).toISOString()))
    return Buffer.from(bytes).toString('hex')
  }

  for (const [key, groupMeasurements] of inetGroups.entries()) {
    debug('Evaluating measurements from inet group %s', key)

    // Pick one measurement per task to reward and mark all others as not eligible for rewards.
    // We also want to reward at most `maxTasksPerNode` measurements in each inet group.
    //
    // The difficult part: how to choose a measurement randomly but also fairly, so
    // that each measurement has the same chance of being picked for the reward?
    // We also want the selection algorithm to be deterministic.
    //
    // Note that we cannot rely on participant addresses because it's super easy
    // for node operators to use a different address for each measurement.
    //
    // Let's start with a simple algorithm we can later tweak:
    // 1. Hash the `finished_at` timestamp recorded by the server
    // 2. Pick the measurement with the lowest hash value
    // This relies on the fact that the hash function has a random distribution.
    // We are also making the assumption that each measurement has a distinct `finished_at` field.
    //
    const measurementsWithHash = await Promise.all(
      groupMeasurements.map(async (m) => ({ m, h: await getHash(m) }))
    )
    // Sort measurements using the computed hash
    measurementsWithHash.sort((a, b) => a.h.localeCompare(b.h, 'en'))

    const tasksSeen = new Set()
    for (const { m, h } of measurementsWithHash) {
      const taskId = getTaskId(m)

      if (tasksSeen.has(taskId)) {
        debug('  pa: %s h: %s task: %s - task was already rewarded', m.participantAddress, h, taskId)
        m.taskingEvaluation = 'DUP_INET_GROUP'
        continue
      }

      if (tasksSeen.size >= sparkRoundDetails.maxTasksPerNode) {
        debug('  pa: %s h: %s task: %s - already rewarded max tasks', m.participantAddress, h, taskId)
        m.taskingEvaluation = 'TOO_MANY_TASKS'
        continue
      }

      tasksSeen.add(taskId)
      m.taskingEvaluation = 'OK'
      debug('  pa: %s h: %s task: %s - REWARD', m.participantAddress, h, taskId)
    }
  }
  timings.inetGroupsEvaluation.end = new Date()

  //
  // 3. Group measurements to per-task committees and find majority result
  //
  timings.majorityEvaluation.start = new Date()

  // PERFORMANCE: Avoid duplicating the array of measurements because there are
  // hundreds of thousands of them. All the function groupMeasurementsToCommittees
  // needs is to iterate over the accepted measurements once.
  const iterateAcceptedMeasurements = function * () {
    for (const m of measurements) {
      if (m.taskingEvaluation !== 'OK') continue
      yield m
    }
  }

  const committees = groupMeasurementsToCommittees(iterateAcceptedMeasurements())

  for (const c of committees.values()) {
    c.evaluate({ requiredCommitteeSize })
  }
  timings.majorityEvaluation.end = new Date()

  if (debug.enabled) {
    for (const m of measurements) {
      // Print round & participant address & CID together to simplify lookup when debugging
      // Omit the `m` object from the format string to get nicer formatting
      debug(
        'FRAUD ASSESSMENT for round=%s client=%s cid=%s minerId=%s',
        roundIndex,
        m.participantAddress,
        m.cid,
        m.minerId,
        m)
    }
  }

  return {
    committees: Array.from(committees.values()),
    timings: Object
      .values(timings)
      .map(({ influxField, start, end }) => ({ influxField, duration: end - start }))
  }
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
