import assert from 'node:assert'
import createDebug from 'debug'
import getValueAtPercentile from 'just-percentile'

/** @import {Measurement} from './preprocess.js' */
/** @import {Committee} from './committee.js' */

const debug = createDebug('spark:retrieval-stats')

export {
  getValueAtPercentile
}

/**
 * @param {Measurement[]} measurements
 * @param {import('./typings.js').Point} telemetryPoint
 */
export const buildRetrievalStats = (measurements, telemetryPoint) => {
  const totalCount = measurements.length
  if (totalCount < 1) {
    telemetryPoint.intField('measurements', 0)
    telemetryPoint.intField('unique_tasks', 0)
    return
  }

  const uniqueTasksCount = countUniqueTasks(measurements)

  /** @type {Set<string>} */
  const tasksWithHttpAdvertisement = new Set()
  /** @type {Set<string>} */
  const tasksWithIndexerResults = new Set()

  // Calculate aggregates per retrieval result

  // We are intentionally not initializing all possible keys here.
  // Example of omitted keys: UNDEFINED, ERROR_500 and ERROR_404.
  // The idea is that if we don't explicitly initialise them here and there is no measurement with
  // such retrieval result, then the Grafana dashboard will not show these results at all.
  /** @type {Partial<Record<import('./typings.js').RetrievalResult, number>>} */
  const resultBreakdown = {
    OK: 0,
    TIMEOUT: 0
  }

  const indexerResultBreakdown = {
    OK: 0,
    HTTP_NOT_ADVERTISED: 0,
    ERROR_404: 0
  }

  const participants = new Set()
  const inetGroups = new Set()
  const tasksPerNode = new Map()
  /** @type {Map<string, number>} */
  const acceptedMeasurementsPerInetGroup = new Map()
  let downloadBandwidth = 0

  const ttfbValues = []
  const durationValues = []
  const sizeValues = []
  let httpSuccesses = 0
  let indexerServerErrorCount = 0

  for (const m of measurements) {
    // `retrievalResult` should be always set by lib/preprocess.js, so we should never encounter
    // `UNDEFINED` result. However, I am still handling that edge case for extra robustness.
    updateBreakdown(resultBreakdown, m.retrievalResult ?? 'UNDEFINED')

    // Older versions of Spark Checker don't query IPNI, indexerResult is undefined.
    updateBreakdown(indexerResultBreakdown, m.indexerResult ?? 'UNDEFINED')

    participants.add(m.participantAddress)
    inetGroups.add(m.inet_group)

    // don't trust the checker to submit a positive integers
    // TODO: reject measurements with invalid values during the preprocess phase?
    const byteLength = typeof m.byte_length === 'number' && m.byte_length >= 0
      ? m.byte_length
      : undefined
    const startAt = m.start_at
    const firstByteAt = m.first_byte_at
    const endAt = m.end_at
    const ttfb = startAt && firstByteAt && (firstByteAt - startAt)
    const duration = startAt && endAt && (endAt - startAt)
    const isAccepted = m.taskingEvaluation === 'OK' && m.consensusEvaluation === 'MAJORITY_RESULT'

    debug('size=%s ttfb=%s duration=%s status=%s accepted? %s', byteLength, ttfb, duration, m.status_code, isAccepted)
    if (byteLength !== undefined && m.status_code === 200) {
      downloadBandwidth += byteLength
      sizeValues.push(byteLength)
    }
    if (ttfb !== undefined && ttfb > 0 && m.status_code === 200) ttfbValues.push(ttfb)
    if (duration !== undefined && duration > 0) durationValues.push(duration)

    const node = `${m.inet_group}::${m.participantAddress}`
    tasksPerNode.set(node, (tasksPerNode.get(node) ?? 0) + 1)

    if (isAccepted) {
      acceptedMeasurementsPerInetGroup.set(m.inet_group, (acceptedMeasurementsPerInetGroup.get(m.inet_group) ?? 0) + 1)
    }

    const taskId = getTaskId(m)
    if (m.indexerResult) tasksWithIndexerResults.add(taskId)
    if (m.indexerResult === 'OK') tasksWithHttpAdvertisement.add(taskId)

    // A successful HTTP response is a response with result breakdown set to OK and the protocol being used is set to HTTP.
    if (m.retrievalResult === 'OK' && m.protocol === 'http') { httpSuccesses++ }

    if (m.retrievalResult.match(/^IPNI_ERROR_5\d\d$/)) {
      indexerServerErrorCount++
    }
  }

  const totalForRSR = totalCount - indexerServerErrorCount
  const successRate = totalForRSR ? resultBreakdown.OK / (totalCount - indexerServerErrorCount) : 0
  const successRateHttp = totalForRSR ? httpSuccesses / (totalCount - indexerServerErrorCount) : 0
  telemetryPoint.intField('total_for_success_rates', totalForRSR)
  telemetryPoint.intField('unique_tasks', uniqueTasksCount)
  telemetryPoint.floatField('success_rate', successRate)
  telemetryPoint.floatField('success_rate_http', successRateHttp)
  telemetryPoint.intField('participants', participants.size)
  telemetryPoint.intField('inet_groups', inetGroups.size)
  telemetryPoint.intField('measurements', totalCount)
  telemetryPoint.intField('download_bandwidth', downloadBandwidth)

  addHistogramToPoint(telemetryPoint, ttfbValues, 'ttfb_')
  addHistogramToPoint(telemetryPoint, durationValues, 'duration_')
  addHistogramToPoint(telemetryPoint, sizeValues, 'car_size_')
  addHistogramToPoint(
    telemetryPoint,
    Array.from(tasksPerNode.values()),
    'tasks_per_node_'
  )

  for (const [result, count] of Object.entries(resultBreakdown)) {
    telemetryPoint.floatField(`result_rate_${result}`, count / totalCount)
  }

  for (const [result, count] of Object.entries(indexerResultBreakdown)) {
    telemetryPoint.floatField(`indexer_rate_${result}`, count / totalCount)
  }

  telemetryPoint.floatField('rate_of_deals_advertising_http', tasksWithIndexerResults.size > 0
    ? tasksWithHttpAdvertisement.size / tasksWithIndexerResults.size
    : 0
  )

  const allScores = Array.from(acceptedMeasurementsPerInetGroup.values())
  const totalScore = allScores.reduce((sum, val) => sum + val, 0)
  const nanoScoresPerInetGroup = allScores.map(s => Math.round(s * 1_000_000_000 / totalScore))
  addHistogramToPoint(telemetryPoint, nanoScoresPerInetGroup, 'nano_score_per_inet_group_')
}

/**
 * @param {{[key: string]: number}} breakdown
 * @param {string} result */
const updateBreakdown = (breakdown, result) => {
  const oldCount = breakdown[result] ?? 0
  breakdown[result] = oldCount + 1
}

/**
 *
 * @param {import('./typings.js').Point} point
 * @param {string} fieldNamePrefix
 * @param {number[]} values
 */
const addHistogramToPoint = (point, values, fieldNamePrefix = '') => {
  const count = values.length
  if (count < 1) return
  values.sort((a, b) => a - b)
  point.intField(`${fieldNamePrefix}min`, values[0])
  point.intField(`${fieldNamePrefix}mean`, values.reduce((sum, v) => sum + BigInt(v), 0n) / BigInt(count))
  point.intField(`${fieldNamePrefix}max`, values[count - 1])
  for (const p of [1, 5, 10, 50, 90, 95, 99]) {
    point.intField(`${fieldNamePrefix}p${p}`, getValueAtPercentile(values, p / 100))
  }
}

/**
 * @param {Pick<Measurement, 'cid' | 'minerId'>} m
 * @returns {string}
 */
export const getTaskId = (m) => `${m.cid}::${m.minerId}`

/**
 * @param {Measurement[]} measurements
 * @returns {number}
 */
const countUniqueTasks = (measurements) => {
  const uniqueTasks = new Set()
  for (const m of measurements) {
    const id = getTaskId(m)
    uniqueTasks.add(id)
  }

  return uniqueTasks.size
}

/**
 * @param {Iterable<Committee>} committees
 * @param {import('./typings.js').Point} point
 */
export const recordCommitteeSizes = (committees, point) => {
  /** @type {Map<string, {
   * subnets: Set<string>;
   * participants: Set<string>;
   * nodes: Set<string>;
   * measurements: number;
   * majoritySize: number | undefined;
   * }>} */
  const stats = new Map()
  for (const c of committees) {
    const key = getTaskId(c.retrievalTask)
    let data = stats.get(key)
    if (!data) {
      data = {
        subnets: new Set(),
        participants: new Set(),
        nodes: new Set(),
        measurements: 0,
        majoritySize: undefined
      }
      stats.set(key, data)
    }
    for (const m of c.measurements) {
      data.subnets.add(m.inet_group)
      data.participants.add(m.participantAddress)
      // We don't have Station instance identifier in the measurement.
      // The pair (inet_group, participant_address) is a good approximation.
      data.nodes.add(`${m.inet_group}::${m.participantAddress}`)
      data.measurements++

      if (m.taskingEvaluation === 'OK' && m.consensusEvaluation === 'MAJORITY_RESULT') {
        data.majoritySize = (data.majoritySize ?? 0) + 1
      }
    }
  }

  /** @type {Array<number>} */
  const subnetCounts = []
  /** @type {Array<number>} */
  const participantCounts = []
  /** @type {Array<number>} */
  const nodeCounts = []
  /** @type {Array<number>} */
  const measurementCounts = []
  /** @type {Array<number>} */
  const majorityToCommitteeRatios = []
  for (const { subnets, participants, nodes, measurements, majoritySize } of stats.values()) {
    subnetCounts.push(subnets.size)
    participantCounts.push(participants.size)
    nodeCounts.push(nodes.size)
    measurementCounts.push(measurements)

    // Ignore committees that are too small or with no majority
    if (majoritySize !== undefined) {
      assert(measurements > 0, 'if there is a majority, there have to be measurements')
      majorityToCommitteeRatios.push(Math.floor(100 * majoritySize / measurements))
    }
  }

  addHistogramToPoint(point, subnetCounts, 'subnets_')
  addHistogramToPoint(point, participantCounts, 'participants_')
  addHistogramToPoint(point, nodeCounts, 'nodes_')
  addHistogramToPoint(point, measurementCounts, 'measurements_')
  addHistogramToPoint(point, majorityToCommitteeRatios, 'majority_ratios_percents_')
}
