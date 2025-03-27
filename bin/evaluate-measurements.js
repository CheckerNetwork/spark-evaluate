import fs from 'node:fs'
import { readFile } from 'node:fs/promises'
import { RoundData } from '../lib/round.js'
import { evaluate } from '../lib/evaluate.js'
import * as SparkImpactEvaluator from '@filecoin-station/spark-impact-evaluator'
import { fetchRoundDetails } from '../lib/spark-api.js'
import createDebug from 'debug'
import { Point } from '@influxdata/influxdb-client'
import { basename } from 'node:path'

const { KEEP_REJECTED } = process.env

const debug = createDebug('spark:bin')

const [nodePath, selfPath, measurementsPath] = process.argv

const USAGE = `
Usage:
  ${nodePath} ${selfPath} measurementsPath
`

if (!measurementsPath) {
  console.error('Missing required argument: measurementsPath')
  console.error(USAGE)
  process.exit(1)
}

const keepRejected = isFlagEnabled(KEEP_REJECTED)

const rounds = new Map()
const measurementsFile = await readFile(measurementsPath, 'utf8')
for (const line of measurementsFile.split('\n').filter(Boolean)) {
  const { roundIndex: _roundIndex, measurement } = JSON.parse(line)
  const roundIndex = BigInt(_roundIndex)
  if (!rounds.has(roundIndex)) rounds.set(roundIndex, [])
  rounds.get(roundIndex).push(measurement)
}

const EVALUATION_TXT_FILE = `${basename(measurementsPath, '.ndjson')}.evaluation.txt`
const EVALUATION_NDJSON_FILE = `${basename(measurementsPath, '.ndjson')}.evaluation.ndjson`

const evaluationTxtWriter = fs.createWriteStream(EVALUATION_TXT_FILE)
const evaluationNdjsonWriter = fs.createWriteStream(EVALUATION_NDJSON_FILE)

evaluationTxtWriter.write(formatHeader({ keepRejected }) + '\n')

const resultCounts = {
  total: 0
}

for (const [roundIndex, measurements] of rounds) {
  await processRound(
    roundIndex,
    measurements,
    resultCounts
  )
}

console.log('Found %s accepted measurements.', resultCounts.total)
for (const [r, c] of Object.entries(resultCounts)) {
  if (r === 'total') continue
  console.log('  %s %s (%s%)',
    r.padEnd(40),
    String(c).padEnd(10),
    Math.floor(c / resultCounts.total * 10000) / 100
  )
}

console.error('Wrote   human-readable evaluation to %s', EVALUATION_TXT_FILE)
console.error('Wrote machine-readable evaluation to %s', EVALUATION_NDJSON_FILE)

/**
 * @param {bigint} roundIndex
 * @param {object[]} measurements
 * @param {Record<string, number>} resultCounts
 */
async function processRound (roundIndex, measurements, resultCounts) {
  console.error(' → evaluating round %s', roundIndex)

  const round = new RoundData(roundIndex)
  round.measurements = measurements

  const ieContract = {
    async getAddress () {
      return SparkImpactEvaluator.ADDRESS
    }
  }

  await evaluate({
    roundIndex: round.index,
    round,
    fetchRoundDetails,
    recordTelemetry,
    logger: { log: debug, error: debug },
    ieContract,
    setScores: async () => {},
    prepareProviderRetrievalResultStats: async () => {}
  })

  if (!keepRejected) {
    round.measurements = round.measurements.filter(m => m.taskingEvaluation === 'OK')
  }

  for (const m of round.measurements) {
    if (m.taskingEvaluation !== 'OK') continue
    resultCounts.total++
    const status = m.consensusEvaluation !== 'MAJORITY_RESULT'
      ? m.consensusEvaluation
      : m.retrievalResult
    resultCounts[status] = (resultCounts[status] ?? 0) + 1
  }

  evaluationTxtWriter.write(
    round.measurements
      .map(m => formatMeasurement(m, { keepRejected }) + '\n')
      .join('')
  )
  evaluationNdjsonWriter.write(
    round.measurements
      .map(m => JSON.stringify(m) + '\n')
      .join('')
  )
  console.error(' → added %s accepted measurements from this round', round.measurements.length)
}

/**
 * @param {string} measurementName
 * @param {(point: Point) => void} fn
 */
function recordTelemetry (measurementName, fn) {
  const point = new Point(measurementName)
  fn(point)
  debug('TELEMETRY %s %o', measurementName, point.fields)
}

/**
 * @param {string | undefined} envVarValue
 */
function isFlagEnabled (envVarValue) {
  return !!envVarValue && envVarValue.toLowerCase() !== 'false' && envVarValue !== '0'
}

/**
 * @param {import('../lib/preprocess.js').Measurement} m
 * @param {object} options
 * @param {boolean} [options.keepRejected]
 */
function formatMeasurement (m, { keepRejected } = {}) {
  const fields = [
    new Date(m.finished_at).toISOString(),
    (m.cid ?? '').padEnd(70),
    (m.protocol ?? '').padEnd(10)
  ]

  if (keepRejected) {
    fields.push((m.taskingEvaluation === 'OK' ? '🫡' : '🙅').padEnd(7))
  }

  fields.push((m.consensusEvaluation === 'MAJORITY_RESULT' ? '✅' : '❌').padEnd(9))
  fields.push((m.retrievalResult ?? ''))

  return fields.join(' ')
}

/**
 * @param {object} options
 * @param {boolean} [options.keepRejected]
 */
function formatHeader ({ keepRejected } = {}) {
  const fields = [
    'Timestamp'.padEnd(new Date().toISOString().length),
    'CID'.padEnd(70),
    'Protocol'.padEnd(10)
  ]

  if (keepRejected) {
    fields.push('Tasking')
  }

  fields.push('Consensus')
  fields.push('RetrievalResult')

  return fields.join(' ')
}
