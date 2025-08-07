// dotenv must be imported before importing anything else
import 'dotenv/config'

import { DATABASE_URL } from '../lib/config.js'
import { evaluate } from '../lib/evaluate.js'
import { preprocess, fetchMeasurements } from '../lib/preprocess.js'
import { fetchRoundDetails } from '../lib/spark-api.js'
import { Point } from '../lib/telemetry.js'
import { readFile, writeFile, mkdir } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import pg from 'pg'
import { RoundData } from '../lib/round.js'
import { createContracts } from '../lib/contracts.js'
import * as SparkImpactEvaluator from '@filecoin-station/spark-impact-evaluator'

/** @typedef {import('../lib/preprocess.js').Measurement} Measurement */

const {
  DUMP
} = process.env

const cacheDir = fileURLToPath(new URL('../.cache', import.meta.url))
await mkdir(cacheDir, { recursive: true })

const args = process.argv.slice(2)
if (args.length === 0 || !args[0].startsWith('0x')) {
  args.unshift(SparkImpactEvaluator.ADDRESS)
}
const [contractAddress, roundIndexStr, ...measurementCids] = args

/** @type {bigint} */
let roundIndex
if (roundIndexStr) {
  roundIndex = BigInt(roundIndexStr)
} else {
  console.log('Round index not specified, fetching the last round index from the smart contract', contractAddress)
  const currentRoundIndex = await fetchLastRoundIndex(contractAddress)
  roundIndex = BigInt(currentRoundIndex - 2n)
}

if (!measurementCids.length) {
  measurementCids.push(...(await fetchMeasurementsAddedEvents(contractAddress, roundIndex)))
}

if (!measurementCids.length) {
  console.error('No measurements for round %s were found in smart-contract\'s event log.', roundIndex)
  process.exit(1)
}

const recordTelemetry = (measurementName, fn) => {
  const point = new Point(measurementName)
  fn(point)
  console.log('TELEMETRY %s %o', measurementName, point.fields)
}

const createPgClient = async () => {
  const pgClient = new pg.Client({ connectionString: DATABASE_URL })
  await pgClient.connect()
  return pgClient
}

const fetchMeasurementsWithCache = async (cid) => {
  const pathOfCachedResponse = path.join(cacheDir, cid + '.json')
  try {
    return JSON.parse(await readFile(pathOfCachedResponse, 'utf-8'))
  } catch (err) {
    if (err.code !== 'ENOENT') console.warn('Cannot read cached measurements:', err)
  }

  const measurements = await fetchMeasurements(cid)
  await writeFile(pathOfCachedResponse, JSON.stringify(measurements))
  return measurements
}

console.log('Evaluating round %s of contract %s', roundIndex, contractAddress)

console.log('==PREPROCESS==')
const round = new RoundData(roundIndex)
for (const cid of measurementCids) {
  try {
    await preprocess({
      roundIndex,
      round,
      cid,
      fetchMeasurements: fetchMeasurementsWithCache,
      recordTelemetry,
      logger: console,
      fetchRetries: 0
    })
  } catch (err) {
    console.error(err)
  }
}

console.log('Fetched %s measurements', round.measurements.length)

console.log('==EVALUATE==')
const ieContract = {
  async getAddress () {
    return contractAddress
  }
}
const started = Date.now()
const { ignoredErrors } = await evaluate({
  roundIndex,
  round,
  fetchRoundDetails,
  ieContract,
  logger: console,
  recordTelemetry,
  createPgClient,
  prepareProviderRetrievalResultStats: async () => {}
})

console.log('Duration: %sms', Date.now() - started)
console.log(process.memoryUsage())

if (ignoredErrors.length) {
  console.log('**ERRORS**')
  for (const err of ignoredErrors) {
    console.log()
    console.log(err)
  }
  process.exit(1)
}

if (DUMP) {
  const props = ['cid', 'minerId', 'participantAddress', 'inet_group', 'retrievalResult', 'taskingEvaluation', 'consensusEvaluation']
  for (const k of Object.keys(round.measurements[0])) {
    if (!props.includes(k)) props.push(k)
  }

  const formatPropValue = (name, value) => {
    return name.endsWith('_at') && value ? new Date(value).toISOString() : value
  }
  const formatAsCsv = (/** @type {Measurement} */ m) => props.map(p => formatPropValue(p, m[p])).join(',')

  let measurements = round.measurements
  let fileSuffix = 'all'
  if (DUMP.startsWith('f0')) {
    const minerId = DUMP.toLowerCase()
    console.log('Storing measurements for miner id %s', minerId)
    measurements = measurements.filter(m => m.minerId.toLowerCase() === minerId)
    fileSuffix = DUMP // preserve the letter casing provided by the user
  } else if (DUMP.startsWith('0x')) {
    const participantAddress = DUMP.toLowerCase()
    console.log('Storing measurements from participant address', participantAddress)
    measurements = measurements.filter(m => m.participantAddress === participantAddress)
    fileSuffix = DUMP // preserve the letter casing provided by the user
  }

  const outfile = `measurements-${roundIndex}-${fileSuffix}.csv`
  await writeFile(
    outfile,
    props.join(',') + '\n' + measurements.map(formatAsCsv).join('\n') + '\n'
  )
  console.log('Evaluated measurements saved to %s', outfile)
}

/**
 * @param {string} contractAddress
 * @param {bigint} roundIndex
 * @returns {Promise<string[]>}
 */
async function fetchMeasurementsAddedEvents (contractAddress, roundIndex) {
  const pathOfCachedResponse = path.join(cacheDir, `round-${contractAddress}-${roundIndex}.json`)
  try {
    return JSON.parse(await readFile(pathOfCachedResponse, 'utf-8'))
  } catch (err) {
    if (err.code !== 'ENOENT') console.warn('Cannot read cached list of measurement CIDs:', err)
  }

  const list = await fetchMeasurementsAddedFromChain(contractAddress, roundIndex)
  await writeFile(pathOfCachedResponse, JSON.stringify(list))
  return list
}

/**
 * @param {string} contractAddress
 * @param {bigint} roundIndex
 */
async function fetchMeasurementsAddedFromChain (contractAddress, roundIndex) {
  const { ieContract, provider } = createContracts(contractAddress)

  console.log('Fetching MeasurementsAdded events from the ledger')

  const blockNumber = await provider.getBlockNumber()
  // console.log('Current block number', blockNumber)

  // TODO: filter only measurements for the given `roundIndex`
  // See https://github.com/Meridian-IE/impact-evaluator/issues/57

  // max look-back period allowed by Glif.io is 2000 blocks (approx 16h40m)
  // SPARK round is ~60 minutes, i.e. ~120 blocks
  const rawEvents = await ieContract.queryFilter('MeasurementsAdded', blockNumber - 1800, 'latest')

  /** @type {Array<{ cid: string, roundIndex: bigint, sender: string }>} */
  const events = rawEvents
    .filter(isEventLog)
    .map(({ args: [cid, roundIndex, sender] }) => ({ cid, roundIndex, sender }))
  // console.log('events', events)

  const prev = roundIndex - 1n
  const prevFound = events.some(e => e.roundIndex === prev)
  if (!prevFound) {
    console.error(
      'Incomplete round data. No measurements from the previous round %s were found.',
      prev.toString()
    )
    process.exit(1)
  }

  const next = roundIndex + 1n
  const nextFound = events.some(e => e.roundIndex === next)
  if (!nextFound) {
    console.error(
      'Incomplete round data. No measurements from the next round %s were found.',
      next.toString()
    )
    process.exit(1)
  }

  return events.filter(e => e.roundIndex === roundIndex).map(e => e.cid)
}

/**
 * @param {import('ethers').Log | import('ethers').EventLog} logOrEventLog
 * @returns {logOrEventLog is import('ethers').EventLog}
 */
function isEventLog (logOrEventLog) {
  return 'args' in logOrEventLog
}

/**
 * @param {string} contractAddress
 */
async function fetchLastRoundIndex (contractAddress) {
  const { ieContract } = await createContracts(contractAddress)
  return await ieContract.currentRoundIndex()
}
