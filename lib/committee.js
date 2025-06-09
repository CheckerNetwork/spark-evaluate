import assert from 'node:assert'
import createDebug from 'debug'
import { getTaskId } from './retrieval-stats.js'

/** @import {Measurement} from './preprocess.js' */
/** @import {RetrievalResult, ConsensusNotFoundReason} from './typings.js' */

const debug = createDebug('spark:committee')

/** @typedef {Map<string, Committee>} TaskIdToCommitteeMap */

/** @typedef {{
   indexMajorityFound: boolean;
   indexerResult: string | ConsensusNotFoundReason;
   retrievalMajorityFound: boolean;
   retrievalResult: RetrievalResult
 }} CommitteeDecision
 */
export class Committee {
  /** @type {Measurement[]} */
  #measurements

  /**
   * @param {Pick<Measurement, 'cid' | 'minerId'>} retrievalTask
   */
  constructor ({ cid, minerId }) {
    this.retrievalTask = { minerId, cid }

    this.#measurements = []

    /** @type {CommitteeDecision | undefined} */
    this.decision = undefined
  }

  get size () {
    return this.#measurements.length
  }

  get measurements () {
    const ret = [...this.#measurements]
    Object.freeze(ret)
    return ret
  }

  /**
   * @param {Measurement} m
   */
  addMeasurement (m) {
    // Only check that the measurement matches the committee's task (cid and minerId)
    assert.strictEqual(m.cid, this.retrievalTask.cid, 'cid must match')
    assert.strictEqual(m.minerId, this.retrievalTask.minerId, 'minerId must match')
    this.#measurements.push(m)
  }

  /**
   * @param {object} args
   * @param {number} args.requiredCommitteeSize
   * @returns
   */
  evaluate ({ requiredCommitteeSize }) {
    debug(
      'Evaluating task %o with a committee of %s measurements',
      this.retrievalTask,
      this.#measurements.length
    )

    if (this.#measurements.length < requiredCommitteeSize) {
      debug('→ committee is too small (size=%s required=%s); retrievalResult=COMMITTEE_TOO_SMALL',
        this.#measurements.length,
        requiredCommitteeSize
      )
      this.decision = {
        indexMajorityFound: false,
        indexerResult: 'COMMITTEE_TOO_SMALL',
        retrievalMajorityFound: false,
        retrievalResult: 'COMMITTEE_TOO_SMALL'
      }
      for (const m of this.#measurements) m.consensusEvaluation = 'COMMITTEE_TOO_SMALL'
      return
    }

    debug('- searching for majority in indexer results')
    /** @type {(keyof Measurement)[]} */
    const indexerResultProps = [
      'providerId',
      'indexerResult',
      'provider_address',
      'protocol'
    ]
    const indexerResultMajority = this.#findMajority(indexerResultProps)
    const indexMajorityFound = !!indexerResultMajority
    const indexerResult = indexerResultMajority
      ? indexerResultMajority.majorityValue.indexerResult
      : 'MAJORITY_NOT_FOUND'

    debug('- searching for majority in retrieval results')
    /** @type {(keyof Measurement)[]} */
    const retrievalResultProps = [
      ...indexerResultProps,
      // NOTE: We are not checking the fields that were used to calculate
      // the retrievalResult value:
      //  - status_code
      //  - timeout
      //  - car_too_large
      // If there is an agreement on the retrieval result, then those fields
      // must have the same value too.
      'retrievalResult',
      'byte_length',
      'carChecksum'
    ]

    const retrievalResultMajority = this.#findMajority(retrievalResultProps)
    const retrievalMajorityFound = !!retrievalResultMajority
    /** @type {CommitteeDecision['retrievalResult']} */
    let retrievalResult
    if (retrievalResultMajority) {
      retrievalResult = retrievalResultMajority.majorityValue.retrievalResult
      for (const m of retrievalResultMajority.majorityMeasurements) {
        m.consensusEvaluation = 'MAJORITY_RESULT'
      }
      for (const m of retrievalResultMajority.minorityMeasurements) {
        m.consensusEvaluation = 'MINORITY_RESULT'
      }
    } else {
      retrievalResult = 'MAJORITY_NOT_FOUND'
      for (const m of this.#measurements) m.consensusEvaluation = 'MAJORITY_NOT_FOUND'
    }

    this.decision = {
      indexMajorityFound,
      indexerResult,
      retrievalMajorityFound,
      retrievalResult
    }
  }

  /**
   * @param {(keyof Measurement)[]} measurementFields
   */
  #findMajority (measurementFields) {
    /** @param {Measurement} m */
    const getResult = m => pick(m, ...measurementFields)

    /** @type {Map<string, Measurement[]>} */
    const resultGroups = new Map()

    // 1. Group measurements using the result as the grouping key
    for (const m of this.#measurements) {
      const key = JSON.stringify(getResult(m))
      let list = resultGroups.get(key)
      if (!list) {
        list = []
        resultGroups.set(key, list)
      }
      list.push(m)
    }

    if (debug.enabled) {
      debug('- results found:')
      for (const k of resultGroups.keys()) {
        debug('  %o', JSON.parse(k))
      }
    }

    // 2. Sort the measurement groups by their size
    const keys = Array.from(resultGroups.keys())
    keys.sort(
      (a, b) => (resultGroups.get(a)?.length ?? 0) - (resultGroups.get(b)?.length ?? 0)
    )
    const measurementGroups = keys.map(k => resultGroups.get(k))

    // 3. Find the majority
    const majorityMeasurements = measurementGroups.pop()
    const majoritySize = majorityMeasurements.length
    const majorityValue = getResult(majorityMeasurements[0])

    debug('- majority=%s committee-size=%s value=%o', majoritySize, this.size, majorityValue)
    if (majoritySize <= this.size / 2) {
      debug('→ majority is not absolute; result=MAJORITY_NOT_FOUND')
      return undefined
    } else {
      debug('→ majority agrees on result=%o', majorityValue)
      return {
        majorityValue,
        majorityMeasurements,
        minorityMeasurements: measurementGroups.flat()
      }
    }
  }
}

/**
 * @template T
 * @template {keyof T} K
 * @param {T} obj
 * @param {K[]} keys
 * @returns {Pick<T, K>}
 */
function pick (obj, ...keys) {
  /** @type {any} */
  const ret = {}
  keys.forEach(key => {
    ret[key] = obj[key]
  })
  return ret
}

/**
 * @param {Iterable<Measurement>} measurements
 * @returns {TaskIdToCommitteeMap}
 */
export function groupMeasurementsToCommittees (measurements) {
  /** @type {TaskIdToCommitteeMap} */
  const committeesMap = new Map()
  for (const m of measurements) {
    const key = getTaskId(m)
    let c = committeesMap.get(key)
    if (!c) {
      c = new Committee(m)
      committeesMap.set(key, c)
    }
    c.addMeasurement(m)
  }
  return committeesMap
}
