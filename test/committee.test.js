import assert from 'node:assert'
import { VALID_TASK, VALID_MEASUREMENT as VALID_MEASUREMENT_BEFORE_ASSESSMENT } from './helpers/test-data.js'
import { Committee } from '../lib/committee.js'

/** @import {Measurement} from '../lib/preprocess.js' */

/** @type {Measurement} */
const VALID_MEASUREMENT = {
  ...VALID_MEASUREMENT_BEFORE_ASSESSMENT,
  fraudAssessment: 'OK'
}
Object.freeze(VALID_MEASUREMENT)

describe('Committee', () => {
  describe('evaluate', () => {
    it('produces OK result when the absolute majority agrees', () => {
      const c = new Committee(VALID_TASK)
      c.measurements.push({ ...VALID_MEASUREMENT, retrievalResult: 'OK' })
      c.measurements.push({ ...VALID_MEASUREMENT, retrievalResult: 'OK' })
      // minority result
      c.measurements.push({ ...VALID_MEASUREMENT, retrievalResult: 'CONTENT_VERIFICATION_FAILED' })

      c.evaluate(2)

      assert.strictEqual(c.retrievalResult, 'OK')
      // TODO, we are not there yet
      // assert.strictEqual(c.indexerResult, 'OK')
      assert.deepStrictEqual(c.measurements.map(m => m.fraudAssessment), [
        'OK',
        'OK',
        'MINORITY_RESULT'
      ])
    })

    it('rejects committees that are too small', () => {
      const c = new Committee(VALID_TASK)
      c.measurements.push({ ...VALID_MEASUREMENT })
      c.evaluate(10)
      assert.strictEqual(c.retrievalResult, 'COMMITTEE_TOO_SMALL')
      assert.strictEqual(c.measurements[0].fraudAssessment, 'COMMITTEE_TOO_SMALL')
    })

    it('rejects committees without absolute majority for providerId', () => {
      const c = new Committee(VALID_TASK)
      c.measurements.push({ ...VALID_MEASUREMENT, providerId: 'pubkey1' })
      c.measurements.push({ ...VALID_MEASUREMENT, providerId: 'pubkey2' })
      c.measurements.push({ ...VALID_MEASUREMENT, providerId: 'pubkey3' })

      c.evaluate(2)

      assert.strictEqual(c.retrievalResult, 'MAJORITY_NOT_FOUND')
      assert.deepStrictEqual(c.measurements.map(m => m.fraudAssessment), [
        'MAJORITY_NOT_FOUND',
        'MAJORITY_NOT_FOUND',
        'MAJORITY_NOT_FOUND'
      ])
    })

    it('finds majority for providerId', () => {
      const c = new Committee(VALID_TASK)
      c.measurements.push({ ...VALID_MEASUREMENT, providerId: 'pubkey1' })
      c.measurements.push({ ...VALID_MEASUREMENT, providerId: 'pubkey1' })
      // minority result
      c.measurements.push({ ...VALID_MEASUREMENT, providerId: 'pubkey3' })

      c.evaluate(2)

      assert.strictEqual(c.retrievalResult, 'OK')
      assert.deepStrictEqual(c.measurements.map(m => m.fraudAssessment), [
        'OK',
        'OK',
        'MINORITY_RESULT'
      ])
    })

    it('rejects committees without absolute majority for retrievalResult', () => {
      const c = new Committee(VALID_TASK)
      c.measurements.push({ ...VALID_MEASUREMENT, retrievalResult: 'OK' })
      c.measurements.push({ ...VALID_MEASUREMENT, retrievalResult: 'IPNI_ERROR_404' })
      c.measurements.push({ ...VALID_MEASUREMENT, retrievalResult: 'ERROR_502' })

      c.evaluate(2)

      assert.strictEqual(c.retrievalResult, 'MAJORITY_NOT_FOUND')
      assert.deepStrictEqual(c.measurements.map(m => m.fraudAssessment), [
        'MAJORITY_NOT_FOUND',
        'MAJORITY_NOT_FOUND',
        'MAJORITY_NOT_FOUND'
      ])
    })

    it('finds majority for retrievalResult', () => {
      const c = new Committee(VALID_TASK)
      c.measurements.push({ ...VALID_MEASUREMENT, retrievalResult: 'CONTENT_VERIFICATION_FAILED' })
      c.measurements.push({ ...VALID_MEASUREMENT, retrievalResult: 'CONTENT_VERIFICATION_FAILED' })
      // minority result
      c.measurements.push({ ...VALID_MEASUREMENT, retrievalResult: 'OK' })

      c.evaluate(2)

      assert.strictEqual(c.retrievalResult, 'CONTENT_VERIFICATION_FAILED')
      assert.deepStrictEqual(c.measurements.map(m => m.fraudAssessment), [
        'OK',
        'OK',
        'MINORITY_RESULT'
      ])
    })

    it('rejects committees without absolute majority for indexerResult', () => {
      const c = new Committee(VALID_TASK)
      c.measurements.push({ ...VALID_MEASUREMENT, indexerResult: 'OK' })
      c.measurements.push({ ...VALID_MEASUREMENT, indexerResult: 'ERROR_404' })
      c.measurements.push({ ...VALID_MEASUREMENT, indexerResult: 'HTTP_NOT_ADVERTISED' })

      c.evaluate(2)

      assert.strictEqual(c.indexerResult, 'MAJORITY_NOT_FOUND')
      assert.strictEqual(c.retrievalResult, 'MAJORITY_NOT_FOUND')
      assert.deepStrictEqual(c.measurements.map(m => m.fraudAssessment), [
        'MAJORITY_NOT_FOUND',
        'MAJORITY_NOT_FOUND',
        'MAJORITY_NOT_FOUND'
      ])
    })

    it('finds majority for indexerResult', () => {
      const c = new Committee(VALID_TASK)
      c.measurements.push({ ...VALID_MEASUREMENT, indexerResult: 'HTTP_NOT_ADVERTISED' })
      c.measurements.push({ ...VALID_MEASUREMENT, indexerResult: 'HTTP_NOT_ADVERTISED' })
      // minority result
      c.measurements.push({ ...VALID_MEASUREMENT, indexerResult: 'OK' })

      c.evaluate(2)

      assert.strictEqual(c.indexerResult, 'HTTP_NOT_ADVERTISED')
      assert.strictEqual(c.retrievalResult, 'OK')
      assert.deepStrictEqual(c.measurements.map(m => m.fraudAssessment), [
        'OK',
        'OK',
        'MINORITY_RESULT'
      ])
    })
  })
})
