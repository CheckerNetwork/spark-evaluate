import { parseParticipantAddress, preprocess } from '../lib/preprocess.js'
import assert from 'node:assert'
import createDebug from 'debug'

const debug = createDebug('test')

const recordTelemetry = (measurementName, fn) => {
  /* no-op */
  debug('recordTelemetry(%s)', measurementName)
}

describe('preprocess', () => {
  it('fetches measurements', async () => {
    const rounds = {}
    const cid = 'bafybeif2'
    const roundIndex = 0
    const measurements = [{
      participant_address: 'f410ftgmzttyqi3ti4nxbvixa4byql3o5d4eo3jtc43i',
      inet_group: 'ig1',
      finished_at: '2023-11-01T09:00.00.000Z'
    }]
    const getCalls = []
    const fetchMeasurements = async (cid) => {
      getCalls.push(cid)
      return measurements
    }
    const logger = { log: debug, error: console.error }
    await preprocess({ rounds, cid, roundIndex, fetchMeasurements, recordTelemetry, logger })

    assert.deepStrictEqual(rounds, {
      0: [{
        participantAddress: '0x999999cf1046e68e36E1aA2E0E07105eDDD1f08E',
        inet_group: 'ig1',
        finished_at: '2023-11-01T09:00.00.000Z'
      }]
    })
    assert.deepStrictEqual(getCalls, [cid])
  })
  it('validates measurements', async () => {
    const rounds = {}
    const cid = 'bafybeif2'
    const roundIndex = 0
    const measurements = [{
      participant_address: 't1foobar',
      inet_group: 'ig1',
      finished_at: '2023-11-01T09:00.00.000Z'
    }]
    const fetchMeasurements = async (_cid) => measurements
    const logger = { log: debug, error: debug }
    await preprocess({ rounds, cid, roundIndex, fetchMeasurements, recordTelemetry, logger })
    // We allow invalid participant address for now.
    // We should update this test when we remove this temporary workaround.
    assert.deepStrictEqual(rounds, {
      0: [{
        participantAddress: '0x000000000000000000000000000000000000dEaD',
        inet_group: 'ig1',
        finished_at: '2023-11-01T09:00.00.000Z'
      }]
    })
  })

  it('converts mainnet wallet address to participant ETH address', () => {
    const converted = parseParticipantAddress('f410ftgmzttyqi3ti4nxbvixa4byql3o5d4eo3jtc43i')
    assert.strictEqual(converted, '0x999999cf1046e68e36E1aA2E0E07105eDDD1f08E')
  })

  it('converts testnet wallet address to participant ETH address', () => {
    const converted = parseParticipantAddress('t410ftgmzttyqi3ti4nxbvixa4byql3o5d4eo3jtc43i')
    assert.strictEqual(converted, '0x999999cf1046e68e36E1aA2E0E07105eDDD1f08E')
  })

  it('converts mainnet f1 wallet address to hard-coded participant ETH adddress', () => {
    const converted = parseParticipantAddress('f17uoq6tp427uzv7fztkbsnn64iwotfrristwpryy')
    assert.strictEqual(converted, '0x000000000000000000000000000000000000dEaD')
  })

  it('converts testnet f1 wallet address to hard-coded participant ETH adddress', () => {
    const converted = parseParticipantAddress('t17uoq6tp427uzv7fztkbsnn64iwotfrristwpryy')
    assert.strictEqual(converted, '0x000000000000000000000000000000000000dEaD')
  })

  it('accepts ETH 0x address', () => {
    const converted = parseParticipantAddress('0x3356fd7D01F001f5FdA3dc032e8bA14E54C2a1a1')
    assert.strictEqual(converted, '0x3356fd7D01F001f5FdA3dc032e8bA14E54C2a1a1')
  })
})
