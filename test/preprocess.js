import {
  getRetrievalResult,
  parseParticipantAddress,
  preprocess,
  Measurement,
  parseMeasurements,
  assertValidMeasurement
} from '../lib/preprocess.js'
import { Point } from '../lib/telemetry.js'
import assert from 'node:assert'
import createDebug from 'debug'
import { assertPointFieldValue, assertRecordedTelemetryPoint } from './helpers/assertions.js'
import { VALID_MEASUREMENT, VALID_STATION_ID } from './helpers/test-data.js'
import { RoundData } from '../lib/round.js'

const debug = createDebug('test')

const telemetry = []
const recordTelemetry = (measurementName, fn) => {
  const point = new Point(measurementName)
  fn(point)
  debug('recordTelemetry(%s): %o', measurementName, point.fields)
  telemetry.push(point)
}
beforeEach(() => telemetry.splice(0))

describe('preprocess', () => {
  it('fetches measurements', async () => {
    const round = new RoundData(0n)
    const cid = 'bafybeif2'
    const roundIndex = 0
    const measurements = [{
      participant_address: 'f410ftgmzttyqi3ti4nxbvixa4byql3o5d4eo3jtc43i',
      station_id: VALID_STATION_ID,
      spark_version: '1.2.3',
      inet_group: 'ig1',
      indexer_result: 'OK',
      finished_at: '2023-11-01T09:00:00.000Z',
      first_byte_at: '2023-11-01T09:00:01.000Z',
      start_at: '2023-11-01T09:00:02.000Z',
      end_at: '2023-11-01T09:00:03.000Z'
    }]
    const getCalls = []
    const fetchMeasurements = async (cid) => {
      getCalls.push(cid)
      return measurements
    }
    const logger = { log: debug, error: console.error }
    await preprocess({ round, cid, roundIndex, fetchMeasurements, recordTelemetry, logger })

    assert.deepStrictEqual(round.measurements, [
      new Measurement({
        participant_address: '0x999999cf1046e68e36E1aA2E0E07105eDDD1f08E',
        station_id: VALID_STATION_ID,
        spark_version: '1.2.3',
        inet_group: 'ig1',
        indexer_result: 'OK',
        finished_at: '2023-11-01T09:00:00.000Z',
        first_byte_at: '2023-11-01T09:00:01.000Z',
        start_at: '2023-11-01T09:00:02.000Z',
        end_at: '2023-11-01T09:00:03.000Z'
      })
    ])
    assert.deepStrictEqual(getCalls, [cid])
    assert.deepStrictEqual(round.measurementBatches, [cid])

    const point = assertRecordedTelemetryPoint(telemetry, 'spark_versions')
    assertPointFieldValue(point, 'round_index', '0i')
    assertPointFieldValue(point, 'v1.2.3', '1i')
    assertPointFieldValue(point, 'total', '1i')
  })

  it('converts mainnet wallet address to participant ETH address', () => {
    const converted = parseParticipantAddress('f410ftgmzttyqi3ti4nxbvixa4byql3o5d4eo3jtc43i')
    assert.strictEqual(converted, '0x999999cf1046e68e36E1aA2E0E07105eDDD1f08E')
  })

  it('converts testnet wallet address to participant ETH address', () => {
    const converted = parseParticipantAddress('t410ftgmzttyqi3ti4nxbvixa4byql3o5d4eo3jtc43i')
    assert.strictEqual(converted, '0x999999cf1046e68e36E1aA2E0E07105eDDD1f08E')
  })

  it('accepts ETH 0x address', () => {
    const converted = parseParticipantAddress('0x3356fd7D01F001f5FdA3dc032e8bA14E54C2a1a1')
    assert.strictEqual(converted, '0x3356fd7D01F001f5FdA3dc032e8bA14E54C2a1a1')
  })
})

describe('parseMeasurements', () => {
  const measurements = [{ foo: 'bar' }, { beep: 'boop' }]
  it('parses a JSON array', () => {
    assert.deepStrictEqual(
      parseMeasurements(JSON.stringify(measurements)),
      measurements
    )
  })
  it('parses NDJSON', () => {
    assert.deepStrictEqual(
      parseMeasurements(measurements.map(m => JSON.stringify(m)).join('\n')),
      measurements
    )
  })
})

describe('getRetrievalResult', () => {
  /** @type {Partial<import('../lib/typings.js').RawMeasurement>} */
  const SUCCESSFUL_RETRIEVAL = {
    spark_version: '1.5.2',
    participant_address: 'f410fgkhpcrbmdvic52o3nivftrjxr7nzw47updmuzra',
    station_id: VALID_STATION_ID,
    finished_at: '2023-11-01T09:42:03.246Z',
    timeout: false,
    start_at: '2023-11-01T09:40:03.393Z',
    status_code: 200,
    first_byte_at: '1970-01-01T00:00:00.000Z',
    end_at: '1970-01-01T00:00:00.000Z',
    byte_length: 1234,
    inet_group: 'ue49TX_JdYjI',
    cid: 'bafkreihstuf2qcu3hs64ersidh46cjtilxcoipmzgu3pifwzmkqdjpraqq',
    miner_id: 'f1abc',
    provider_address: '/ip4/108.89.91.150/tcp/46717/p2p/12D3KooWSsaFCtzDJUEhLQYDdwoFtdCMqqfk562UMvccFz12kYxU',
    provider_id: 'PROVIDERID',
    protocol: 'http',
    indexer_result: 'OK'
  }

  it('successful retrieval', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL
    })
    assert.strictEqual(result, 'OK')
  })

  it('TIMEOUT', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      timeout: true
    })
    assert.strictEqual(result, 'TIMEOUT')
  })

  it('CAR_TOO_LARGE', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      car_too_large: true
    })
    assert.strictEqual(result, 'CAR_TOO_LARGE')
  })

  it('HTTP_502 (Bad Gateway)', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      protocol: 'http',
      status_code: 502
    })
    assert.strictEqual(result, 'HTTP_502')
  })

  it('LASSIE_502 (Bad Gateway)', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      protocol: 'graphsync',
      status_code: 502
    })
    assert.strictEqual(result, 'LASSIE_502')
  })

  it('HTTP_504 (Gateway Timeout)', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      protocol: 'http',
      status_code: 504
    })
    assert.strictEqual(result, 'HTTP_504')
  })

  it('LASSIE_504 (Gateway Timeout)', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      protocol: 'graphsync',
      status_code: 504
    })
    assert.strictEqual(result, 'LASSIE_504')
  })

  it('SERVER_ERROR - 500 (over HTTP)', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      status_code: 500
    })
    assert.strictEqual(result, 'HTTP_500')
  })

  it('SERVER_ERROR - 503 (over HTTP)', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      status_code: 503
    })
    assert.strictEqual(result, 'HTTP_503')
  })

  it('UNKNOWN_ERROR - missing end_at', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      end_at: undefined
    })
    assert.strictEqual(result, 'UNKNOWN_ERROR')
  })

  it('UNKNOWN_ERROR - status_code is null', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      timeout: false,
      status_code: null
    })
    assert.strictEqual(result, 'UNKNOWN_ERROR')
  })

  it('IPNI HTTP_NOT_ADVERTISED -> OK', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      indexer_result: 'HTTP_NOT_ADVERTISED'
    })
    assert.strictEqual(result, 'OK')
  })

  it('IPNI errors', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      indexer_result: 'ERROR_FETCH'
    })
    assert.strictEqual(result, 'IPNI_ERROR_FETCH')
  })

  for (const code of [701, 702, 703, 704]) {
    it(`UNSUPPORTED_MULTIADDR_FORMAT - ${code}`, () => {
      const result = getRetrievalResult({
        ...SUCCESSFUL_RETRIEVAL,
        status_code: code
      })
      assert.strictEqual(result, 'UNSUPPORTED_MULTIADDR_FORMAT')
    })
  }

  it('UNKNOWN_FETCH_ERROR - 600', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      status_code: 600
    })
    assert.strictEqual(result, 'UNKNOWN_FETCH_ERROR')
  })

  it('HOSTNAME_DNS_ERROR - 801', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      status_code: 801
    })
    assert.strictEqual(result, 'HOSTNAME_DNS_ERROR')
  })

  it('HOSTNAME_DNS_ERROR - 802', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      status_code: 802
    })
    assert.strictEqual(result, 'CONNECTION_REFUSED')
  })

  it('UNSUPPORTED_CID_HASH_ALGO - 901', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      status_code: 901
    })
    assert.strictEqual(result, 'UNSUPPORTED_CID_HASH_ALGO')
  })

  it('CONTENT_VERIFICATION_FAILED - 902', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      status_code: 902
    })
    assert.strictEqual(result, 'CONTENT_VERIFICATION_FAILED')
  })

  it('UNEXPECTED_CAR_BLOCK - 903', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      status_code: 903
    })
    assert.strictEqual(result, 'UNEXPECTED_CAR_BLOCK')
  })

  it('CANNOT_PARSE_CAR_FILE - 904', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      status_code: 904
    })
    assert.strictEqual(result, 'CANNOT_PARSE_CAR_FILE')
  })
})

describe('assertValidMeasurement', () => {
  it('rejects measurements where indexer_result is null', () => {
    const m = {
      ...VALID_MEASUREMENT,
      indexerResult: null
    }
    assert.throws(
      () => assertValidMeasurement(m),
      /field `indexerResult` must be set/
    )
  })

  it('should throw an error for invalid start_at', () => {
    const measurement = {
      ...VALID_MEASUREMENT,
      start_at: -1,
      first_byte_at: 1672531201000,
      end_at: 1672531202000
    }
    assert.throws(() => assertValidMeasurement(measurement), /field `start_at` must be a number greater than 0/)
  })

  it('should throw an error for end_at set to 0', () => {
    const measurement = {
      ...VALID_MEASUREMENT,
      start_at: 1672531200000,
      first_byte_at: 1672531201000,
      end_at: 0
    }
    assert.throws(() => assertValidMeasurement(measurement), /field `end_at` must be a number greater than 0/)
  })

  it('should throw an error for first_byte_at set to 0', () => {
    const measurement = {
      ...VALID_MEASUREMENT,
      start_at: 1672531200000,
      first_byte_at: 0,
      end_at: 1672531202000
    }
    assert.throws(() => assertValidMeasurement(measurement), /field `first_byte_at` must be a number greater than 0/)
  })

  it('should throw an error for start_at greater than end_at', () => {
    const measurement = {
      ...VALID_MEASUREMENT,
      start_at: 1672531203000, // Timestamp for 2023-01-01T00:00:03Z
      first_byte_at: 1672531201000,
      end_at: 1672531202000 // Timestamp for 2023-01-01T00:00:02Z
    }
    assert.throws(() => assertValidMeasurement(measurement), /end_at must be greater than or equal to start_at/)
  })

  it('should throw an error for first_byte_at greater than end_at', () => {
    const measurement = {
      ...VALID_MEASUREMENT,
      start_at: 1672531200000,
      first_byte_at: 1672531203000, // Timestamp for 2023-01-01T00:00:03Z
      end_at: 1672531202000 // Timestamp for 2023-01-01T00:00:02Z
    }
    assert.throws(() => assertValidMeasurement(measurement), /end_at must be greater than or equal to first_byte_at/)
  })

  it('should throw an error for first_byte_at less than start_at', () => {
    const measurement = {
      ...VALID_MEASUREMENT,
      start_at: 1672531201000, // Timestamp for 2023-01-01T00:00:01Z
      first_byte_at: 1672531200000, // Timestamp for 2023-01-01T00:00:00Z
      end_at: 1672531202000
    }
    assert.throws(() => assertValidMeasurement(measurement), /first_byte_at must be greater than or equal to start_at/)
  })
})
