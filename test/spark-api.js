import assert from 'node:assert'
import { fetchRoundDetails } from '../lib/spark-api.js'

const recordTelemetry = (measurementName, fn) => { /* no-op */ }

describe('spark-api client', () => {
  it('fetches round details', async function () {
    this.timeout(10_000)
    const { retrievalTasks, maxTasksPerNode, ...details } = await fetchRoundDetails(
      '0x8460766edc62b525fc1fa4d628fc79229dc73031',
      12600n,
      recordTelemetry
    )

    assert.deepStrictEqual(details, {
      roundId: '18024', // BigInt serialized as String,
      startEpoch: '4158303'
    })

    assert.strictEqual(typeof maxTasksPerNode, 'number')

    assert.strictEqual(retrievalTasks.length, 1000)
    assert.deepStrictEqual(retrievalTasks.slice(0, 2), [
      {
        cid: 'bafkreia3oovvt7sws7wnz43zbr33lsu2yrdmx4mqswdumravjrnxfoxdka',
        minerId: 'f02228866',
        clients: ['f01990536'],
        allocators: null
      },
      {
        cid: 'bafkreibipuscsrko7tlrw62rttqvbma3qqqkksjoi6bhvwp27qaylwupp4',
        minerId: 'f02982293',
        clients: ['f03064945'],
        allocators: null
      }
    ])
  })
})
