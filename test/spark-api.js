import assert from 'node:assert'
import { fetchRoundDetails } from '../lib/spark-api.js'

const recordTelemetry = (measurementName, fn) => { /* no-op */ }

describe('spark-api client', () => {
  it('fetches round details', async function () {
    this.timeout(10_000)
    const tests = [
      {
        contractAddress: '0x8460766edc62b525fc1fa4d628fc79229dc73031',
        roundIndex: 12600n,
        expectedTaskCount: 1000,
        expectedRoundDetails: {
          roundId: '18024', // BigInt serialized as String,
          startEpoch: '4158303'
        },
        expectedTasks: [
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
        ]
      },
      {
        contractAddress: '0x8460766edc62b525fc1fa4d628fc79229dc73031',
        roundIndex: 27774n,
        expectedRoundDetails: {
          roundId: '33198',
          startEpoch: '4799802'
        },
        expectedTaskCount: 666,
        expectedTasks: [
          {
            cid: 'bafk2bzacea2fqjim35ouwqh4cjlxf7nov4qgc65ufu2nzpxuhv5utdqy54jne',
            minerId: 'f03233333',
            clients: ['f03493383'],
            allocators: ['f03019261']
          },
          {
            cid: 'bafk2bzaceaqybia3guy7szzvgkoshgmxdxknlm4hdyp3mb466tmwq264ky3ec',
            minerId: 'f03408862',
            clients: ['f03266237'],
            allocators: ['f03018489']
          }
        ]
      }
    ]

    for (const test of tests) {
      const { retrievalTasks, maxTasksPerNode, ...details } = await fetchRoundDetails(
        test.contractAddress,
        test.roundIndex,
        recordTelemetry
      )

      assert.strictEqual(typeof details.roundId, 'string')
      assert.strictEqual(typeof details.startEpoch, 'string')
      assert.deepStrictEqual(details, test.expectedRoundDetails)

      assert.strictEqual(typeof maxTasksPerNode, 'number')

      assert.strictEqual(retrievalTasks.length, test.expectedTaskCount)
      assert.deepStrictEqual(retrievalTasks.slice(0, 2), test.expectedTasks)
    }
  })
})
