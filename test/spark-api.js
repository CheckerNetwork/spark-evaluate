import assert from 'node:assert'
import { fetchRoundDetails } from '../lib/spark-api.js'

const recordTelemetry = (measurementName, fn) => { /* no-op */ }

describe('spark-api client', () => {
  it('fetches round details', async function () {
    this.timeout(10_000)
    const { retrievalTasks, maxTasksPerNode, ...details } = await fetchRoundDetails(
      '0x8460766edc62b525fc1fa4d628fc79229dc73031',
      27834n,
      recordTelemetry
    )

    assert.deepStrictEqual(details, {
      roundId: '33258', // BigInt serialized as String,
      startEpoch: '4802338'
    })

    assert.strictEqual(typeof maxTasksPerNode, 'number')

    assert.strictEqual(retrievalTasks.length, 666)
    assert.deepStrictEqual(retrievalTasks.slice(0, 2), [
      {
        cid: 'bafk2bzaceas6yo7vfxvjcjfrkls6dtddpq5nkhgbaunlqmb6aulofuhbbq4sy',
        minerId: 'f03499325',
        clients: ['f03498278'],
        allocators: ['f03358620']
      },
      {
        cid: 'bafk2bzaceaxtimy3b6dcubgj4xe2fs76gmo7h5kcraaoxjegei5mp7ddkaply',
        minerId: 'f03322378',
        clients: ['f03441573'],
        allocators: ['f03019261']
      }
    ])
  })
})
