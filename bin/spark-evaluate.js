import { DATABASE_URL } from '../lib/config.js'
import { startEvaluate } from '../index.js'
import { fetchRoundDetails } from '../lib/spark-api.js'
import assert from 'node:assert'
import { ethers } from 'ethers'
import { CoinType, newDelegatedEthAddress } from '@glif/filecoin-address'
import { fetchMeasurements } from '../lib/preprocess.js'
import { migrateWithPgConfig } from '../lib/migrate.js'
import pg from 'pg'
import { createContracts } from '../lib/contracts.js'
import * as providerRetrievalResultStats from '../lib/provider-retrieval-result-stats.js'
import { createStorachaClient } from '../lib/storacha.js'
import { createInflux } from '../lib/telemetry.js'

const {
  WALLET_SEED,
  STORACHA_SECRET_KEY,
  STORACHA_PROOF,
  GIT_COMMIT,
  INFLUXDB_TOKEN
} = process.env

assert(WALLET_SEED, 'WALLET_SEED required')
assert(STORACHA_SECRET_KEY, 'STORACHA_SECRET_KEY required')
assert(STORACHA_PROOF, 'STORACHA_PROOF required')
assert(INFLUXDB_TOKEN, 'INFLUXDB_TOKEN required')

await migrateWithPgConfig({ connectionString: DATABASE_URL })

const storachaClient = await createStorachaClient({
  secretKey: STORACHA_SECRET_KEY,
  proof: STORACHA_PROOF
})
const { ieContract, ieContractAddress, rsrContract, provider } = createContracts()

const signer = ethers.Wallet.fromPhrase(WALLET_SEED, provider)
const walletDelegatedAddress = newDelegatedEthAddress(/** @type {any} */(signer.address), CoinType.MAIN).toString()

console.log('Wallet address:', signer.address, walletDelegatedAddress)

const createPgClient = async () => {
  const pgClient = new pg.Client({ connectionString: DATABASE_URL })
  await pgClient.connect()
  return pgClient
}

const { recordTelemetry } = createInflux(INFLUXDB_TOKEN)

await Promise.all([
  startEvaluate({
    ieContract,
    fetchMeasurements,
    fetchRoundDetails,
    recordTelemetry,
    createPgClient,
    logger: console,
    prepareProviderRetrievalResultStats: (round, committees) => providerRetrievalResultStats.prepare({
      storachaClient,
      createPgClient,
      round,
      committees,
      sparkEvaluateVersion: GIT_COMMIT,
      ieContractAddress
    })
  }),
  providerRetrievalResultStats.runPublishLoop({
    createPgClient,
    storachaClient,
    rsrContract: rsrContract.connect(signer)
  })
])
