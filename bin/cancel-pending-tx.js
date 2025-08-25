// dotenv must be imported before importing anything else
import 'dotenv/config'

import assert from 'node:assert'
import { ethers } from 'ethers'
import { CoinType, newDelegatedEthAddress } from '@glif/filecoin-address'
import pRetry from 'p-retry'

import { createContracts } from '../lib/contracts.js'

const {
  WALLET_SEED
} = process.env

const [, , tx] = process.argv

assert(WALLET_SEED, 'WALLET_SEED required')
assert(tx, 'Transaction hash must be provided as the first argument')

const { provider } = createContracts()

const signer = ethers.Wallet.fromPhrase(WALLET_SEED, provider)
const walletDelegatedAddress = newDelegatedEthAddress(/** @type {any} */(signer.address), CoinType.MAIN).toString()
console.log(
  'Wallet address:',
  signer.address,
  walletDelegatedAddress
)

console.log('Going to revoke %s', tx)

const messages = await getMessagesInMempool(walletDelegatedAddress)
const txDetails = messages.find(m => m.cid === tx)
if (!txDetails) {
  console.log('Transaction message not found in the mempool.')
  console.log('https://filfox.info/en/message/%s', tx)
  console.log('Note: we have access to 50 oldest messages only.')
  for (const { cid, createTimestamp } of messages) {
    console.log(' %s (created at %s)', cid, new Date(createTimestamp * 1000).toISOString())
  }
  process.exit(1)
}
console.log('TX message:', txDetails)

/*
https://filecoinproject.slack.com/archives/C0179RNEMU4/p1607269156412600?thread_ts=1606987775.021900&cid=C0179RNEMU4
while you can not cancel a message, you have an option to replace it with a different one, in this
case you would replace it with the cheapest possible one ( a self-send of 0 ) in order to "convince"
the network to take the new message all you have to do is to increase your --gas-premium by 25.2%
from the previous value ( technically it is 25% + 1 attofil, but 25.2 is easier )

you also must LOWER your gas-limit to match the new gas requirements of the message you are sending:
if you do not do so, the network will penalize you for lying ( you said this will cost X gas but
used 1/10th of X to execute: no good )

all in all the steps to replace a message (currently to be fixed soon) are:
- you go to some explorer, find a recent send message, that just landed on chain
- you note what is its gas-used (RECENTGU) and gas-feecap (RECENTFC)
- you find your own message you want to replace and find its current gas-premium (OLDGP)
- then you do magic:

lotus send
  --gas-feecap $RECENTFC-from-above \
  --gas-limit $RECENTGU*1.1 \
  --gas-premium $OLDGP*1.252  \
  --from {{address-you-are-unblocking}} \
  --nonce {{nonce-you-are-replacing}} \
  {{address-you-are-unblocking-as-destination}} \
  0

that should be all, and should not cost you much at all ( we just cleared ~80 messages in the thread
above by paying ~0.05 fil
 */

const recentSendMessage = await getRecentSendMessage()
console.log('Calculating gas fees from the recent Send message %s (created at %s)',
  recentSendMessage.cid,
  new Date(recentSendMessage.timestamp * 1000).toISOString()
)

const gasUsed = recentSendMessage.receipt.gasUsed
const gasFeeCap = Number(recentSendMessage.gasFeeCap)
const oldGasPremium = Number(txDetails.gasPremium)
const nonce = txDetails.nonce

console.log('SENDING THE REPLACEMENT TRANSACTION')
const replacementTx = await signer.sendTransaction({
  to: signer.address,
  value: 0,
  nonce,
  gasLimit: Math.ceil(gasUsed * 1.1),
  maxFeePerGas: gasFeeCap,
  maxPriorityFeePerGas: Math.ceil(oldGasPremium * 1.252)
})
console.log('Waiting for the transaction receipt:', replacementTx.hash)
const receipt = await replacementTx.wait()
console.log('TX status:', receipt?.status)

/**
 * @param {string} f4addr
 * @returns 1000 oldest messages
 */
async function getMessagesInMempool (f4addr) {
  const res = await pRetry(
    () => fetch(`https://filfox.info/api/v1/message/mempool/filtered-list?address=${f4addr}&pageSize=1000`),
    {
      async onFailedAttempt (ctx) {
        console.warn(ctx)
        console.warn('Filfox request failed. Retrying...')
      }
    }
  )
  if (!res.ok) {
    throw new Error(`Filfox request failed with ${res.status}: ${(await res.text()).trimEnd()}`)
  }
  /** @type {{
     messages: {
       cid: string;
       from: string;
       to: string;
       nonce: number;
       value: string;
       gasLimit: number;
       gasFeeCap: string;
       gasPremium: string;
       method: string;
       methodNumber: number;
       evmMethod: string;
       createTimestamp: number;
     }[];
   }}
   */
  const { messages } = /** @type {any} */(await res.json())
  return messages
}

/**
 * @returns {Promise<{
   "cid": string;
   "height": number;
   "timestamp": number;
   "gasLimit": number;
   "gasFeeCap": string;
   "gasPremium": string;
   "method": string;
   "methodNumber": number;
   "receipt": {
      "exitCode": number;
      "return": string;
      "gasUsed": number;
    },
    "size": number;
    "error": string;
    "baseFee": string;
    "fee": {
      "baseFeeBurn": string;
      "overEstimationBurn": string;
      "minerPenalty": string;
      "minerTip": string;
      "refund": string;
    },
  }>}
 */
async function getRecentSendMessage () {
  let res = await fetch('https://filfox.info/api/v1/message/list?method=Send')
  if (!res.ok) {
    throw new Error(`Filfox request failed with ${res.status}: ${(await res.text()).trimEnd()}`)
  }
  const body = /** @type {any} */(await res.json())
  assert(body.messages.length > 0, '/message/list returned an empty list')
  const sendMsg = body.messages.find(m => m.method === 'Send')
  assert(!!sendMsg, 'No Send message found in the recent committed messages')
  const cid = sendMsg.cid

  res = await fetch(`https://filfox.info/api/v1/message/${cid}`)
  if (!res.ok) {
    throw new Error(`Filfox request failed with ${res.status}: ${(await res.text()).trimEnd()}`)
  }

  return /** @type {any} */(await res.json())
}
