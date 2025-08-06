import * as Client from '@web3-storage/w3up-client'
import { ed25519 } from '@ucanto/principal'
import { CarReader } from '@ipld/car'
import { importDAG } from '@ucanto/core/delegation'

/**
 * @param {string} data
 */
async function parseProof (data) {
  // Casting to unsafe `any[]` to avoid the following TypeScript error:
  //    Argument of type 'Block[]' is not assignable to parameter
  //    of type 'Iterable<Block<unknown, number, number, 1>>'
  /** @type {any[]} */
  const blocks = []
  const reader = await CarReader.fromBytes(Buffer.from(data, 'base64'))
  for await (const block of reader.blocks()) {
    blocks.push(block)
  }
  return importDAG(blocks)
}

export async function createStorachaClient ({ secretKey, proof }) {
  const principal = ed25519.Signer.parse(secretKey)
  const client = await Client.create({ principal })
  const space = await client.addSpace(await parseProof(proof))
  await client.setCurrentSpace(space.did())
  return client
}
