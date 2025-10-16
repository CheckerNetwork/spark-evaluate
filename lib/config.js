const {
  RPC_URLS = 'https://api.node.glif.io/rpc/v0,https://filecoin.chain.love/rpc/v1',
  GLIF_TOKEN,
  DATABASE_URL = 'postgres://localhost:5432/spark_evaluate',
  SPARK_API = 'https://api.filspark.com'
} = process.env

const rpcUrls = RPC_URLS.split(',')

export {
  rpcUrls,
  DATABASE_URL,
  SPARK_API,
  GLIF_TOKEN
}
