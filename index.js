import ms from 'ms'
import assert from 'node:assert'
import * as Sentry from '@sentry/node'
import { preprocess } from './lib/preprocess.js'
import { evaluate } from './lib/evaluate.js'
import { RoundData } from './lib/round.js'
import { refreshDatabase } from './lib/platform-stats.js'
import timers from 'node:timers/promises'

// Tweak this value to improve the chances of the data being available
const PREPROCESS_DELAY = 120_000

const EVALUATE_DELAY = PREPROCESS_DELAY + 60_000

export const startEvaluate = async ({
  ieContract,
  fetchMeasurements,
  fetchRoundDetails,
  recordTelemetry,
  createPgClient,
  logger,
  setScores,
  prepareProviderRetrievalResultStats
}) => {
  assert(typeof createPgClient === 'function', 'createPgClient must be a function')

  const rounds = {
    current: null,
    previous: null
  }
  const cidsSeen = []
  const roundsSeen = []
  let lastNewEventSeenAt = null

  const onMeasurementsAdded = async (cid, _roundIndex) => {
    const roundIndex = BigInt(_roundIndex)
    if (cidsSeen.includes(cid)) return
    cidsSeen.push(cid)
    if (cidsSeen.length > 1000) cidsSeen.shift()
    lastNewEventSeenAt = new Date()

    if (!rounds.current) {
      rounds.current = new RoundData(roundIndex)
    } else if (rounds.current.index !== roundIndex) {
      // This occassionally happens because of a race condition between onMeasurementsAdded
      // and onRoundStart event handlers.
      // See https://github.com/filecoin-station/spark-evaluate/issues/233
      const msg = 'Round index mismatch when processing MeasurementsAdded event'
      const details = {
        currentRoundIndex: rounds.current.index,
        eventRoundIndex: roundIndex,
        measurementsCid: cid
      }
      console.error(msg, details)
      Sentry.captureException(new Error(msg), { extra: details })
      return
    }

    console.log('Event: MeasurementsAdded', { roundIndex })
    console.log(`Sleeping for ${PREPROCESS_DELAY}ms before preprocessing to improve chances of the data being available`)
    await timers.setTimeout(PREPROCESS_DELAY)
    console.log(`Now preprocessing measurements for CID ${cid} in round ${roundIndex}`)

    // Preprocess
    try {
      await preprocess({
        round: rounds.current,
        cid,
        roundIndex,
        fetchMeasurements,
        recordTelemetry,
        logger
      })
    } catch (err) {
      console.error('CANNOT PREPROCESS MEASUREMENTS [ROUND=%s]:', roundIndex, err)
      Sentry.captureException(err, {
        extra: {
          roundIndex,
          measurementsCid: cid
        }
      })
    }
  }

  const onRoundStart = async (_roundIndex) => {
    const roundIndex = BigInt(_roundIndex)
    if (roundsSeen.includes(roundIndex)) return
    roundsSeen.push(roundIndex)
    if (roundsSeen.length > 1000) roundsSeen.shift()
    lastNewEventSeenAt = new Date()

    console.log('Event: RoundStart', { roundIndex })

    if (!rounds.current) {
      console.error('No current round data available, skipping evaluation')
      return
    }

    rounds.previous = rounds.current
    rounds.current = new RoundData(roundIndex)
    console.log('Advanced the current round to %s', roundIndex)

    // TODO: Fix this properly and implement a signalling mechanism allowing the "preprocess" step
    // to notify the "evaluate" when the preprocessing is done, so that we don't have to use a timer
    // here. See also https://github.com/filecoin-station/spark-evaluate/issues/64
    console.log(`Sleeping for ${EVALUATE_DELAY}ms before evaluating the round to let the preprocess step finish for the last batch of measurements`)
    await timers.setTimeout(EVALUATE_DELAY)
    console.log(`Now evaluating the round ${roundIndex}`)

    // Evaluate previous round
    const evaluatedRoundIndex = roundIndex - 1n
    evaluate({
      round: rounds.previous,
      roundIndex: evaluatedRoundIndex,
      ieContract,
      fetchRoundDetails,
      recordTelemetry,
      createPgClient,
      logger,
      prepareProviderRetrievalResultStats
    }).catch(err => {
      console.error('CANNOT EVALUATE ROUND %s:', evaluatedRoundIndex, err)
      Sentry.captureException(err, {
        extra: {
          roundIndex: evaluatedRoundIndex
        }
      })
    })
  }

  // Listen for events
  ieContract.on('MeasurementsAdded', (...args) => {
    onMeasurementsAdded(...args).catch(err => {
      console.error('CANNOT ADD MEASUREMENTS:', err)
      Sentry.captureException(err)
    })
  })

  ieContract.on('RoundStart', (...args) => {
    onRoundStart(...args).catch(err => {
      console.error('CANNOT HANDLE START OF ROUND %s:', args[0], err)
      Sentry.captureException(err)
    })
  })

  // Periodically update tables aggregating fine-grained statistics
  setInterval(() => refreshDatabase(createPgClient), ms('4 hours'))

  while (true) {
    await timers.setTimeout(10_000)
    if (lastNewEventSeenAt) {
      recordTelemetry('last_new_event_seen', point => {
        point.intField(
          'age_s',
          Math.round(
            (new Date().getTime() - lastNewEventSeenAt.getTime()) / 1000
          )
        )
      })
    }
  }
}
