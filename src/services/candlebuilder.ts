// src/services/candleBuilder.ts
//
// Responsible for:
//   1. Upserting OHLCV candle rows in Postgres for all intervals
//   2. Publishing live candle updates to Redis pub/sub (Step 3 WebSocket uses this)
//   3. Keeping a Redis cache of the latest candle per pair+interval

import IORedis, { Redis }                    from "ioredis";
import { prisma }                 from "../lib/prisma";
import { bullmqRedis }            from "../lib/redis";
import { createLogger }           from "../lib/logger";
import {
  CandleJobPayload,
  CandleInterval,
  CANDLE_INTERVALS,
  OHLCVCandle,
}                                 from "../types";
import {
  weiToFloat,
  isValidPrice,
  floorToWindow,
  windowCloseTime,
  buildNewCandle,
  mergeTradeIntoCandle,
  toOHLCV,
}                                 from "./candleMath";

const log = createLogger("candle-builder");

// ─── Redis key helpers ────────────────────────────────────────────────────────

const redisKey = {
  /** Pub/sub channel — WebSocket server subscribes to this (Step 3) */
  candleChannel: (t1: string, t2: string, interval: string) =>
    `candle:${t1}:${t2}:${interval}`,

  /** Latest candle cache */
  latestCandle: (t1: string, t2: string, interval: string) =>
    `latest_candle:${t1}:${t2}:${interval}`,

  /** History cache — invalidated on every update */
  historyCache: (t1: string, t2: string, interval: string) =>
    `candle_history:${t1}:${t2}:${interval}`,

  /** Dedup key — prevents double-processing the same fill for candles */
  candleDedup: (txHash: string) =>
    `candle_dedup:${txHash}`,
};

const DEDUP_TTL_SECONDS  = 86400 * 2;  // 2 days
const LATEST_CANDLE_TTL  = 30;         // 30 seconds
const HISTORY_CACHE_TTL  = 15;         // 15 seconds — short: live data changes often

// ─── Main entry point ─────────────────────────────────────────────────────────

/**
 * Process one CandleJobPayload:
 *   - Validate price/amount
 *   - Upsert OHLCV rows for all 6 intervals in a single Postgres transaction
 *   - Publish live update to Redis for each interval
 *   - Update Redis latest-candle cache
 */
export async function buildCandlesFromFill(
  payload: CandleJobPayload
): Promise<CandleInterval[]> {
  const { fillTxHash, tokenOne, tokenTwo, fillAmount, pricePerToken, blockTimestamp } = payload;

  const price  = weiToFloat(pricePerToken);
  const amount = weiToFloat(fillAmount);

  if (!isValidPrice(price)) {
    log.warn("Skipping candle build — invalid price", { fillTxHash, price });
    return [];
  }

  if (!isValidPrice(amount)) {
    log.warn("Skipping candle build — invalid amount", { fillTxHash, amount });
    return [];
  }

  const timestamp = new Date(blockTimestamp);
  const start     = Date.now();

  // ── Deduplication via Redis ────────────────────────────────────────────────
  // Prevents the same fill from being double-counted if the job is retried
  const dedupKey    = redisKey.candleDedup(fillTxHash);
  const dedupExists = await (bullmqRedis as Redis).set(
    dedupKey, "1", "EX", DEDUP_TTL_SECONDS, "NX"
  );

  if (dedupExists === null) {
    // NX failed — key already existed — this fill was already processed
    log.debug("Candle dedup hit — skipping", { fillTxHash });
    return [];
  }

  const updatedIntervals: CandleInterval[] = [];

  // ── Upsert candles for all intervals ──────────────────────────────────────
  // Each interval is an independent upsert — they don't need to be in one
  // transaction because candles are rebuildable from fills at any time.
  await Promise.all(
    CANDLE_INTERVALS.map(async (interval) => {
      try {
        const wasUpdated = await upsertCandle(
          tokenOne, tokenTwo, interval, timestamp, price, amount
        );
        if (wasUpdated) updatedIntervals.push(interval);
      } catch (err: unknown) {
        // One interval failing must NOT block the others
        log.error("Candle upsert failed for interval", {
          interval,
          fillTxHash,
          tokenOne,
          tokenTwo,
          error: err instanceof Error ? err.message : String(err),
        });
        // Re-throw so the worker retries the whole job
        throw err;
      }
    })
  );

  log.info("Candles built from fill", {
    fillTxHash,
    tokenOne:  tokenOne.slice(0, 10),
    tokenTwo:  tokenTwo.slice(0, 10),
    intervals: updatedIntervals,
    durationMs: Date.now() - start,
  });

  return updatedIntervals;
}

// ─── Candle upsert ─────────────────────────────────────────────────────────────

async function upsertCandle(
  tokenOne:  string,
  tokenTwo:  string,
  interval:  CandleInterval,
  timestamp: Date,
  price:     number,
  amount:    number
): Promise<boolean> {
  const openTime  = floorToWindow(timestamp, interval);
  const closeTime = windowCloseTime(openTime, interval);

  // ── Try to find existing candle for this window ────────────────────────────
  const existing = await prisma.candle.findUnique({
    where: {
      tokenOne_tokenTwo_interval_openTime: {
        tokenOne,
        tokenTwo,
        interval,
        openTime,
      },
    },
    select: {
      id:     true,
      high:   true,
      low:    true,
      volume: true,
    },
  });

  let savedCandle: {
    openTime:   Date;
    open:       string;
    high:       string;
    low:        string;
    close:      string;
    volume:     string;
    tradeCount: number;
  };

  if (!existing) {
    // ── CREATE: first trade in this window ────────────────────────────────────
    const newData = buildNewCandle(tokenOne, tokenTwo, interval, timestamp, price, amount);

    savedCandle = await prisma.candle.create({
      data: {
        tokenOne:   newData.tokenOne,
        tokenTwo:   newData.tokenTwo,
        interval:   newData.interval,
        openTime:   newData.openTime,
        closeTime:  newData.closeTime,
        open:       newData.open,
        high:       newData.high,
        low:        newData.low,
        close:      newData.close,
        volume:     newData.volume,
        tradeCount: newData.tradeCount,
      },
      select: {
        openTime: true, open: true, high: true,
        low: true, close: true, volume: true, tradeCount: true,
      },
    });

    log.debug("Candle created", {
      interval,
      openTime: openTime.toISOString(),
      price,
    });
  } else {
    // ── UPDATE: merge into existing candle ────────────────────────────────────
    const merged = mergeTradeIntoCandle(existing, price, amount);

    savedCandle = await prisma.candle.update({
      where: { id: existing.id },
      data: {
        high:       merged.newHigh,
        low:        merged.newLow,
        close:      merged.newClose,
        volume:     merged.newVolume,
        tradeCount: { increment: 1 },
      },
      select: {
        openTime: true, open: true, high: true,
        low: true, close: true, volume: true, tradeCount: true,
      },
    });

    log.debug("Candle updated", {
      interval,
      openTime:  openTime.toISOString(),
      newClose:  merged.newClose,
      newHigh:   merged.newHigh,
    });
  }

  // ── Publish to Redis ───────────────────────────────────────────────────────
  const ohlcv = toOHLCV({ ...savedCandle, openTime });
  await publishCandleUpdate(tokenOne, tokenTwo, interval, ohlcv, !existing);

  return true;
}

// ─── Redis publish ─────────────────────────────────────────────────────────────

async function publishCandleUpdate(
  tokenOne: string,
  tokenTwo: string,
  interval: CandleInterval,
  candle:   OHLCVCandle,
  isNew:    boolean
): Promise<void> {
  const redis = bullmqRedis as Redis;

  const message = JSON.stringify({
    type: "CANDLE_UPDATE",
    payload: {
      tokenOne,
      tokenTwo,
      interval,
      candle,
      isNew,
    },
  });

  const channel   = redisKey.candleChannel(tokenOne, tokenTwo, interval);
  const latestKey = redisKey.latestCandle(tokenOne, tokenTwo, interval);
  const historyKey = redisKey.historyCache(tokenOne, tokenTwo, interval);

  await Promise.all([
    // Publish to WebSocket server (Step 3 will subscribe here)
    redis.publish(channel, message),

    // Cache latest candle
    redis.set(latestKey, JSON.stringify(candle), "EX", LATEST_CANDLE_TTL),

    // Invalidate history cache so next REST/WS fetch is fresh from DB
    redis.del(historyKey),
  ]);
}

// ─── Reconciliation helper ─────────────────────────────────────────────────────

/**
 * Rebuild all candles for a token pair from raw FillEvent rows.
 * Wipes existing candles first — safe because fills are the source of truth.
 * Called by the reconciliation cron (see reconciliation.ts).
 */
export async function rebuildPairCandles(
  tokenOne: string,
  tokenTwo: string
): Promise<{ created: number; durationMs: number }> {
  const start = Date.now();
  log.info("Rebuilding candles for pair", {
    tokenOne: tokenOne.slice(0, 10),
    tokenTwo: tokenTwo.slice(0, 10),
  });

  // 1. Load all fills for this pair ordered by time
  const fills = await prisma.fillEvent.findMany({
    where:   { tokenOne, tokenTwo },
    orderBy: { blockTimestamp: "asc" },
    select: {
      txHash:         true,
      blockTimestamp: true,
      pricePerToken:  true,
      fillAmount:     true,
      tokenOne:       true,
      tokenTwo:       true,
    },
  });

  if (fills.length === 0) {
    log.info("No fills for pair — skipping rebuild", { tokenOne, tokenTwo });
    return { created: 0, durationMs: Date.now() - start };
  }

  // 2. Delete all existing candles for this pair
  const deleted = await prisma.candle.deleteMany({ where: { tokenOne, tokenTwo } });
  log.debug("Deleted existing candles", { count: deleted.count, tokenOne, tokenTwo });

  // 3. Invalidate all Redis caches for this pair
  const redis = bullmqRedis as Redis;
  for (const interval of CANDLE_INTERVALS) {
    await redis.del(
      redisKey.latestCandle(tokenOne, tokenTwo, interval),
      redisKey.historyCache(tokenOne, tokenTwo, interval)
    );
    // Clear dedup keys so candles can be re-created
    for (const fill of fills) {
      await redis.del(redisKey.candleDedup(fill.txHash));
    }
  }

  // 4. Rebuild by re-processing each fill sequentially
  let created = 0;
  for (const fill of fills) {
    const payload: CandleJobPayload = {
      fillTxHash:     fill.txHash,
      blockNumber:    0,  // Not needed for rebuild
      blockTimestamp: fill.blockTimestamp.toISOString(),
      tokenOne:       fill.tokenOne,
      tokenTwo:       fill.tokenTwo,
      fillAmount:     fill.fillAmount,
      pricePerToken:  fill.pricePerToken,
      enqueuedAt:     Date.now(),
    };
    const intervals = await buildCandlesFromFill(payload);
    created += intervals.length;
  }

  const durationMs = Date.now() - start;

  log.info("Candle rebuild complete", {
    tokenOne: tokenOne.slice(0, 10),
    tokenTwo: tokenTwo.slice(0, 10),
    fillsProcessed: fills.length,
    candleRowsCreated: created,
    durationMs,
  });

  return { created, durationMs };
}