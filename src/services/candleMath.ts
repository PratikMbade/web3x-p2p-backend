// src/services/candleMath.ts
//
// Pure functions only — no DB, no Redis, no side effects.
// Every function here is independently unit-testable.

import { ethers }                     from "ethers";
import { CandleInterval, INTERVAL_SECONDS, OHLCVCandle } from "../types";

// ─── Price / amount conversion ────────────────────────────────────────────────

/**
 * Convert a wei string to a JavaScript float.
 * Uses ethers.BigNumber internally to avoid precision loss.
 */
export function weiToFloat(wei: string, decimals = 18): number {
  try {
    return parseFloat(ethers.utils.formatUnits(wei, decimals));
  } catch {
    // Fallback: try plain parse (handles already-formatted numbers)
    const n = parseFloat(wei);
    return isNaN(n) ? 0 : n;
  }
}

/**
 * Return true if a price / amount value is usable for candle math.
 * Rejects zero, negative, NaN, and Infinity.
 */
export function isValidPrice(price: number): boolean {
  return Number.isFinite(price) && price > 0;
}

// ─── Time window helpers ──────────────────────────────────────────────────────

/**
 * Floor a Date to the start of a candle interval window (UTC).
 *
 * Example: 14:37:22 on a 15m interval → 14:30:00
 */
export function floorToWindow(timestamp: Date, interval: CandleInterval): Date {
  const secs = INTERVAL_SECONDS[interval];
  const ms   = secs * 1000;
  return new Date(Math.floor(timestamp.getTime() / ms) * ms);
}

/**
 * Return the close time (last millisecond) of a candle window.
 * closeTime = openTime + interval - 1ms
 */
export function windowCloseTime(openTime: Date, interval: CandleInterval): Date {
  return new Date(openTime.getTime() + INTERVAL_SECONDS[interval] * 1000 - 1);
}

/**
 * Return the Unix timestamp (seconds) for a candle's open time.
 * This is what lightweight-charts expects as `time`.
 */
export function toChartTime(openTime: Date): number {
  return Math.floor(openTime.getTime() / 1000);
}

// ─── Candle construction ──────────────────────────────────────────────────────

/** All the data needed to create a brand-new candle row */
export interface NewCandleData {
  tokenOne:   string;
  tokenTwo:   string;
  interval:   CandleInterval;
  openTime:   Date;
  closeTime:  Date;
  open:       string;
  high:       string;
  low:        string;
  close:      string;
  volume:     string;
  tradeCount: number;
}

/** All the data needed to update an existing candle row */
export interface CandleUpdateData {
  newHigh:    string;
  newLow:     string;
  newClose:   string;
  newVolume:  string;
}

/**
 * Build a new candle from the first trade in a window.
 */
export function buildNewCandle(
  tokenOne:       string,
  tokenTwo:       string,
  interval:       CandleInterval,
  timestamp:      Date,
  price:          number,
  fillAmount:     number
): NewCandleData {
  const openTime  = floorToWindow(timestamp, interval);
  const closeTime = windowCloseTime(openTime, interval);

  return {
    tokenOne,
    tokenTwo,
    interval,
    openTime,
    closeTime,
    open:       price.toString(),
    high:       price.toString(),
    low:        price.toString(),
    close:      price.toString(),
    volume:     fillAmount.toString(),
    tradeCount: 1,
  };
}

/**
 * Merge a new trade into an existing candle.
 * Returns only the fields that changed — for a targeted Prisma update.
 */
export function mergeTradeIntoCandle(
  existing: { high: string; low: string; volume: string },
  price:    number,
  fillAmount: number
): CandleUpdateData {
  const currentHigh   = parseFloat(existing.high);
  const currentLow    = parseFloat(existing.low);
  const currentVolume = parseFloat(existing.volume);

  return {
    newHigh:   Math.max(currentHigh,   price).toString(),
    newLow:    Math.min(currentLow,    price).toString(),
    newClose:  price.toString(),
    newVolume: (currentVolume + fillAmount).toString(),
  };
}

/**
 * Convert a raw DB candle row to the OHLCVCandle shape
 * that lightweight-charts and the WebSocket server expect.
 */
export function toOHLCV(row: {
  openTime:   Date;
  open:       string;
  high:       string;
  low:        string;
  close:      string;
  volume:     string;
  tradeCount: number;
}): OHLCVCandle {
  return {
    time:       toChartTime(row.openTime),
    open:       parseFloat(row.open),
    high:       parseFloat(row.high),
    low:        parseFloat(row.low),
    close:      parseFloat(row.close),
    volume:     parseFloat(row.volume),
    tradeCount: row.tradeCount,
  };
}

/**
 * Rebuild all candles for a given set of fill events from scratch.
 * Returns a Map<interval → Map<windowKey → NewCandleData>>
 *
 * Used by the reconciliation service to detect and fix gaps.
 */
export function rebuildCandlesFromFills(
  fills: Array<{
    blockTimestamp: Date;
    pricePerToken:  string;
    fillAmount:     string;
    tokenOne:       string;
    tokenTwo:       string;
  }>,
  intervals: CandleInterval[]
): Map<CandleInterval, Map<string, NewCandleData>> {
  const result = new Map<CandleInterval, Map<string, NewCandleData>>();

  for (const interval of intervals) {
    result.set(interval, new Map<string, NewCandleData>());
  }

  for (const fill of fills) {
    const price  = weiToFloat(fill.pricePerToken);
    const amount = weiToFloat(fill.fillAmount);

    if (!isValidPrice(price) || !isValidPrice(amount)) continue;

    for (const interval of intervals) {
      const windowMap = result.get(interval)!;
      const openTime  = floorToWindow(fill.blockTimestamp, interval);
      const windowKey = openTime.toISOString();

      const existing = windowMap.get(windowKey);

      if (!existing) {
        windowMap.set(
          windowKey,
          buildNewCandle(fill.tokenOne, fill.tokenTwo, interval, fill.blockTimestamp, price, amount)
        );
      } else {
        const merged = mergeTradeIntoCandle(existing, price, amount);
        windowMap.set(windowKey, {
          ...existing,
          high:       merged.newHigh,
          low:        merged.newLow,
          close:      merged.newClose,
          volume:     merged.newVolume,
          tradeCount: existing.tradeCount + 1,
        });
      }
    }
  }

  return result;
}