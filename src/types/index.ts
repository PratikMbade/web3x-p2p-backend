// src/types/index.ts

// ─── Order status / type enums ───────────────────────────────────────────────
export type OrderStatus = "open" | "filled" | "cancelled";
export type OrderType   = "buy" | "sell";   // 1 = buy, 2 = sell

// ─── Raw OrderCreated event ───────────────────────────────────────────────────
export interface RawOrderCreatedEvent {
  txHash:         string;
  blockNumber:    number;
  blockTimestamp: Date;
  logIndex:       number;
  creator:        string;   // toLowerCase'd
  tokenOne:       string;   // tokenToBuy toLowerCase'd
  tokenTwo:       string;   // tokenToExchange toLowerCase'd
  orderId:        string;   // BigInt as string
  orderType:      OrderType;
  amount:         string;   // wei string
  pricePerToken:  string;   // wei string — available in this event
}

// ─── Raw OrderCancelled event ─────────────────────────────────────────────────
export interface RawOrderCancelledEvent {
  txHash:         string;
  blockNumber:    number;
  blockTimestamp: Date;
  logIndex:       number;
  canceller:      string;   // toLowerCase'd
  tokenOne:       string;   // toLowerCase'd
  tokenTwo:       string;   // toLowerCase'd
  orderId:        string;
}

// ─── Redis pub/sub message shape (Step 3 WebSocket consumes this) ─────────────
export interface OrderBookEvent {
  type:      "ORDER_CREATED" | "ORDER_CANCELLED" | "ORDER_FILLED";
  txHash:    string;
  tokenOne:  string;
  tokenTwo:  string;
  orderId:   string;
  timestamp: number;   // unix seconds
  data:      Record<string, unknown>;
}

// ─── Raw FillEvent off the chain ──────────────────────────────────────────────
export interface RawFillEvent {
  txHash:         string;
  blockNumber:    number;
  blockTimestamp: Date;
  logIndex:       number;   // position within block — needed for ordering
  tokenOne:       string;   // toLowerCase'd
  tokenTwo:       string;   // toLowerCase'd
  orderId:        string;   // BigInt serialised as string (JSON-safe)
  filler:         string;   // toLowerCase'd
  fillAmount:     string;   // wei as string
  pricePerToken:  string;   // wei as string
}

// ─── BullMQ job payloads ──────────────────────────────────────────────────────

/** Payload pushed onto "fill-events" queue by the indexer */
export interface FillJobPayload {
  raw:       RawFillEvent;
  enqueuedAt: number;       // Date.now() — latency tracking
}

/** Payload pushed onto "dead-letter" queue when fill fails all retries */
export interface DeadLetterPayload {
  originalJobId:   string;
  originalQueue:   string;
  failedAt:        string;   // ISO date
  lastError:       string;
  attemptsMade:    number;
  payload:         FillJobPayload;
}

// ─── Indexer state ────────────────────────────────────────────────────────────
export interface IndexerCheckpoint {
  lastSafeBlock:      number;   // last block fully processed
  lastUpdatedAt:      Date;
}

export interface GapDetectionResult {
  hasGap:       boolean;
  gapStartBlock: number;
  gapEndBlock:   number;
  gapSizeBlocks: number;
}

// ─── Worker result ────────────────────────────────────────────────────────────
export type WorkerResult =
  | { status: "persisted"; txHash: string }
  | { status: "duplicate"; txHash: string }
  | { status: "skipped";   reason: string };

// ─── Health / metrics ─────────────────────────────────────────────────────────
export interface QueueHealth {
  name:       string;
  waiting:    number;
  active:     number;
  completed:  number;
  failed:     number;
  delayed:    number;
  paused:     boolean;
}

export interface IndexerHealth {
  status:           "healthy" | "degraded" | "down";
  wsConnected:      boolean;
  lastIndexedBlock: number;
  currentBlock:     number;
  blockLag:         number;
  queues:           QueueHealth[];
  uptime:           number;      // seconds
}

// ─── Candle intervals ─────────────────────────────────────────────────────────
export const CANDLE_INTERVALS = ["1m", "5m", "15m", "1h", "4h", "1d"] as const;
export type  CandleInterval   = typeof CANDLE_INTERVALS[number];

export const INTERVAL_SECONDS: Record<CandleInterval, number> = {
  "1m":  60,
  "5m":  300,
  "15m": 900,
  "1h":  3600,
  "4h":  14400,
  "1d":  86400,
};

// ─── OHLCV candle (lightweight-charts compatible) ─────────────────────────────
export interface OHLCVCandle {
  time:       number;   // Unix timestamp (seconds) — openTime of the window
  open:       number;
  high:       number;
  low:        number;
  close:      number;
  volume:     number;
  tradeCount: number;
}

// ─── Candle build job payload ─────────────────────────────────────────────────

/** Pushed onto "candle-build" queue by fillWorker after persisting a fill */
export interface CandleJobPayload {
  fillTxHash:     string;    // source fill — for tracing
  blockNumber:    number;
  blockTimestamp: string;    // ISO string — Date not JSON-safe
  tokenOne:       string;
  tokenTwo:       string;
  fillAmount:     string;    // wei string
  pricePerToken:  string;    // wei string
  enqueuedAt:     number;    // Date.now()
}

/** Result returned by candle worker */
export type CandleWorkerResult =
  | { status: "built";     intervals: CandleInterval[]; tokenOne: string; tokenTwo: string }
  | { status: "skipped";   reason: string }
  | { status: "duplicate"; fillTxHash: string };

// ─── Reconciliation ───────────────────────────────────────────────────────────

export interface ReconciliationResult {
  ranAt:            Date;
  tokenPairsChecked: number;
  candlesFixed:     number;
  candlesCreated:   number;
  durationMs:       number;
  errors:           string[];
}

// ─── Pair registry ────────────────────────────────────────────────────────────

/** Represents a unique token pair that has at least one fill */
export interface TokenPair {
  tokenOne: string;
  tokenTwo: string;
}

// ─── Logger context shape ─────────────────────────────────────────────────────
export interface LogContext {
  service?:     string;
  txHash?:      string;
  blockNumber?: number;
  jobId?:       string;
  orderId?:     string;
  tokenOne?:    string;
  tokenTwo?:    string;
  attempt?:     number;
  durationMs?:  number;
  error?:       string;
  [key: string]: unknown;
}