// src/config/index.ts
import { z } from "zod";
import dotenv from "dotenv";

dotenv.config();

// ─── Schema ───────────────────────────────────────────────────────────────────
const ConfigSchema = z.object({
  // Server
  NODE_ENV:  z.enum(["development", "production", "test"]).default("development"),
  PORT:      z.coerce.number().default(4000),

  // Database
  DATABASE_URL: z.string().min(1, "DATABASE_URL is required"),

  // Redis
  REDIS_HOST:     z.string().default("localhost"),
  REDIS_PORT:     z.coerce.number().default(6379),
  REDIS_PASSWORD: z.string().optional(),

  // Alchemy
  ALCHEMY_HTTP_URL: z.string().url("ALCHEMY_HTTP_URL must be a valid URL"),
  ALCHEMY_WS_URL:   z.string().min(1, "ALCHEMY_WS_URL is required"),

  // Contract
  P2P_CONTRACT_ADDRESS: z
    .string()
    .regex(/^0x[a-fA-F0-9]{40}$/, "Invalid contract address"),
  DEPLOYED_BLOCK:      z.coerce.number().default(0),

  // Indexer tuning
  BLOCK_BATCH_SIZE:    z.coerce.number().min(100).max(10000).default(2000),
  CONFIRMATION_DEPTH:  z.coerce.number().min(1).max(100).default(12),
BLOCK_TIME_MS: z.coerce.number().default(3000),  // 3000 for BSC, 12000 for ETH

  // Queue tuning
  FILL_JOB_MAX_ATTEMPTS:    z.coerce.number().min(1).max(20).default(5),
  FILL_WORKER_CONCURRENCY:  z.coerce.number().min(1).max(20).default(3),
  CANDLE_JOB_MAX_ATTEMPTS:  z.coerce.number().min(1).max(20).default(5),
  CANDLE_WORKER_CONCURRENCY: z.coerce.number().min(1).max(20).default(5),

  // Logging
  LOG_LEVEL: z.enum(["error", "warn", "info", "debug"]).default("info"),
  LOG_DIR:   z.string().default("./logs"),
});

// ─── Parse & validate ─────────────────────────────────────────────────────────
const _parsed = ConfigSchema.safeParse(process.env);

if (!_parsed.success) {
  console.error("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  console.error("  FATAL: Invalid environment config");
  console.error("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  _parsed.error.issues.forEach((issue) => {
    console.error(`  ✗ ${issue.path.join(".")}: ${issue.message}`);
  });
  console.error("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  process.exit(1);
}

const _env = _parsed.data;

// ─── Exported config (never import process.env directly) ─────────────────────
export const config = {
  env:  _env.NODE_ENV,
  port: _env.PORT,
  isDev: _env.NODE_ENV === "development",
  isProd: _env.NODE_ENV === "production",

  db: {
    url: _env.DATABASE_URL,
  },

  redis: {
    host:     _env.REDIS_HOST,
    port:     _env.REDIS_PORT,
    password: _env.REDIS_PASSWORD,
  },

  alchemy: {
    httpUrl: _env.ALCHEMY_HTTP_URL,
    wsUrl:   _env.ALCHEMY_WS_URL,
  },

  contract: {
    address:       _env.P2P_CONTRACT_ADDRESS.toLowerCase(),
    deployedBlock: _env.DEPLOYED_BLOCK,
  },

  indexer: {
    blockBatchSize:   _env.BLOCK_BATCH_SIZE,
    confirmationDepth: _env.CONFIRMATION_DEPTH,
      blockTimeMs:       _env.BLOCK_TIME_MS,       // ← add this

  },

  queue: {
    fillJobMaxAttempts:     _env.FILL_JOB_MAX_ATTEMPTS,
    fillWorkerConcurrency:  _env.FILL_WORKER_CONCURRENCY,
    candleJobMaxAttempts:   _env.CANDLE_JOB_MAX_ATTEMPTS,
    candleWorkerConcurrency: _env.CANDLE_WORKER_CONCURRENCY,

    // Queue names — single source of truth
    names: {
      fillEvents:   "fill-events",
      candleBuild:  "candle-build",
      deadLetter:   "dead-letter",
    },
  },

  logging: {
    level: _env.LOG_LEVEL,
    dir:   _env.LOG_DIR,
  },
} as const;

export type Config = typeof config;