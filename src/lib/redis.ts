// src/lib/redis.ts
import { Redis, type RedisOptions } from "ioredis";
import { config } from "../config";
import { createLogger } from "./logger";

const log = createLogger("redis");

// ─── Connection options shared by all clients ─────────────────────────────────
const redisOptions: RedisOptions = {
  host:            config.redis.host,
  port:            config.redis.port,
  password:        config.redis.password || undefined,
  maxRetriesPerRequest: null,  // Required by BullMQ
  enableReadyCheck: false,     // Required by BullMQ
  lazyConnect:     true,       // Connect explicitly

  retryStrategy(times: number): number | null {
    if (times > 20) {
      log.error("Redis retry limit reached — giving up", { attempt: times });
      return null;  // Stop retrying
    }
    const delay = Math.min(times * 200, 5000);
    log.warn(`Redis reconnecting`, { attempt: times, delayMs: delay });
    return delay;
  },

  reconnectOnError(err: Error): boolean {
    log.error("Redis connection error", { error: err.message });
    return true;  // Always reconnect
  },
};

// ─── Create a named Redis client ───────────────────────────────────────────────
function createRedisClient(name: string): Redis {
  const client = new Redis({ ...redisOptions });

  client.on("connect",  () => log.info(`Redis client connected`,    { client: name }));
  client.on("ready",    () => log.info(`Redis client ready`,         { client: name }));
  client.on("error",    (err: { message: any; }) => log.error(`Redis client error`,     { client: name, error: err.message }));
  client.on("close",    () => log.warn(`Redis client closed`,        { client: name }));
  client.on("reconnecting", () => log.warn(`Redis client reconnecting`, { client: name }));

  return client;
}

// ─── Exported clients ─────────────────────────────────────────────────────────
// BullMQ requires separate IORedis instances for queue and worker
export const bullmqRedis    = createRedisClient("bullmq");       // BullMQ queue operations
export const bullmqSubRedis = createRedisClient("bullmq-sub");   // BullMQ event subscription

// ─── Connect all clients ──────────────────────────────────────────────────────
export async function connectRedis(): Promise<void> {
  const clients = [bullmqRedis, bullmqSubRedis];

  await Promise.all(
    clients.map(async (client) => {
      if (client.status === "end") {
        await client.connect();
      } else if (client.status === "wait") {
        await client.connect();
      }
      // if status is "connecting" or "ready" → skip
    })
  );

  log.info("All Redis clients connected");
}

// ─── Disconnect all clients ───────────────────────────────────────────────────
export async function disconnectRedis(): Promise<void> {
  await Promise.all([
    bullmqRedis.quit(),
    bullmqSubRedis.quit(),
  ]);
  log.info("All Redis clients disconnected");
}

// ─── Health check ─────────────────────────────────────────────────────────────
export async function pingRedis(): Promise<boolean> {
  try {
    const result = await bullmqRedis.ping();
    return result === "PONG";
  } catch {
    return false;
  }
}