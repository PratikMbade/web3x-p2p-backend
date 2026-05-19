// src/queues/fillQueue.ts
import { Queue, QueueEvents } from "bullmq";
import { bullmqRedis, bullmqSubRedis } from "../lib/redis";
import { config } from "../config";
import { createLogger } from "../lib/logger";
import { FillJobPayload, DeadLetterPayload, CandleJobPayload } from "../types";

const log = createLogger("queue");

// ─── Queue: fill-events ───────────────────────────────────────────────────────
// Every on-chain fill is pushed here. Workers consume and persist to Postgres.
export const fillQueue = new Queue<FillJobPayload>(
  config.queue.names.fillEvents,
  {
    connection: bullmqRedis,
    defaultJobOptions: {
      attempts:    config.queue.fillJobMaxAttempts,
      backoff: {
        type:  "exponential",
        delay: 1000,   // 1s → 2s → 4s → 8s → 16s
      },
      removeOnComplete: {
        age:   86400 * 7,  // Keep completed jobs 7 days
        count: 10000,
      },
      removeOnFail: false, // NEVER auto-remove failed jobs — we need the audit trail
    },
  }
);

// ─── Queue: dead-letter ───────────────────────────────────────────────────────
// Jobs that exhaust all retries land here for manual inspection / replay.
export const deadLetterQueue = new Queue<DeadLetterPayload>(
  config.queue.names.deadLetter,
  {
    connection: bullmqRedis,
    defaultJobOptions: {
      attempts: 1,         // Dead-letter jobs don't retry automatically
      removeOnComplete: false,
      removeOnFail:     false,
    },
  }
);

// ─── Queue: candle-build ──────────────────────────────────────────────────────
// After a fill is persisted, a candle job is enqueued here.
// Isolated failure domain: if candle build fails, fill data is already safe.
export const candleQueue = new Queue<CandleJobPayload>(
  config.queue.names.candleBuild,
  {
    connection: bullmqRedis,
    defaultJobOptions: {
      attempts: config.queue.candleJobMaxAttempts,
      backoff: {
        type:  "exponential",
        delay: 500,   // 0.5s → 1s → 2s → 4s → 8s
      },
      removeOnComplete: {
        age:   86400 * 3,  // Keep 3 days of completed candle jobs
        count: 50000,
      },
      removeOnFail: false,
    },
  }
);

export const candleQueueEvents = new QueueEvents(
  config.queue.names.candleBuild,
  { connection: bullmqSubRedis }
);

candleQueueEvents.on("failed", ({ jobId, failedReason }) => {
  log.error("Candle job failed", {
    jobId,
    queue: config.queue.names.candleBuild,
    error: failedReason,
  });
});

candleQueueEvents.on("stalled", ({ jobId }) => {
  log.warn("Candle job stalled — will retry", { jobId });
});

// ─── Helper: enqueue a candle build job ──────────────────────────────────────
export async function enqueueCandleBuild(payload: CandleJobPayload): Promise<void> {
  // Job ID = txHash so re-enqueuing the same fill never creates duplicate candle jobs
  const jobId = `candle-${payload.fillTxHash}`;

  const job = await candleQueue.add("build-candles", payload, { jobId });

  log.debug("Candle build enqueued", {
    jobId:      job.id,
    fillTxHash: payload.fillTxHash,
    tokenOne:   payload.tokenOne,
    tokenTwo:   payload.tokenTwo,
  });
}

export async function getCandleQueueHealth() {
  const [waiting, active, completed, failed, delayed] = await Promise.all([
    candleQueue.getWaitingCount(),
    candleQueue.getActiveCount(),
    candleQueue.getCompletedCount(),
    candleQueue.getFailedCount(),
    candleQueue.getDelayedCount(),
  ]);

  return {
    name:    config.queue.names.candleBuild,
    waiting,
    active,
    completed,
    failed,
    delayed,
    paused:  await candleQueue.isPaused(),
  };
}
export const fillQueueEvents = new QueueEvents(
  config.queue.names.fillEvents,
  { connection: bullmqSubRedis }
);

fillQueueEvents.on("completed", ({ jobId }) => {
  log.info("Job completed", { jobId, queue: config.queue.names.fillEvents });
});

fillQueueEvents.on("failed", ({ jobId, failedReason }) => {
  log.error("Job failed", {
    jobId,
    queue: config.queue.names.fillEvents,
    error: failedReason,
  });
});

fillQueueEvents.on("stalled", ({ jobId }) => {
  log.warn("Job stalled — will be retried", {
    jobId,
    queue: config.queue.names.fillEvents,
  });
});

// ─── Helper: enqueue a fill event ────────────────────────────────────────────
export async function enqueueFill(payload: FillJobPayload): Promise<void> {
  const jobId = payload.raw.txHash; // idempotency: same txHash = same jobId

  // BullMQ deduplicates by jobId automatically
  const job = await fillQueue.add(
    "process-fill",
    payload,
    {
      jobId,
      // Delay processing until block has enough confirmations
      // (handled by indexer before enqueue — but belt + suspenders)
    }
  );

  log.debug("Fill enqueued", {
    jobId:       job.id,
    txHash:      payload.raw.txHash,
    blockNumber: payload.raw.blockNumber,
  });
}

// ─── Helper: move failed job to dead-letter ───────────────────────────────────
export async function moveToDeadLetter(
  originalJobId: string,
  payload:        FillJobPayload,
  error:          Error,
  attemptsMade:   number
): Promise<void> {
  const dlPayload: DeadLetterPayload = {
    originalJobId,
    originalQueue: config.queue.names.fillEvents,
    failedAt:      new Date().toISOString(),
    lastError:     error.message,
    attemptsMade,
    payload,
  };

  await deadLetterQueue.add(`dlq-${originalJobId}`, dlPayload, {
    jobId: `dlq-${originalJobId}`,
  });

  log.error("Job moved to dead-letter queue", {
    originalJobId,
    txHash:       payload.raw.txHash,
    error:        error.message,
    attemptsMade,
  });
}

// ─── Health: get queue counts ─────────────────────────────────────────────────
export async function getFillQueueHealth() {
  const [waiting, active, completed, failed, delayed] = await Promise.all([
    fillQueue.getWaitingCount(),
    fillQueue.getActiveCount(),
    fillQueue.getCompletedCount(),
    fillQueue.getFailedCount(),
    fillQueue.getDelayedCount(),
  ]);

  return {
    name:      config.queue.names.fillEvents,
    waiting,
    active,
    completed,
    failed,
    delayed,
    paused:    await fillQueue.isPaused(),
  };
}

export async function getDeadLetterHealth() {
  const [waiting, active, failed] = await Promise.all([
    deadLetterQueue.getWaitingCount(),
    deadLetterQueue.getActiveCount(),
    deadLetterQueue.getFailedCount(),
  ]);

  return {
    name:      config.queue.names.deadLetter,
    waiting,
    active,
    completed: 0,
    failed,
    delayed:   0,
    paused:    await deadLetterQueue.isPaused(),
  };
}

// ─── Graceful shutdown ────────────────────────────────────────────────────────
export async function closeQueues(): Promise<void> {
  await Promise.all([
    fillQueue.close(),
    candleQueue.close(),
    deadLetterQueue.close(),
    fillQueueEvents.close(),
    candleQueueEvents.close(),
  ]);
  log.info("Queues closed");
}