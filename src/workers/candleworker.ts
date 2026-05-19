// src/workers/candleWorker.ts
import { Worker, Job, UnrecoverableError } from "bullmq";
import { bullmqRedis }                     from "../lib/redis";
import { config }                          from "../config";
import { createLogger }                    from "../lib/logger";
import { buildCandlesFromFill }            from "../services/candlebuilder";
import { CandleJobPayload, CandleWorkerResult } from "../types";

const log = createLogger("candle-worker");

// ─── Process a single candle job ──────────────────────────────────────────────
async function processCandleJob(
  job: Job<CandleJobPayload>
): Promise<CandleWorkerResult> {
  const { fillTxHash, tokenOne, tokenTwo, blockTimestamp, enqueuedAt } = job.data;
  const start = Date.now();

  log.info("Processing candle job", {
    jobId:      job.id,
    fillTxHash: fillTxHash.slice(0, 12),
    tokenOne:   tokenOne.slice(0, 10),
    tokenTwo:   tokenTwo.slice(0, 10),
    attempt:    job.attemptsMade + 1,
    queueLatencyMs: start - enqueuedAt,
  });

  // ── Validate ──────────────────────────────────────────────────────────────
  if (!fillTxHash || !tokenOne || !tokenTwo) {
    throw new UnrecoverableError(
      `Invalid candle payload: missing required fields on job ${job.id}`
    );
  }

  if (!blockTimestamp || isNaN(new Date(blockTimestamp).getTime())) {
    throw new UnrecoverableError(
      `Invalid blockTimestamp on candle job ${job.id}: "${blockTimestamp}"`
    );
  }

  // ── Build candles ─────────────────────────────────────────────────────────
  const updatedIntervals = await buildCandlesFromFill(job.data);

  if (updatedIntervals.length === 0) {
    log.debug("Candle job produced no updates (likely dedup or invalid price)", {
      jobId: job.id, fillTxHash,
    });
    return { status: "duplicate", fillTxHash };
  }

  const durationMs = Date.now() - start;

  log.info("Candle job complete", {
    jobId:      job.id,
    fillTxHash: fillTxHash.slice(0, 12),
    intervals:  updatedIntervals,
    durationMs,
  });

  return {
    status:    "built",
    intervals: updatedIntervals,
    tokenOne,
    tokenTwo,
  };
}

// ─── Worker instance ──────────────────────────────────────────────────────────
let workerInstance: Worker<CandleJobPayload, CandleWorkerResult> | null = null;

export function startCandleWorker(): Worker<CandleJobPayload, CandleWorkerResult> {
  if (workerInstance) {
    log.warn("Candle worker already running — skipping double-start");
    return workerInstance;
  }

  const worker = new Worker<CandleJobPayload, CandleWorkerResult>(
    config.queue.names.candleBuild,
    processCandleJob,
    {
      connection:  bullmqRedis,
      concurrency: config.queue.candleWorkerConcurrency,
      lockDuration:   20_000,
      lockRenewTime:  10_000,
      stalledInterval: 30_000,
      maxStalledCount: 3,
    }
  );

  worker.on("completed", (job, result) => {
    if (result.status === "built") {
      log.info("Candle worker completed", {
        jobId:     job.id,
        intervals: result.intervals,
        tokenOne:  result.tokenOne.slice(0, 10),
        tokenTwo:  result.tokenTwo.slice(0, 10),
      });
    }
  });

  worker.on("failed", (job, err) => {
    if (!job) return;
    log.error("Candle worker failed", {
      jobId:      job.id,
      fillTxHash: job.data.fillTxHash,
      attempt:    job.attemptsMade,
      error:      err.message,
      willRetry:  job.attemptsMade < config.queue.candleJobMaxAttempts,
    });
  });

  worker.on("error", (err) => {
    log.error("Candle worker internal error", { error: err.message });
  });

  worker.on("stalled", (jobId) => {
    log.warn("Candle job stalled", { jobId });
  });

  workerInstance = worker;
  log.info("Candle worker started", {
    concurrency: config.queue.candleWorkerConcurrency,
    maxAttempts: config.queue.candleJobMaxAttempts,
  });

  return worker;
}

export async function stopCandleWorker(): Promise<void> {
  if (!workerInstance) return;
  await workerInstance.close();
  workerInstance = null;
  log.info("Candle worker stopped");
}