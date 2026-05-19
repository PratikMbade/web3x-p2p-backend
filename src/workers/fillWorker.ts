// src/workers/fillWorker.ts
import { Worker, Job, UnrecoverableError } from "bullmq";
import { bullmqRedis }                     from "../lib/redis";
import { prisma }                          from "../lib/prisma";
import { config }                          from "../config";
import { createLogger }                    from "../lib/logger";
import { moveToDeadLetter, enqueueCandleBuild } from "../queues/fillQueue";
import { FillJobPayload, WorkerResult, CandleJobPayload } from "../types";
import { calculateTotalValue } from "../lib/tradingHelpers";

const log = createLogger("fill-worker");

// ─── Helper: safely convert blockTimestamp to ISO string ─────────────────────
function toISOString(ts: Date | string): string {
  if (typeof ts === "string") return ts;
  return ts.toISOString();
}

function toDate(ts: Date | string): Date {
  return new Date(toISOString(ts));
}

// ─── Helper: build the candle payload safely ──────────────────────────────────
function buildCandlePayload(raw: FillJobPayload["raw"]): CandleJobPayload {
  return {
    fillTxHash:     raw.txHash,
    blockNumber:    raw.blockNumber,
    blockTimestamp: toISOString(raw.blockTimestamp as unknown as Date | string),
    tokenOne:       raw.tokenOne,
    tokenTwo:       raw.tokenTwo,
    fillAmount:     raw.fillAmount,
    pricePerToken:  raw.pricePerToken,
    enqueuedAt:     Date.now(),
  };
}

// ─── Process a single fill job ────────────────────────────────────────────────
async function processFillJob(job: Job<FillJobPayload>): Promise<WorkerResult> {
  const { raw, enqueuedAt } = job.data;
  const start = Date.now();

  log.info("Processing fill job", {
    jobId:          job.id,
    txHash:         raw.txHash,
    blockNumber:    raw.blockNumber,
    attempt:        job.attemptsMade + 1,
    queueLatencyMs: start - enqueuedAt,
  });

  if (!raw.txHash || !raw.tokenOne || !raw.tokenTwo) {
    throw new UnrecoverableError(`Invalid fill payload: missing required fields on job ${job.id}`);
  }
  if (!raw.fillAmount || !raw.pricePerToken) {
    throw new UnrecoverableError(`Invalid fill payload: missing amount/price on tx ${raw.txHash}`);
  }

  // ── Idempotency check ─────────────────────────────────────────────────────
  const existing = await prisma.fillEvent.findUnique({
    where: { txHash: raw.txHash }, select: { id: true },
  });

  if (existing) {
    log.debug("Duplicate fill — already in DB", { txHash: raw.txHash });
    try {
      await enqueueCandleBuild(buildCandlePayload(raw));
    } catch (candleErr: unknown) {
      log.warn("Failed to enqueue candle for existing fill", {
        txHash: raw.txHash,
        error:  candleErr instanceof Error ? candleErr.message : String(candleErr),
      });
    }
    return { status: "duplicate", txHash: raw.txHash };
  }

  const blockTimestamp = toDate(raw.blockTimestamp as unknown as Date | string);
  const totalValue     = calculateTotalValue(raw.fillAmount, raw.pricePerToken);

  // ── Persist everything in one transaction ─────────────────────────────────
  await prisma.$transaction(async (tx:any) => {

    // 1. Write FillEvent (raw source of truth)
    await tx.fillEvent.create({
      data: {
        txHash:         raw.txHash,
        blockNumber:    raw.blockNumber,
        logIndex:       raw.logIndex,
        blockTimestamp,
        tokenOne:       raw.tokenOne,
        tokenTwo:       raw.tokenTwo,
        orderId:        raw.orderId,
        filler:         raw.filler,
        fillAmount:     raw.fillAmount,
        pricePerToken:  raw.pricePerToken,
      },
    });

    // 2. Update OrderCreatedEvent — decrement remaining, mark filled if done
    const order = await tx.orderCreatedEvent.findFirst({
      where:  { tokenOne: raw.tokenOne, tokenTwo: raw.tokenTwo, orderId: raw.orderId },
      select: { id: true, remainingAmt: true, amount: true, creator: true,
                orderType: true, txHash: true, blockTimestamp: true ,pricePerToken:true},
    });

    let isFullyFilled  = false;
    let creatorAddress = "";

    if (order) {
      const currentRemaining = BigInt(order.remainingAmt);
      const filled           = BigInt(raw.fillAmount);
      const newRemaining     = currentRemaining >= filled ? currentRemaining - filled : BigInt(0);
      isFullyFilled  = newRemaining === BigInt(0);
      creatorAddress = order.creator;

      await tx.orderCreatedEvent.update({
        where: { id: order.id },
        data: {
          remainingAmt: newRemaining.toString(),
          status:       isFullyFilled ? "filled" : "open",
          filledAt:     isFullyFilled ? new Date() : undefined,
          updatedAt:    new Date(),
        },
      });

      log.info("Order status updated", {
        orderId:      raw.orderId,
        newRemaining: newRemaining.toString(),
        status:       isFullyFilled ? "filled" : "open",
        isFullyFilled,
      });

      // 3. Write RecentTrade row (UI trading feed)
      await tx.recentTrade.create({
        data: {
          tokenOne:       raw.tokenOne,
          tokenTwo:       raw.tokenTwo,
          fillerAddress:  raw.filler,
          creatorAddress: order.creator,
          orderId:        raw.orderId,
          orderType:      order.orderType,
          fillAmount:     raw.fillAmount,
          pricePerToken:  raw.pricePerToken,
          totalValue,
          fullyFilled:    isFullyFilled,
          remainingAmt:   (currentRemaining >= filled ? currentRemaining - filled : BigInt(0)).toString(),
          txHash:         raw.txHash,
          blockNumber:    raw.blockNumber,
          blockTimestamp,
        },
      });

      // 4. Write P2PActivity for the FILLER
      await tx.p2PActivity.create({
        data: {
          walletAddress:  raw.filler,
          activityType:   "fill_order",
          orderId:        raw.orderId,
          tokenOne:       raw.tokenOne,
          tokenTwo:       raw.tokenTwo,
          amount:         raw.fillAmount,
          pricePerToken:  raw.pricePerToken,
          totalCost:      totalValue,
          orderType:      order.orderType,
          txHash:         raw.txHash,
          blockNumber:    raw.blockNumber,
          blockTimestamp,
        },
      });

      // 5. If fully filled — write ClosedOrder
      if (isFullyFilled) {
        await tx.closedOrder.create({
          data: {
            orderId:         raw.orderId,
            tokenOne:        raw.tokenOne,
            tokenTwo:        raw.tokenTwo,
            creatorAddress:  order.creator,
            orderType:       order.orderType,
            amount:          order.amount,
            pricePerToken:   order.pricePerToken,
            closedStatus:    "filled",
            closedAt:        blockTimestamp,
            closedTxHash:    raw.txHash,
            filledByAddress: raw.filler,
            totalFilled:     order.amount,
            createdTxHash:   order.txHash,
            orderCreatedAt:  order.blockTimestamp,
          },
        });

        log.info("ClosedOrder created (filled)", {
          orderId:  raw.orderId,
          tokenOne: raw.tokenOne.slice(0, 10),
          tokenTwo: raw.tokenTwo.slice(0, 10),
        });
      }
    } else {
      log.warn("OrderCreatedEvent not found for fill — RecentTrade/Activity skipped", {
        orderId:  raw.orderId,
        tokenOne: raw.tokenOne,
        tokenTwo: raw.tokenTwo,
      });
    }

    // 6. Advance indexer checkpoint
    await tx.$executeRaw`
      INSERT INTO "IndexerState" (id, "lastSafeBlock", "lastUpdatedAt")
      VALUES ('singleton', ${raw.blockNumber}, NOW())
      ON CONFLICT (id) DO UPDATE
      SET "lastSafeBlock" = GREATEST("IndexerState"."lastSafeBlock", ${raw.blockNumber}),
          "lastUpdatedAt" = NOW()
    `;
  });

  const durationMs = Date.now() - start;
  log.info("Fill persisted", {
    txHash:      raw.txHash,
    blockNumber: raw.blockNumber,
    tokenOne:    raw.tokenOne.slice(0, 10),
    tokenTwo:    raw.tokenTwo.slice(0, 10),
    durationMs,
  });

  // ── Enqueue candle build ───────────────────────────────────────────────────
  try {
    await enqueueCandleBuild(buildCandlePayload(raw));
  } catch (candleErr: unknown) {
    log.error("Failed to enqueue candle build — reconciliation will catch it", {
      txHash: raw.txHash,
      error:  candleErr instanceof Error ? candleErr.message : String(candleErr),
    });
  }

  return { status: "persisted", txHash: raw.txHash };
}

// ─── Worker instance ──────────────────────────────────────────────────────────
let workerInstance: Worker<FillJobPayload, WorkerResult> | null = null;

export function startFillWorker(): Worker<FillJobPayload, WorkerResult> {
  if (workerInstance) {
    log.warn("Fill worker already running — skipping double-start");
    return workerInstance;
  }

  const worker = new Worker<FillJobPayload, WorkerResult>(
    config.queue.names.fillEvents,
    processFillJob,
    {
      connection:      bullmqRedis,
      concurrency:     config.queue.fillWorkerConcurrency,
      lockDuration:    30_000,
      lockRenewTime:   15_000,
      stalledInterval: 30_000,
      maxStalledCount: 3,
    }
  );

  worker.on("completed", (job, result) => {
    log.info("Worker completed job", { jobId: job.id, result: result.status, txHash: result.status === "persisted"?result.txHash:'' });
  });

  worker.on("failed", async (job, err) => {
    if (!job) return;
    const isUnrecoverable = err instanceof UnrecoverableError;
    const exhausted       = job.attemptsMade >= config.queue.fillJobMaxAttempts;
    log.error("Worker failed job", {
      jobId:         job.id,
      txHash:        job.data.raw.txHash,
      attempt:       job.attemptsMade,
      maxAttempts:   config.queue.fillJobMaxAttempts,
      error:         err.message,
      unrecoverable: isUnrecoverable,
      willRetry:     !exhausted && !isUnrecoverable,
    });
    if (exhausted || isUnrecoverable) {
      await moveToDeadLetter(job.id ?? "unknown", job.data, err, job.attemptsMade);
    }
  });

  worker.on("error",   (err)    => log.error("Worker internal error",    { error: err.message }));
  worker.on("stalled", (jobId)  => log.warn("Job stalled — will retry",  { jobId }));
  worker.on("active",  (job)    => log.debug("Job active", { jobId: job.id, txHash: job.data.raw.txHash }));

  workerInstance = worker;
  log.info("Fill worker started", {
    concurrency: config.queue.fillWorkerConcurrency,
    maxAttempts: config.queue.fillJobMaxAttempts,
  });

  return worker;
}

export async function stopFillWorker(): Promise<void> {
  if (!workerInstance) return;
  await workerInstance.close();
  workerInstance = null;
  log.info("Fill worker stopped");
}