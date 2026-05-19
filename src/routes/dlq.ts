// src/routes/dlq.ts
import { Router, Request, Response } from "express";
import { Job }                        from "bullmq";
import { createLogger }               from "../lib/logger";
import { deadLetterQueue, fillQueue } from "../queues/fillQueue";
import { DeadLetterPayload, FillJobPayload } from "../types";

const log    = createLogger("dlq-api");
const router = Router();

// ─── GET /api/dlq ─────────────────────────────────────────────────────────────
// List all dead-letter jobs
router.get("/", async (_req: Request, res: Response) => {
  try {
    const jobs = await deadLetterQueue.getJobs(["waiting", "failed", "completed"], 0, 100);

    const payload = jobs.map((job) => ({
      jobId:         job.id,
      originalJobId: job.data.originalJobId,
      txHash:        job.data.payload.raw.txHash,
      blockNumber:   job.data.payload.raw.blockNumber,
      tokenOne:      job.data.payload.raw.tokenOne,
      tokenTwo:      job.data.payload.raw.tokenTwo,
      failedAt:      job.data.failedAt,
      lastError:     job.data.lastError,
      attemptsMade:  job.data.attemptsMade,
      addedAt:       job.timestamp,
    }));

    res.json({ count: payload.length, jobs: payload });
  } catch (err: unknown) {
    log.error("Failed to list DLQ jobs", {
      error: err instanceof Error ? err.message : String(err),
    });
    res.status(500).json({ error: "Failed to fetch DLQ jobs" });
  }
});

// ─── POST /api/dlq/:jobId/replay ──────────────────────────────────────────────
// Replay a single dead-letter job back into the fill-events queue
router.post("/:jobId/replay", async (req: Request, res: Response) => {
  const { jobId } = req.params;

  try {
    const dlqJob = await deadLetterQueue.getJob(jobId);
    if (!dlqJob) {
      return res.status(404).json({ error: `DLQ job ${jobId} not found` });
    }

    const originalPayload: FillJobPayload = dlqJob.data.payload;
    const originalTxHash = originalPayload.raw.txHash;

    // Re-enqueue with fresh timestamp
    const replayPayload: FillJobPayload = {
      ...originalPayload,
      enqueuedAt: Date.now(),
    };

    const newJob = await fillQueue.add("process-fill-replay", replayPayload, {
      jobId: `replay-${originalTxHash}-${Date.now()}`,
    });

    // Remove from DLQ
    await dlqJob.remove();

    log.info("DLQ job replayed", {
      originalJobId: jobId,
      newJobId:      newJob.id,
      txHash:        originalTxHash,
    });

    return res.json({
      message:       "Job replayed successfully",
      originalJobId: jobId,
      newJobId:      newJob.id,
      txHash:        originalTxHash,
    });
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    log.error("Failed to replay DLQ job", { jobId, error: msg });
    return res.status(500).json({ error: msg });
  }
});

// ─── POST /api/dlq/replay-all ────────────────────────────────────────────────
// Replay all dead-letter jobs (use with caution)
router.post("/replay-all", async (_req: Request, res: Response) => {
  try {
    const jobs: Job<DeadLetterPayload>[] = await deadLetterQueue.getJobs(["waiting"], 0, 500);

    const results: { jobId: string; txHash: string; replayed: boolean; error?: string }[] = [];

    for (const job of jobs) {
      try {
        const originalPayload = job.data.payload;
        const replayPayload: FillJobPayload = {
          ...originalPayload,
          enqueuedAt: Date.now(),
        };

        await fillQueue.add("process-fill-replay", replayPayload, {
          jobId: `replay-${originalPayload.raw.txHash}-${Date.now()}`,
        });

        await job.remove();

        results.push({
          jobId:    job.id ?? "unknown",
          txHash:   originalPayload.raw.txHash,
          replayed: true,
        });
      } catch (err: unknown) {
        results.push({
          jobId:    job.id ?? "unknown",
          txHash:   job.data.payload.raw.txHash,
          replayed: false,
          error:    err instanceof Error ? err.message : String(err),
        });
      }
    }

    const replayed = results.filter((r) => r.replayed).length;
    log.info("DLQ bulk replay complete", { total: jobs.length, replayed });

    return res.json({ total: jobs.length, replayed, results });
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    log.error("Bulk DLQ replay failed", { error: msg });
    return res.status(500).json({ error: msg });
  }
});

// ─── DELETE /api/dlq/:jobId ───────────────────────────────────────────────────
// Permanently remove a DLQ job (only after manual investigation)
router.delete("/:jobId", async (req: Request, res: Response) => {
  const { jobId } = req.params;

  try {
    const job = await deadLetterQueue.getJob(jobId);
    if (!job) {
      return res.status(404).json({ error: `Job ${jobId} not found` });
    }

    const txHash = job.data.payload.raw.txHash;
    await job.remove();

    log.warn("DLQ job manually deleted", { jobId, txHash });
    return res.json({ message: "Job deleted", jobId, txHash });
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    log.error("Failed to delete DLQ job", { jobId, error: msg });
    return res.status(500).json({ error: msg });
  }
});

export default router;