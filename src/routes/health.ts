// src/routes/health.ts
import { Router, Request, Response } from "express";
import { ethers }                    from "ethers";
import { config }                    from "../config";
import { createLogger }              from "../lib/logger";
import { pingRedis }                 from "../lib/redis";
import { prisma }                    from "../lib/prisma";
import { getFillQueueHealth, getDeadLetterHealth } from "../queues/fillQueue";
import { getIndexerState }           from "../services/indexer";
import { IndexerHealth }             from "../types";

const log    = createLogger("health");
const router = Router();

// ─── GET /health ─────────────────────────────────────────────────────────────
// Lightweight — for load-balancer/k8s liveness probe
router.get("/", (_req: Request, res: Response) => {
  res.status(200).json({ status: "ok", ts: new Date().toISOString() });
});

// ─── GET /health/ready ───────────────────────────────────────────────────────
// Deep check — Postgres + Redis + indexer state
router.get("/ready", async (_req: Request, res: Response) => {
  const checks = {
    postgres: false,
    redis:    false,
    indexer:  false,
  };

  try {
    await prisma.$queryRaw`SELECT 1`;
    checks.postgres = true;
  } catch (err: unknown) {
    log.error("Postgres health check failed", {
      error: err instanceof Error ? err.message : String(err),
    });
  }

  try {
    checks.redis = await pingRedis();
  } catch (err: unknown) {
    log.error("Redis health check failed", {
      error: err instanceof Error ? err.message : String(err),
    });
  }

  const { isWsConnected } = getIndexerState();
  checks.indexer = isWsConnected;

  const allHealthy = Object.values(checks).every(Boolean);

  res.status(allHealthy ? 200 : 503).json({
    status: allHealthy ? "ready" : "degraded",
    checks,
    ts:     new Date().toISOString(),
  });
});

// ─── GET /health/detailed ────────────────────────────────────────────────────
// Full metrics — for dashboards and alerts
router.get("/detailed", async (_req: Request, res: Response) => {
  try {
    const [fillHealth, dlqHealth, indexerState, dbState] = await Promise.all([
      getFillQueueHealth(),
      getDeadLetterHealth(),
      Promise.resolve(getIndexerState()),
      prisma.indexerState.findUnique({ where: { id: "singleton" } }),
    ]);

    // Get current block from Alchemy
    let currentBlock = 0;
    try {
      const provider = new ethers.providers.JsonRpcProvider(config.alchemy.httpUrl);
      currentBlock = await provider.getBlockNumber();
    } catch (err: unknown) {
      log.warn("Could not fetch current block for health check", {
        error: err instanceof Error ? err.message : String(err),
      });
    }

    const lastIndexedBlock = dbState?.lastSafeBlock ?? 0;
    const blockLag         = Math.max(0, currentBlock - lastIndexedBlock);

    const health: IndexerHealth = {
      status:
        !indexerState.isWsConnected ? "down"
        : blockLag > 500            ? "degraded"
        : fillHealth.failed > 0     ? "degraded"
        : "healthy",

      wsConnected:      indexerState.isWsConnected,
      lastIndexedBlock,
      currentBlock,
      blockLag,
      queues:           [fillHealth, dlqHealth],
      uptime:           indexerState.uptimeSeconds,
    };

    // Alert log if dead-letter queue has items
    if (dlqHealth.waiting > 0) {
      log.warn("Dead-letter queue has items — investigation required", {
        dlqWaiting: dlqHealth.waiting,
      });
    }

    res.status(health.status === "down" ? 503 : 200).json(health);
  } catch (err: unknown) {
    log.error("Detailed health check failed", {
      error: err instanceof Error ? err.message : String(err),
    });
    res.status(500).json({ status: "error", message: "Health check failed" });
  }
});

export default router;