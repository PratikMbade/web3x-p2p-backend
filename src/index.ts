import http from "http";
import app               from "./app";
import { config }        from "./config";
import { createLogger, attachProcessHandlers } from "./lib/logger";
import { connectRedis, disconnectRedis }       from "./lib/redis";
import { connectDB, disconnectDB }             from "./lib/prisma";
import { closeQueues }                         from "./queues/fillQueue";
import { startFillWorker, stopFillWorker }     from "./workers/fillWorker";
import { startCandleWorker, stopCandleWorker }  from "./workers/candleworker";
import { syncHistoricalBlocks, startLiveListener, stopIndexer } from "./services/indexer";
import { createWsServer }                                          from "./services/wsServer";

const log = createLogger("server");

// ─── Bootstrap ────────────────────────────────────────────────────────────────
async function bootstrap(): Promise<void> {
  log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  log.info("  P2P Indexer — Step 1: Fill Event Queue");
  log.info(`  Environment: ${config.env}`);
  log.info(`  Contract:    ${config.contract.address}`);
  log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

  // 1. Attach global error handlers → Winston
  attachProcessHandlers();

  // 2. Connect infrastructure
  log.info("Connecting to infrastructure…");
  await connectDB();
  await connectRedis();

  // 3. Start HTTP server
  const httpServer = http.createServer(app);
  await new Promise<void>((resolve) => {
    httpServer.listen(config.port, () => {
      log.info(`HTTP server listening on port ${config.port}`);
      resolve();
    });
  });

  // 3b. Start WebSocket server
  await createWsServer(httpServer);
  log.info("WebSocket server started on /ws");

  // 4. Start fill worker (consumes fill-events queue → Postgres)
  log.info("Starting fill worker…");
  startFillWorker();

  // 4b. Start candle worker (consumes candle-build queue → Candle table)
  log.info("Starting candle worker…");
  startCandleWorker();

  // 5. Historical sync — catch up any missed blocks from last checkpoint
  log.info("Running historical sync…");
  try {
    await syncHistoricalBlocks();
  } catch (err: unknown) {
    // Non-fatal: log and continue — live listener will still work
    log.error("Historical sync encountered an error", {
      error: err instanceof Error ? err.message : String(err),
    });
    log.warn("Continuing with live listener despite sync error…");
  }

  // 6. Start live WebSocket listener
  log.info("Starting live event listener…");
  await startLiveListener();

  log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  log.info("  Indexer fully operational ✓");
  log.info(`  Health:  http://localhost:${config.port}/health`);
  log.info(`  Details: http://localhost:${config.port}/health/detailed`);
  log.info(`  DLQ:     http://localhost:${config.port}/api/dlq`);
  log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
}

// ─── Graceful shutdown ────────────────────────────────────────────────────────
let isShuttingDown = false;

async function shutdown(signal: string): Promise<void> {
  if (isShuttingDown) return;
  isShuttingDown = true;

  log.info(`Shutdown signal received: ${signal}`);
  log.info("Graceful shutdown in progress…");

  try {
    // Order matters:
    // 1. Stop accepting new events first
    await stopIndexer();

    // 2. Let workers finish in-flight jobs
    await stopFillWorker();
    await stopCandleWorker();

    // 3. Close queues
    await closeQueues();

    // 4. Disconnect infrastructure
    await disconnectDB();
    await disconnectRedis();

    log.info("Shutdown complete — goodbye");
    process.exit(0);
  } catch (err: unknown) {
    log.error("Error during shutdown", {
      error: err instanceof Error ? err.message : String(err),
    });
    process.exit(1);
  }
}

process.on("SIGINT",  () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));

// ─── Run ──────────────────────────────────────────────────────────────────────
bootstrap().catch((err: unknown) => {
  log.error("Fatal bootstrap error", {
    error: err instanceof Error ? err.message : String(err),
    stack: err instanceof Error ? err.stack   : undefined,
  });
  process.exit(1);
});