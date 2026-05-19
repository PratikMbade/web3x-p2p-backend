// src/services/indexer.ts
import { ethers } from "ethers";
import { prisma }  from "../lib/prisma";
import { config }  from "../config";
import { createLogger } from "../lib/logger";
import { P2P_ABI }      from "../lib/contractAbi";
import { enqueueFill }  from "../queues/fillQueue";
import { bullmqRedis }  from "../lib/redis";
import IORedis, { Redis }          from "ioredis";
import {
  RawFillEvent,
  RawOrderCreatedEvent,
  RawOrderCancelledEvent,
  GapDetectionResult,
  FillJobPayload,
  OrderBookEvent,
  OrderType,
} from "../types";

const log = createLogger("indexer");

// ─── Module-level state ───────────────────────────────────────────────────────
let wsProvider:       ethers.providers.WebSocketProvider | null = null;
let wsContract:       ethers.Contract | null = null;
let reconnectTimer:   NodeJS.Timeout | null = null;
let keepAliveTimer:   NodeJS.Timeout | null = null;
let isShuttingDown:   boolean = false;
let isWsConnected:    boolean = false;
let lastIndexedBlock: number  = 0;
const startedAt = Date.now();

// ─── Redis channel names ──────────────────────────────────────────────────────
// Step 3 WebSocket server subscribes to these channels to push live UI updates
export const ORDER_BOOK_CHANNEL = (tokenOne: string, tokenTwo: string) =>
  `orderbook:${tokenOne.toLowerCase()}:${tokenTwo.toLowerCase()}`;

// Global channel — every order event regardless of pair
// Frontend clients subscribe to this to receive ALL orders
export const ORDER_BOOK_GLOBAL_CHANNEL = "orderbook:global";

// Global channel — every order event regardless of pair
// Frontend clients subscribe to this to receive ALL orders

// ─── Health state getter ──────────────────────────────────────────────────────
export const getIndexerState = () => ({
  isWsConnected,
  lastIndexedBlock,
  uptimeSeconds: Math.floor((Date.now() - startedAt) / 1000),
});

// ─── Provider factory ─────────────────────────────────────────────────────────
function createWsProvider(): ethers.providers.WebSocketProvider {
  const provider = new ethers.providers.WebSocketProvider(config.alchemy.wsUrl);

  provider._websocket.on("open", () => {
    isWsConnected = true;
    log.info("Alchemy WebSocket connected");
  });

  provider._websocket.on("close", (code: number, reason: string) => {
    isWsConnected = false;
    if (isShuttingDown) return;
    log.warn("Alchemy WebSocket closed — scheduling reconnect", {
      code, reason: reason?.toString() || "unknown",
    });
    scheduleReconnect();
  });

  provider._websocket.on("error", (err: Error) => {
    isWsConnected = false;
    log.error("Alchemy WebSocket error", { error: err.message });
  });

  return provider;
}

function scheduleReconnect(delayMs = 5000): void {
  if (reconnectTimer) clearTimeout(reconnectTimer);
  reconnectTimer = setTimeout(async () => {
    if (isShuttingDown) return;
    log.info("Reconnecting WebSocket listener...");
    try {
      await startLiveListener();
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      log.error("Reconnect failed", { error: msg });
      scheduleReconnect(10_000);
    }
  }, delayMs);
}

// ─── Gap detection ────────────────────────────────────────────────────────────
export async function detectGap(
  httpProvider: ethers.providers.JsonRpcProvider
): Promise<GapDetectionResult> {
  const state        = await prisma.indexerState.findUnique({ where: { id: "singleton" } });
  const currentBlock = await httpProvider.getBlockNumber();
  const dbLastSafe   = state?.lastSafeBlock ?? 0;

  // CRITICAL: Never scan below deployedBlock.
  // Contract did not exist before this block — scanning earlier is wasted RPC calls.
  // Also auto-corrects bad DB state from previous misconfigured runs.
  const lastSafe = Math.max(dbLastSafe, config.contract.deployedBlock - 1);

  if (dbLastSafe < config.contract.deployedBlock - 1) {
    log.warn("DB lastSafeBlock is below deployedBlock — auto-correcting", {
      dbLastSafe,
      deployedBlock: config.contract.deployedBlock,
      clampedTo:     config.contract.deployedBlock - 1,
    });
    await prisma.indexerState.upsert({
      where:  { id: "singleton" },
      create: { id: "singleton", lastSafeBlock: config.contract.deployedBlock - 1, lastUpdatedAt: new Date() },
      update: { lastSafeBlock: config.contract.deployedBlock - 1, lastUpdatedAt: new Date() },
    });
  }

  const processableBlock = currentBlock - config.indexer.confirmationDepth;
  const gapSize          = Math.max(0, processableBlock - lastSafe);

  const result: GapDetectionResult = {
    hasGap:        gapSize > 0,
    gapStartBlock: lastSafe + 1,
    gapEndBlock:   processableBlock,
    gapSizeBlocks: gapSize,
  };

  if (result.hasGap) {
    log.warn("Block gap detected", {
      gapStartBlock: result.gapStartBlock,
      gapEndBlock:   result.gapEndBlock,
      gapSizeBlocks: result.gapSizeBlocks,
      deployedBlock: config.contract.deployedBlock,
    });
  } else {
    log.info("No block gap — indexer is up to date", { lastSafeBlock: lastSafe, currentBlock });
  }

  return result;
}

// ─── Timestamp resolver ───────────────────────────────────────────────────────
// Uses the on-chain timestamp from event args when available.
// Saves one getBlock() RPC call per event — meaningful at scale.
async function resolveTimestamp(
  eventTimestamp: ethers.BigNumber | undefined,
  blockNumber:    number,
  provider:       ethers.providers.BaseProvider,
  txHash:         string
): Promise<Date> {
  if (eventTimestamp && eventTimestamp.toString() !== "0") {
    return new Date(Number(eventTimestamp.toString()) * 1000);
  }
  try {
    const block = await provider.getBlock(blockNumber);
    return new Date(block.timestamp * 1000);
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    log.error("Failed to fetch block timestamp", { blockNumber, txHash, error: msg });
    throw err;
  }
}

// ─── Redis publish ────────────────────────────────────────────────────────────
// Publishes order book events to Redis pub/sub so Step 3 WebSocket can
// broadcast live updates to connected frontend clients.
async function publishOrderBookEvent(event: OrderBookEvent): Promise<void> {
  try {
    const redis   = bullmqRedis as Redis;
    const message = JSON.stringify(event);

    // Publish to pair-specific channel AND global channel simultaneously
    // Pair channel: clients watching a specific tokenOne/tokenTwo pair
    // Global channel: clients watching ALL orders (order book page, admin, etc.)
    await Promise.all([
      redis.publish(ORDER_BOOK_CHANNEL(event.tokenOne, event.tokenTwo), message),
      redis.publish(ORDER_BOOK_GLOBAL_CHANNEL, message),
    ]);

    log.debug("Order book event published to Redis", {
      type:    event.type,
      orderId: event.txHash.slice(0, 12),
      pairChannel:  ORDER_BOOK_CHANNEL(event.tokenOne, event.tokenTwo),
    });
  } catch (err: unknown) {
    log.warn("Redis publish failed (non-fatal — data safe in DB)", {
      type:  event.type,
      error: err instanceof Error ? err.message : String(err),
    });
  }
}

// ─── Parse OrderFilled ────────────────────────────────────────────────────────
async function parseOrderFilled(
  event:        ethers.Event,
  provider:     ethers.providers.BaseProvider,
  httpContract: ethers.Contract
): Promise<RawFillEvent | null> {
  if (!event.args) {
    log.warn("OrderFilled has no args", { txHash: event.transactionHash });
    return null;
  }

  const { filler, tokenToBuy, tokenToExchange, orderId, filledAmount, timestamp } = event.args;

  if (!filler || !tokenToBuy || !tokenToExchange || !orderId || !filledAmount) {
    log.warn("OrderFilled missing required args", {
      txHash:      event.transactionHash,
      hasFiller:   !!filler,
      hasTokenBuy: !!tokenToBuy,
      hasTokenEx:  !!tokenToExchange,
      hasOrderId:  !!orderId,
      hasFillAmt:  !!filledAmount,
    });
    return null;
  }

  const blockTimestamp = await resolveTimestamp(
    timestamp, event.blockNumber, provider, event.transactionHash
  );

  // pricePerToken is NOT in OrderFilled event — must fetch from orderDetails()
  let pricePerToken = "0";
  try {
    const details = await httpContract.orderDetails(tokenToBuy, tokenToExchange, orderId);
    pricePerToken  = details.pricePerToken.toString();
    log.debug("Price fetched from orderDetails", {
      txHash: event.transactionHash, orderId: orderId.toString(), pricePerToken,
    });
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    log.error("orderDetails() failed — cannot resolve price", {
      txHash: event.transactionHash, orderId: orderId.toString(), error: msg,
    });
    throw new Error(`Failed to fetch pricePerToken for tx ${event.transactionHash}: ${msg}`);
  }

  return {
    txHash:         event.transactionHash,
    blockNumber:    event.blockNumber,
    blockTimestamp,
    logIndex:       event.logIndex,
    tokenOne:       tokenToBuy.toLowerCase(),
    tokenTwo:       tokenToExchange.toLowerCase(),
    orderId:        orderId.toString(),
    filler:         filler.toLowerCase(),
    fillAmount:     filledAmount.toString(),
    pricePerToken,
  };
}

// ─── Parse OrderCreated ───────────────────────────────────────────────────────
async function parseOrderCreated(
  event:    ethers.Event,
  provider: ethers.providers.BaseProvider
): Promise<RawOrderCreatedEvent | null> {
  if (!event.args) {
    log.warn("OrderCreated has no args", { txHash: event.transactionHash });
    return null;
  }

  const {
    creator, tokenToBuy, tokenToExchange,
    orderId, orderType, amount, pricePerToken, timestamp,
  } = event.args;

  if (!creator || !tokenToBuy || !tokenToExchange || !orderId) {
    log.warn("OrderCreated missing required args", { txHash: event.transactionHash });
    return null;
  }

  // ── Address validation ────────────────────────────────────────────────────
  // Log the full raw addresses so we can detect frontend bugs where wrong
  // addresses are passed to createOrder() (e.g. wallet address instead of token)
  log.info("OrderCreated raw addresses", {
    txHash:          event.transactionHash,
    creator:         creator.toString(),
    tokenToBuy:      tokenToBuy.toString(),
    tokenToExchange: tokenToExchange.toString(),
    orderId:         orderId.toString(),
  });

  // Warn if tokenToBuy === creator — this means the frontend passed
  // the wallet address as tokenToBuy instead of an ERC20 token address
  if (creator.toLowerCase() === tokenToBuy.toLowerCase()) {
    log.warn("FRONTEND BUG: tokenToBuy equals creator address — wrong address passed to createOrder()", {
      txHash:     event.transactionHash,
      creator:    creator.toString(),
      tokenToBuy: tokenToBuy.toString(),
      hint:       "createOrder(tokenToBuy, tokenToExchange, ...) — tokenToBuy must be an ERC20 contract address, not a wallet address",
    });
  }

  const blockTimestamp = await resolveTimestamp(
    timestamp, event.blockNumber, provider, event.transactionHash
  );

  return {
    txHash:         event.transactionHash,
    blockNumber:    event.blockNumber,
    blockTimestamp,
    logIndex:       event.logIndex,
    creator:        creator.toString().toLowerCase(),
    tokenOne:       tokenToBuy.toString().toLowerCase(),
    tokenTwo:       tokenToExchange.toString().toLowerCase(),
    orderId:        orderId.toString(),
    orderType:      orderType?.toString() === "1" ? "buy" : "sell" as OrderType,
    amount:         amount?.toString() ?? "0",
    pricePerToken:  pricePerToken?.toString() ?? "0",
  };
}

// ─── Parse OrderCancelled ─────────────────────────────────────────────────────
async function parseOrderCancelled(
  event:    ethers.Event,
  provider: ethers.providers.BaseProvider
): Promise<RawOrderCancelledEvent | null> {
  if (!event.args) {
    log.warn("OrderCancelled has no args", { txHash: event.transactionHash });
    return null;
  }

  const { canceller, tokenToBuy, tokenToExchange, orderId, timestamp } = event.args;

  if (!canceller || !tokenToBuy || !tokenToExchange || !orderId) {
    log.warn("OrderCancelled missing required args", { txHash: event.transactionHash });
    return null;
  }

  const blockTimestamp = await resolveTimestamp(
    timestamp, event.blockNumber, provider, event.transactionHash
  );

  return {
    txHash:         event.transactionHash,
    blockNumber:    event.blockNumber,
    blockTimestamp,
    logIndex:       event.logIndex,
    canceller:      canceller.toLowerCase(),
    tokenOne:       tokenToBuy.toLowerCase(),
    tokenTwo:       tokenToExchange.toLowerCase(),
    orderId:        orderId.toString(),
  };
}

// ─── Persist OrderCreated ─────────────────────────────────────────────────────
async function persistOrderCreated(raw: RawOrderCreatedEvent): Promise<void> {
  const existing = await prisma.orderCreatedEvent.findUnique({
    where: { txHash: raw.txHash }, select: { id: true },
  });
  if (existing) {
    log.debug("Duplicate OrderCreated — skipping", { txHash: raw.txHash });
    return;
  }

  await prisma.orderCreatedEvent.create({
    data: {
      txHash:         raw.txHash,
      blockNumber:    raw.blockNumber,
      logIndex:       raw.logIndex,
      blockTimestamp: raw.blockTimestamp,
      creator:        raw.creator,
      tokenOne:       raw.tokenOne,
      tokenTwo:       raw.tokenTwo,
      orderId:        raw.orderId,
      orderType:      raw.orderType,
      amount:         raw.amount,
      pricePerToken:  raw.pricePerToken,
      remainingAmt:   raw.amount,   // starts equal to amount — decremented on fills
      status:         "open",
    },
  });

  // Write P2PActivity for the creator
  try {
    await prisma.p2PActivity.create({
      data: {
        walletAddress:  raw.creator,
        activityType:   "create_order",
        orderId:        raw.orderId,
        tokenOne:       raw.tokenOne,
        tokenTwo:       raw.tokenTwo,
        amount:         raw.amount,
        pricePerToken:  raw.pricePerToken,
        orderType:      raw.orderType,
        txHash:         raw.txHash,
        blockNumber:    raw.blockNumber,
        blockTimestamp: raw.blockTimestamp,
      },
    });
  } catch (err: unknown) {
    log.warn("Failed to write P2PActivity for OrderCreated (non-fatal)", {
      txHash: raw.txHash,
      error:  err instanceof Error ? err.message : String(err),
    });
  }

  log.info("OrderCreated persisted", {
    txHash:        raw.txHash,
    orderId:       raw.orderId,
    creator:       raw.creator.slice(0, 10),
    orderType:     raw.orderType,
    tokenOne:      raw.tokenOne.slice(0, 10),
    tokenTwo:      raw.tokenTwo.slice(0, 10),
    pricePerToken: raw.pricePerToken,
  });

  await publishOrderBookEvent({
    type:      "ORDER_CREATED",
    txHash:    raw.txHash,
    tokenOne:  raw.tokenOne,
    tokenTwo:  raw.tokenTwo,
    orderId:   raw.orderId,
    timestamp: Math.floor(raw.blockTimestamp.getTime() / 1000),
    data: {
      creator:       raw.creator,
      orderType:     raw.orderType,
      amount:        raw.amount,
      pricePerToken: raw.pricePerToken,
    },
  });
}

// ─── Persist OrderCancelled ───────────────────────────────────────────────────
async function persistOrderCancelled(raw: RawOrderCancelledEvent): Promise<void> {
  const existing = await prisma.orderCancelledEvent.findUnique({
    where: { txHash: raw.txHash }, select: { id: true },
  });
  if (existing) {
    log.debug("Duplicate OrderCancelled — skipping", { txHash: raw.txHash });
    return;
  }

  await prisma.orderCancelledEvent.create({
    data: {
      txHash:         raw.txHash,
      blockNumber:    raw.blockNumber,
      logIndex:       raw.logIndex,
      blockTimestamp: raw.blockTimestamp,
      canceller:      raw.canceller,
      tokenOne:       raw.tokenOne,
      tokenTwo:       raw.tokenTwo,
      orderId:        raw.orderId,
    },
  });

  // Update the order status to cancelled in OrderCreatedEvent
  try {
    const order = await prisma.orderCreatedEvent.findFirst({
      where: { tokenOne: raw.tokenOne, tokenTwo: raw.tokenTwo, orderId: raw.orderId },
      select: { id: true },
    });
    if (order) {
      await prisma.orderCreatedEvent.update({
        where: { id: order.id },
        data: {
          status:      "cancelled",
          cancelledAt: raw.blockTimestamp,
          updatedAt:   new Date(),
        },
      });
    }
  } catch (err: unknown) {
    log.warn("Failed to update order status to cancelled (non-fatal)", {
      orderId: raw.orderId,
      error:   err instanceof Error ? err.message : String(err),
    });
  }

  // Write P2PActivity for canceller + ClosedOrder
  try {
    await prisma.p2PActivity.create({
      data: {
        walletAddress:  raw.canceller,
        activityType:   "cancel_order",
        orderId:        raw.orderId,
        tokenOne:       raw.tokenOne,
        tokenTwo:       raw.tokenTwo,
        amount:         "0",
        pricePerToken:  "0",
        txHash:         raw.txHash,
        blockNumber:    raw.blockNumber,
        blockTimestamp: raw.blockTimestamp,
      },
    });

    // Write ClosedOrder for cancelled
    const originalOrder = await prisma.orderCreatedEvent.findFirst({
      where:  { tokenOne: raw.tokenOne, tokenTwo: raw.tokenTwo, orderId: raw.orderId },
      select: { creator: true, orderType: true, amount: true, pricePerToken: true,
                txHash: true, blockTimestamp: true, remainingAmt: true },
    });

    if (originalOrder) {
      const totalFilled = (
        BigInt(originalOrder.amount) - BigInt(originalOrder.remainingAmt)
      ).toString();

      await prisma.closedOrder.create({
        data: {
          orderId:        raw.orderId,
          tokenOne:       raw.tokenOne,
          tokenTwo:       raw.tokenTwo,
          creatorAddress: originalOrder.creator,
          orderType:      originalOrder.orderType,
          amount:         originalOrder.amount,
          pricePerToken:  originalOrder.pricePerToken,
          closedStatus:   "cancelled",
          closedAt:       raw.blockTimestamp,
          closedTxHash:   raw.txHash,
          totalFilled,
          createdTxHash:  originalOrder.txHash,
          orderCreatedAt: originalOrder.blockTimestamp,
        },
      });
    }
  } catch (err: unknown) {
    log.warn("Failed to write P2PActivity/ClosedOrder for OrderCancelled (non-fatal)", {
      txHash: raw.txHash,
      error:  err instanceof Error ? err.message : String(err),
    });
  }

  log.info("OrderCancelled persisted", {
    txHash:    raw.txHash,
    orderId:   raw.orderId,
    canceller: raw.canceller.slice(0, 10),
    tokenOne:  raw.tokenOne.slice(0, 10),
    tokenTwo:  raw.tokenTwo.slice(0, 10),
  });

  await publishOrderBookEvent({
    type:      "ORDER_CANCELLED",
    txHash:    raw.txHash,
    tokenOne:  raw.tokenOne,
    tokenTwo:  raw.tokenTwo,
    orderId:   raw.orderId,
    timestamp: Math.floor(raw.blockTimestamp.getTime() / 1000),
    data: { canceller: raw.canceller },
  });
}

// ─── Enqueue fill event ───────────────────────────────────────────────────────
async function enqueueRawFill(raw: RawFillEvent): Promise<void> {
  const payload: FillJobPayload = { raw, enqueuedAt: Date.now() };
  await enqueueFill(payload);
  lastIndexedBlock = Math.max(lastIndexedBlock, raw.blockNumber);
}

// ─── Historical sync ──────────────────────────────────────────────────────────
// Fetches all three event types in ONE parallel queryFilter call per batch.
// Single pass = 3x fewer Alchemy round trips vs scanning each event separately.
export async function syncHistoricalBlocks(): Promise<void> {
  log.info("Starting historical sync...");

  const httpProvider = new ethers.providers.JsonRpcProvider(config.alchemy.httpUrl);
  const httpContract = new ethers.Contract(config.contract.address, P2P_ABI, httpProvider);

  const gap = await detectGap(httpProvider);
  if (!gap.hasGap) {
    log.info("Historical sync skipped — no gap");
    return;
  }

  const { gapStartBlock, gapEndBlock, gapSizeBlocks } = gap;
  log.info("Syncing historical blocks", {
    fromBlock:        gapStartBlock,
    toBlock:          gapEndBlock,
    totalBlocks:      gapSizeBlocks,
    estimatedBatches: Math.ceil(gapSizeBlocks / config.indexer.blockBatchSize),
    scanning:         ["OrderFilled", "OrderCreated", "OrderCancelled"],
  });

  const fillFilter    = httpContract.filters.OrderFilled();
  const createdFilter = httpContract.filters.OrderCreated();
  const cancelFilter  = httpContract.filters.OrderCancelled();

  let fromBlock      = gapStartBlock;
  let totalFills     = 0;
  let totalCreated   = 0;
  let totalCancelled = 0;
  let batchNum       = 0;

  while (fromBlock <= gapEndBlock) {
    if (isShuttingDown) {
      log.warn("Shutdown signal — halting historical sync", { fromBlock });
      break;
    }

    const toBlock = Math.min(fromBlock + config.indexer.blockBatchSize - 1, gapEndBlock);
    batchNum++;

    try {
      // Fetch all three event types in parallel — single round trip per type
      const [fillEvents, createdEvents, cancelledEvents] = await Promise.all([
        httpContract.queryFilter(fillFilter,    fromBlock, toBlock),
        httpContract.queryFilter(createdFilter, fromBlock, toBlock),
        httpContract.queryFilter(cancelFilter,  fromBlock, toBlock),
      ]);

      const currentBlock = await httpProvider.getBlockNumber();

      // Process OrderFilled
      for (const event of fillEvents) {
        if (currentBlock - event.blockNumber < config.indexer.confirmationDepth) continue;
        const fill = await parseOrderFilled(event as ethers.Event, httpProvider, httpContract);
        if (fill) { await enqueueRawFill(fill); totalFills++; }
      }

      // Process OrderCreated
      for (const event of createdEvents) {
        if (currentBlock - event.blockNumber < config.indexer.confirmationDepth) continue;
        const created = await parseOrderCreated(event as ethers.Event, httpProvider);
        if (created) { await persistOrderCreated(created); totalCreated++; }
      }

      // Process OrderCancelled
      for (const event of cancelledEvents) {
        if (currentBlock - event.blockNumber < config.indexer.confirmationDepth) continue;
        const cancelled = await parseOrderCancelled(event as ethers.Event, httpProvider);
        if (cancelled) { await persistOrderCancelled(cancelled); totalCancelled++; }
      }

      // Save checkpoint after each successful batch
      await prisma.$executeRaw`
        UPDATE "IndexerState"
        SET "lastSafeBlock" = GREATEST("lastSafeBlock", ${toBlock}),
            "lastUpdatedAt" = NOW()
        WHERE id = 'singleton'
      `;

      log.info("Batch complete", {
        batchNum,
        fromBlock,
        toBlock,
        eventsFound: fillEvents.length + createdEvents.length + cancelledEvents.length,
        fills:       fillEvents.length,
        created:     createdEvents.length,
        cancelled:   cancelledEvents.length,
      });

      fromBlock = toBlock + 1;
      await sleep(100);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      log.error("Batch scan failed — retrying in 5s", { batchNum, fromBlock, toBlock, error: msg });
      await sleep(5000);
    }
  }

  log.info("Historical sync complete", {
    processedBlocks: gapSizeBlocks,
    batches:         batchNum,
    totalFills,
    totalCreated,
    totalCancelled,
  });
}

// ─── Live WebSocket listener ──────────────────────────────────────────────────
export async function startLiveListener(): Promise<void> {
  await destroyWsConnection();

  log.info("Starting live WebSocket listener...");
  wsProvider = createWsProvider();
  wsContract = new ethers.Contract(config.contract.address, P2P_ABI, wsProvider);

  // HTTP contract for orderDetails() calls — WS provider is unreliable for eth_call
  const liveHttpProvider = new ethers.providers.JsonRpcProvider(config.alchemy.httpUrl);
  const liveHttpContract = new ethers.Contract(config.contract.address, P2P_ABI, liveHttpProvider);

  // ── OrderFilled — wait for confirmation depth before processing ─────────────
  // Uses polling instead of a fixed sleep — processes as soon as confirmed.
  // On BSC with CONFIRMATION_DEPTH=3, this typically resolves in ~3-9 seconds.
  wsContract.on("OrderFilled", async (...args: unknown[]) => {
    const event = args[args.length - 1] as ethers.Event;
    try {
      // Poll until enough confirmations — check every blockTime instead of sleeping
      // the full estimated wait upfront (avoids over-waiting when blocks are fast)
      let confirmations = 0;
      let attempts = 0;
      const maxAttempts = 30; // give up after 30 × blockTimeMs

      while (attempts < maxAttempts) {
        const currentBlock = await wsProvider!.getBlockNumber();
        confirmations = currentBlock - event.blockNumber;

        if (confirmations >= config.indexer.confirmationDepth) break;

        log.debug("Waiting for confirmations on fill", {
          txHash:       event.transactionHash,
          confirmations,
          required:     config.indexer.confirmationDepth,
          attempt:      attempts + 1,
        });

        await sleep(config.indexer.blockTimeMs);
        attempts++;
      }

      if (confirmations < config.indexer.confirmationDepth) {
        log.warn("Fill event timed out waiting for confirmations — skipping", {
          txHash:       event.transactionHash,
          confirmations,
          required:     config.indexer.confirmationDepth,
        });
        return;
      }

      const fill = await parseOrderFilled(event, wsProvider!, liveHttpContract);
      if (fill) {
        await enqueueRawFill(fill);
        log.info("Live OrderFilled enqueued", {
          txHash:   fill.txHash,
          orderId:  fill.orderId,
          tokenOne: fill.tokenOne.slice(0, 10),
          tokenTwo: fill.tokenTwo.slice(0, 10),
        });
      }
    } catch (err: unknown) {
      log.error("Live OrderFilled handler error", {
        txHash: event.transactionHash,
        error:  err instanceof Error ? err.message : String(err),
      });
    }
  });

  // ── OrderCreated — no confirmation wait needed (UI only, not financial) ────
  wsContract.on("OrderCreated", async (...args: unknown[]) => {
    const event = args[args.length - 1] as ethers.Event;
    try {
      const created = await parseOrderCreated(event, wsProvider!);
      if (created) {
        await persistOrderCreated(created);
        log.info("Live OrderCreated persisted", {
          txHash:    created.txHash,
          orderId:   created.orderId,
          creator:   created.creator.slice(0, 10),
          orderType: created.orderType,
          tokenOne:  created.tokenOne.slice(0, 10),
          tokenTwo:  created.tokenTwo.slice(0, 10),
        });
      }
    } catch (err: unknown) {
      log.error("Live OrderCreated handler error", {
        txHash: event.transactionHash,
        error:  err instanceof Error ? err.message : String(err),
      });
    }
  });

  // ── OrderCancelled — no confirmation wait needed (UI only) ─────────────────
  wsContract.on("OrderCancelled", async (...args: unknown[]) => {
    const event = args[args.length - 1] as ethers.Event;
    try {
      const cancelled = await parseOrderCancelled(event, wsProvider!);
      if (cancelled) {
        await persistOrderCancelled(cancelled);
        log.info("Live OrderCancelled persisted", {
          txHash:    cancelled.txHash,
          orderId:   cancelled.orderId,
          canceller: cancelled.canceller.slice(0, 10),
          tokenOne:  cancelled.tokenOne.slice(0, 10),
          tokenTwo:  cancelled.tokenTwo.slice(0, 10),
        });
      }
    } catch (err: unknown) {
      log.error("Live OrderCancelled handler error", {
        txHash: event.transactionHash,
        error:  err instanceof Error ? err.message : String(err),
      });
    }
  });

  // Keep-alive ping every 30s to prevent Alchemy from dropping idle connections
  if (keepAliveTimer) clearInterval(keepAliveTimer);
  keepAliveTimer = setInterval(async () => {
    try { await wsProvider?.getBlockNumber(); }
    catch (err: unknown) {
      log.warn("Keep-alive ping failed", {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }, 30_000);

  log.info("Live WebSocket listener active", {
    listening: ["OrderFilled", "OrderCreated", "OrderCancelled"],
  });
}

// ─── Destroy WS connection cleanly ───────────────────────────────────────────
async function destroyWsConnection(): Promise<void> {
  if (keepAliveTimer) { clearInterval(keepAliveTimer); keepAliveTimer = null; }
  if (wsContract)     { wsContract.removeAllListeners(); wsContract = null; }
  if (wsProvider) {
    try { wsProvider.removeAllListeners(); wsProvider.destroy(); }
    catch (err: unknown) {
      log.debug("WS provider destroy error (safe to ignore)", {
        error: err instanceof Error ? err.message : String(err),
      });
    }
    wsProvider = null;
  }
  isWsConnected = false;
}

// ─── Graceful shutdown ────────────────────────────────────────────────────────
export async function stopIndexer(): Promise<void> {
  log.info("Stopping indexer...");
  isShuttingDown = true;
  if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
  await destroyWsConnection();
  log.info("Indexer stopped");
}

// ─── Utils ────────────────────────────────────────────────────────────────────
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}