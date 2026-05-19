// src/services/wsServer.ts
import { WebSocketServer, WebSocket } from "ws";
import { Server }                     from "http";
import IORedis, { Redis }                        from "ioredis";
import { bullmqRedis, bullmqSubRedis } from "../lib/redis";
import { prisma }                     from "../lib/prisma";
import { createLogger }               from "../lib/logger";
import { ORDER_BOOK_CHANNEL, ORDER_BOOK_GLOBAL_CHANNEL } from "./indexer";
import { CandleInterval, OHLCVCandle } from "../types";

const log = createLogger("ws-server");

// ─── Redis key helpers (inline — avoids importing from old redis lib) ─────────
const candleChannel     = (t1: string, t2: string, iv: string) =>
  `candle:${t1.toLowerCase()}:${t2.toLowerCase()}:${iv}`;
const candleHistoryKey  = (t1: string, t2: string, iv: string, limit: number) =>
  `candle_history:${t1.toLowerCase()}:${t2.toLowerCase()}:${iv}:${limit}`;
const HISTORY_TTL = 15; // seconds

// ─── Per-client subscription tracking ────────────────────────────────────────
// ws → Set of Redis channel strings the client is subscribed to
const clientSubs    = new Map<WebSocket, Set<string>>();
// channel → Set of WebSocket clients subscribed to it
const channelClients = new Map<string, Set<WebSocket>>();

// ─── Inbound message types ────────────────────────────────────────────────────
type InboundType =
  | "SUBSCRIBE_CANDLES"
  | "UNSUBSCRIBE_CANDLES"
  | "SUBSCRIBE_ORDERBOOK"
  | "UNSUBSCRIBE_ORDERBOOK"
  | "SUBSCRIBE_ALL_ORDERS"
  | "UNSUBSCRIBE_ALL_ORDERS"
  | "PONG";

interface InboundMessage {
  type:    InboundType;
  payload: Record<string, unknown>;
}

interface CandleSubscribePayload {
  tokenOne: string;
  tokenTwo: string;
  interval: CandleInterval;
}

interface OrderBookSubscribePayload {
  tokenOne: string;
  tokenTwo: string;
}

// ─── Bootstrap ────────────────────────────────────────────────────────────────
export async function createWsServer(httpServer: Server): Promise<WebSocketServer> {
  const wss = new WebSocketServer({ server: httpServer, path: "/ws" });

  // ── Redis pub/sub subscriptions ──────────────────────────────────────────────
  // Single pSubscribe per pattern — all matching channels fan out to clients

  // Candle updates (published by candleBuilder.ts)
  await (bullmqSubRedis as Redis).psubscribe("candle:*", (err) => {
    if (err) log.error("Failed to subscribe to candle:*", { error: err.message });
    else     log.info("Redis subscribed to candle:*");
  });

  // Order book pair-specific updates
  await (bullmqSubRedis as Redis).psubscribe("orderbook:*", (err) => {
    if (err) log.error("Failed to subscribe to orderbook:*", { error: err.message });
    else     log.info("Redis subscribed to orderbook:*");
  });

  // Route all incoming Redis messages to subscribed WebSocket clients
  (bullmqSubRedis as Redis).on("pmessage", (_pattern, channel, message) => {
    broadcastToChannel(channel, message);
  });

  // ── WebSocket connection handler ─────────────────────────────────────────────
  wss.on("connection", (ws: WebSocket, req) => {
    clientSubs.set(ws, new Set());

    log.info("Client connected", {
      total: wss.clients.size,
      ip:    req.socket.remoteAddress,
    });

    // Keep-alive ping every 30s
    const pingTimer = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        safeSend(ws, { type: "PING", payload: { ts: Date.now() } });
      } else {
        clearInterval(pingTimer);
      }
    }, 30_000);

    ws.on("message", async (raw) => {
      let msg: InboundMessage;
      try {
        msg = JSON.parse(raw.toString()) as InboundMessage;
      } catch {
        safeSend(ws, { type: "ERROR", payload: { message: "Invalid JSON" } });
        return;
      }

      try {
        await handleMessage(ws, msg);
      } catch (err: unknown) {
        const error = err instanceof Error ? err.message : String(err);
        log.error("Message handler error", { type: msg.type, error });
        safeSend(ws, { type: "ERROR", payload: { message: "Internal error" } });
      }
    });

    ws.on("close", (code, reason) => {
      clearInterval(pingTimer);
      disconnectClient(ws);
      log.info("Client disconnected", {
        total:  wss.clients.size,
        code,
        reason: reason.toString() || "unknown",
      });
    });

    ws.on("error", (err) => {
      log.error("Client WebSocket error", { error: err.message });
      clearInterval(pingTimer);
      disconnectClient(ws);
    });
  });

  log.info("WebSocket server ready", { path: "/ws" });
  return wss;
}

// ─── Message router ───────────────────────────────────────────────────────────
async function handleMessage(ws: WebSocket, msg: InboundMessage): Promise<void> {
  switch (msg.type) {

    // ── Candle subscriptions ─────────────────────────────────────────────────
    case "SUBSCRIBE_CANDLES": {
      const p = msg.payload as Partial<CandleSubscribePayload>;
      if (!p.tokenOne || !p.tokenTwo || !p.interval) {
        safeSend(ws, { type: "ERROR", payload: { message: "SUBSCRIBE_CANDLES requires tokenOne, tokenTwo, interval" } });
        return;
      }
      await handleSubscribeCandles(ws, p.tokenOne, p.tokenTwo, p.interval);
      break;
    }

    case "UNSUBSCRIBE_CANDLES": {
      const p = msg.payload as Partial<CandleSubscribePayload>;
      if (p.tokenOne && p.tokenTwo && p.interval) {
        removeSubscription(ws, candleChannel(p.tokenOne, p.tokenTwo, p.interval));
      }
      break;
    }

    // ── Subscribe to ALL orders across all pairs ────────────────────────────
    // Use this when you want every order event regardless of token pair
    case "SUBSCRIBE_ALL_ORDERS": {
      addSubscription(ws, ORDER_BOOK_GLOBAL_CHANNEL);
      log.debug("Client subscribed to global order book");

      // Send snapshot of recent open orders across all pairs
      const allOrders = await prisma.orderCreatedEvent.findMany({
        where:   { status: "open" },
        orderBy: { blockTimestamp: "desc" },
        take:    100,
      });
      safeSend(ws, {
        type:    "ORDERBOOK_SNAPSHOT",
        payload: { global: true, orders: allOrders },
      });
      break;
    }

    case "UNSUBSCRIBE_ALL_ORDERS": {
      removeSubscription(ws, ORDER_BOOK_GLOBAL_CHANNEL);
      break;
    }

    // ── Order book subscriptions ─────────────────────────────────────────────
    case "SUBSCRIBE_ORDERBOOK": {
      const p = msg.payload as Partial<OrderBookSubscribePayload>;
      if (!p.tokenOne || !p.tokenTwo) {
        safeSend(ws, { type: "ERROR", payload: { message: "SUBSCRIBE_ORDERBOOK requires tokenOne, tokenTwo" } });
        return;
      }
      await handleSubscribeOrderBook(ws, p.tokenOne, p.tokenTwo);
      break;
    }

    case "UNSUBSCRIBE_ORDERBOOK": {
      const p = msg.payload as Partial<OrderBookSubscribePayload>;
      if (p.tokenOne && p.tokenTwo) {
        removeSubscription(ws, ORDER_BOOK_CHANNEL(p.tokenOne, p.tokenTwo));
      }
      break;
    }

    // ── Heartbeat ────────────────────────────────────────────────────────────
    case "PONG":
      // Client is alive — nothing to do
      break;

    default:
      safeSend(ws, { type: "ERROR", payload: { message: `Unknown type: ${msg.type}` } });
  }
}

// ─── SUBSCRIBE_CANDLES handler ────────────────────────────────────────────────
async function handleSubscribeCandles(
  ws:       WebSocket,
  tokenOne: string,
  tokenTwo: string,
  interval: CandleInterval
): Promise<void> {
  const channel = candleChannel(tokenOne, tokenTwo, interval);
  addSubscription(ws, channel);

  log.debug("Client subscribed to candles", {
    tokenOne: tokenOne.slice(0, 10),
    tokenTwo: tokenTwo.slice(0, 10),
    interval,
  });

  // Send full history immediately on subscribe
  const history = await fetchCandleHistory(tokenOne, tokenTwo, interval, 500);

  safeSend(ws, {
    type:    "CANDLE_HISTORY",
    payload: { tokenOne, tokenTwo, interval, candles: history },
  });
}

// ─── SUBSCRIBE_ORDERBOOK handler ──────────────────────────────────────────────
async function handleSubscribeOrderBook(
  ws:       WebSocket,
  tokenOne: string,
  tokenTwo: string
): Promise<void> {
  const channel = ORDER_BOOK_CHANNEL(tokenOne, tokenTwo);
  addSubscription(ws, channel);

  log.debug("Client subscribed to order book", {
    tokenOne: tokenOne.slice(0, 10),
    tokenTwo: tokenTwo.slice(0, 10),
  });

  // Send open orders snapshot immediately on subscribe
  const orders = await prisma.orderCreatedEvent.findMany({
    where: {
      tokenOne: tokenOne.toLowerCase(),
      tokenTwo: tokenTwo.toLowerCase(),
      status:   "open",
    },
    orderBy: { blockTimestamp: "desc" },
    take:    50,
  });

  safeSend(ws, {
    type:    "ORDERBOOK_SNAPSHOT",
    payload: { tokenOne, tokenTwo, orders },
  });
}

// ─── Subscription management ──────────────────────────────────────────────────
function addSubscription(ws: WebSocket, channel: string): void {
  const subs = clientSubs.get(ws);
  if (!subs) return;

  subs.add(channel);

  if (!channelClients.has(channel)) {
    channelClients.set(channel, new Set());
  }
  channelClients.get(channel)!.add(ws);
}

function removeSubscription(ws: WebSocket, channel: string): void {
  clientSubs.get(ws)?.delete(channel);
  channelClients.get(channel)?.delete(ws);
}

function disconnectClient(ws: WebSocket): void {
  const subs = clientSubs.get(ws);
  if (subs) {
    for (const channel of subs) {
      channelClients.get(channel)?.delete(ws);
    }
  }
  clientSubs.delete(ws);
}

// ─── Broadcast ────────────────────────────────────────────────────────────────
function broadcastToChannel(channel: string, message: string): void {
  const clients = channelClients.get(channel);
  if (!clients || clients.size === 0) return;

  let sent = 0;
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
      sent++;
    }
  }

  if (sent > 0) {
    log.debug("Broadcast sent", { channel: channel.slice(0, 30), clients: sent });
  }
}

function safeSend(ws: WebSocket, payload: Record<string, unknown>): void {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

// ─── Candle history (with Redis cache) ────────────────────────────────────────
async function fetchCandleHistory(
  tokenOne: string,
  tokenTwo: string,
  interval: CandleInterval,
  limit:    number
): Promise<OHLCVCandle[]> {
  const redis    = bullmqRedis as Redis;
  const cacheKey = candleHistoryKey(tokenOne, tokenTwo, interval, limit);

  // Try Redis cache first
  try {
    const cached = await redis.get(cacheKey);
    if (cached) return JSON.parse(cached) as OHLCVCandle[];
  } catch (err: unknown) {
    log.warn("Redis cache read failed — falling back to DB", {
      error: err instanceof Error ? err.message : String(err),
    });
  }

  // Fetch from Postgres
  const rows = await prisma.candle.findMany({
    where: {
      tokenOne: tokenOne.toLowerCase(),
      tokenTwo: tokenTwo.toLowerCase(),
      interval,
    },
    orderBy: { openTime: "asc" },
    take:    limit,
    select: {
      openTime:   true,
      open:       true,
      high:       true,
      low:        true,
      close:      true,
      volume:     true,
      tradeCount: true,
    },
  });

  const candles: OHLCVCandle[] = rows.map((r:any) => ({
    time:       Math.floor(r.openTime.getTime() / 1000),
    open:       parseFloat(r.open),
    high:       parseFloat(r.high),
    low:        parseFloat(r.low),
    close:      parseFloat(r.close),
    volume:     parseFloat(r.volume),
    tradeCount: r.tradeCount,
  }));

  // Cache result
  try {
    await redis.set(cacheKey, JSON.stringify(candles), "EX", HISTORY_TTL);
  } catch (err: unknown) {
    log.warn("Redis cache write failed (non-fatal)", {
      error: err instanceof Error ? err.message : String(err),
    });
  }

  return candles;
}