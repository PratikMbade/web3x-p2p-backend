import { Router, Request, Response } from "express";
import { Redis }        from "ioredis";
import { prisma }       from "../lib/prisma";
import { bullmqRedis }  from "../lib/redis";
import { createLogger } from "../lib/logger";

const log    = createLogger("stats-api");
const router = Router();

const STATS_CACHE_KEY = "stats:global";
const STATS_CACHE_TTL = 30; // seconds
const PAIRS_CACHE_KEY = "stats:pairs";
const PAIRS_CACHE_TTL = 60;

// ─── GET /api/stats ───────────────────────────────────────────────────────────
// Global platform statistics. Cached 30s.
// Returns order counts, trade counts, and 24h rolling volume.
router.get("/", async (_req: Request, res: Response) => {
  const redis = bullmqRedis as Redis;

  try {
    const cached = await redis.get(STATS_CACHE_KEY);
    if (cached) return res.json(JSON.parse(cached));
  } catch { /* cache miss — fall through */ }

  try {
    const since24h = new Date(Date.now() - 24 * 60 * 60 * 1000);

    const [
      totalOrders,
      openOrders,
      filledOrders,
      cancelledOrders,
      totalTrades,
      trades24h,
      volume24hRows,
      activePairsRaw,
    ] = await Promise.all([
      prisma.orderCreatedEvent.count(),
      prisma.orderCreatedEvent.count({ where: { status: "open" } }),
      prisma.orderCreatedEvent.count({ where: { status: "filled" } }),
      prisma.orderCreatedEvent.count({ where: { status: "cancelled" } }),
      prisma.recentTrade.count(),
      prisma.recentTrade.count({ where: { blockTimestamp: { gte: since24h } } }),
      prisma.recentTrade.findMany({
        where:  { blockTimestamp: { gte: since24h } },
        select: { totalValue: true },
      }),
      prisma.orderCreatedEvent.groupBy({
        by:    ["tokenOne", "tokenTwo"],
        where: { status: "open" },
        _count: { id: true },
      }),
    ]);

    const volume24h = volume24hRows
      .reduce((sum:any, t:any) => {
        try { return sum + BigInt(t.totalValue); } catch { return sum; }
      }, BigInt(0))
      .toString();

    const payload = {
      orders:      { total: totalOrders, open: openOrders, filled: filledOrders, cancelled: cancelledOrders },
      trades:      { total: totalTrades, last24h: trades24h },
      volume:      { last24h: volume24h },
      activePairs: activePairsRaw.length,
      generatedAt: new Date().toISOString(),
    };

    try { await redis.set(STATS_CACHE_KEY, JSON.stringify(payload), "EX", STATS_CACHE_TTL); } catch { /* non-fatal */ }

    return res.json(payload);
  } catch (err: unknown) {
    log.error("Stats fetch failed", { error: err instanceof Error ? err.message : String(err) });
    return res.status(500).json({ error: "Failed to fetch stats" });
  }
});

// ─── GET /api/stats/pairs ─────────────────────────────────────────────────────
// All active trading pairs with open order counts and 24h trade activity.
// Cached 60s.
router.get("/pairs", async (_req: Request, res: Response) => {
  const redis = bullmqRedis as Redis;

  try {
    const cached = await redis.get(PAIRS_CACHE_KEY);
    if (cached) return res.json(JSON.parse(cached));
  } catch { /* cache miss — fall through */ }

  try {
    const since24h = new Date(Date.now() - 24 * 60 * 60 * 1000);

    const [openPairs, recentTrades] = await Promise.all([
      prisma.orderCreatedEvent.groupBy({
        by:    ["tokenOne", "tokenTwo"],
        where: { status: "open" },
        _count: { id: true },
      }),
      prisma.recentTrade.groupBy({
        by:    ["tokenOne", "tokenTwo"],
        where: { blockTimestamp: { gte: since24h } },
        _count: { id: true },
      }),
    ]);

    // Merge both sets into one unified pair list
    const pairMap = new Map<string, { tokenOne: string; tokenTwo: string; openOrders: number; trades24h: number }>();

    for (const p of openPairs) {
      const key = `${p.tokenOne}:${p.tokenTwo}`;
      pairMap.set(key, { tokenOne: p.tokenOne, tokenTwo: p.tokenTwo, openOrders: p._count.id, trades24h: 0 });
    }

    for (const r of recentTrades) {
      const key = `${r.tokenOne}:${r.tokenTwo}`;
      const existing = pairMap.get(key);
      if (existing) {
        existing.trades24h = r._count.id;
      } else {
        pairMap.set(key, { tokenOne: r.tokenOne, tokenTwo: r.tokenTwo, openOrders: 0, trades24h: r._count.id });
      }
    }

    const pairs   = Array.from(pairMap.values());
    const payload = { pairs, count: pairs.length, generatedAt: new Date().toISOString() };

    try { await redis.set(PAIRS_CACHE_KEY, JSON.stringify(payload), "EX", PAIRS_CACHE_TTL); } catch { /* non-fatal */ }

    return res.json(payload);
  } catch (err: unknown) {
    log.error("Pairs fetch failed", { error: err instanceof Error ? err.message : String(err) });
    return res.status(500).json({ error: "Failed to fetch pairs" });
  }
});

export default router;
