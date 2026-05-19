import { Router, Request, Response } from "express";
import { Redis }        from "ioredis";
import { prisma }       from "../lib/prisma";
import { bullmqRedis }  from "../lib/redis";
import { createLogger } from "../lib/logger";
import { validatePairQuery, validateAddressParam, isValidInterval } from "../lib/validate";
import { CandleInterval, OHLCVCandle } from "../types";

const log    = createLogger("trades-api");
const router = Router();

const CANDLE_CACHE_TTL: Record<string, number> = {
  "1m":  10,
  "5m":  20,
  "15m": 30,
  "1h":  60,
  "4h":  120,
  "1d":  300,
};

// ─── GET /api/trades/candles ──────────────────────────────────────────────────
// OHLCV candle data for lightweight-charts or any charting library.
// Query: tokenOne (req), tokenTwo (req), interval (req), from, to, limit
router.get("/candles", validatePairQuery, async (req: Request, res: Response) => {
  const { tokenOne, tokenTwo, interval, from, to, limit: limitRaw } = req.query;

  if (!tokenOne || !tokenTwo) {
    return res.status(400).json({ error: "tokenOne and tokenTwo are required" });
  }
  if (!isValidInterval(interval)) {
    return res.status(400).json({
      error: "interval is required and must be one of: 1m, 5m, 15m, 1h, 4h, 1d",
    });
  }

  const limit  = Math.min(Number.parseInt(limitRaw as string) || 500, 1000);
  const t1     = (tokenOne as string).toLowerCase();
  const t2     = (tokenTwo as string).toLowerCase();
  const iv     = interval as CandleInterval;

  // Build date filters
  const timeWhere: { gte?: Date; lte?: Date } = {};
  if (from) {
    const fromTs = Number(from);
    timeWhere.gte = isNaN(fromTs) ? new Date(from as string) : new Date(fromTs * 1000);
  }
  if (to) {
    const toTs = Number(to);
    timeWhere.lte = isNaN(toTs) ? new Date(to as string) : new Date(toTs * 1000);
  }

  // Cache key includes the full query so different ranges don't collide
  const cacheKey = `candle_http:${t1}:${t2}:${iv}:${from ?? ""}:${to ?? ""}:${limit}`;
  const ttl      = CANDLE_CACHE_TTL[iv] ?? 30;

  try {
    // Try Redis cache first
    const redis = bullmqRedis as Redis;
    try {
      const cached = await redis.get(cacheKey);
      if (cached) {
        return res.json(JSON.parse(cached));
      }
    } catch {
      // Cache miss is non-fatal — continue to DB
    }

    const rows = await prisma.candle.findMany({
      where: {
        tokenOne: t1,
        tokenTwo: t2,
        interval: iv,
        ...(Object.keys(timeWhere).length > 0 ? { openTime: timeWhere } : {}),
      },
      orderBy: { openTime: "asc" },
      take:    limit,
      select: {
        openTime:   true,
        closeTime:  true,
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

    const payload = { tokenOne: t1, tokenTwo: t2, interval: iv, count: candles.length, candles };

    try {
      await redis.set(cacheKey, JSON.stringify(payload), "EX", ttl);
    } catch {
      // Cache write failure is non-fatal
    }

    return res.json(payload);
  } catch (err: unknown) {
    log.error("Candle fetch failed", { error: err instanceof Error ? err.message : String(err) });
    return res.status(500).json({ error: "Failed to fetch candles" });
  }
});

// ─── GET /api/trades/recent ───────────────────────────────────────────────────
// Recent trades feed for a pair (trading chart sidebar).
// Query: tokenOne, tokenTwo, limit
router.get("/recent", validatePairQuery, async (req: Request, res: Response) => {
  const { tokenOne, tokenTwo, limit: lim } = req.query;
  const limit = Math.min(Number.parseInt(lim as string) || 50, 200);

  const where: Record<string, unknown> = {};
  if (tokenOne) where["tokenOne"] = (tokenOne as string).toLowerCase();
  if (tokenTwo) where["tokenTwo"] = (tokenTwo as string).toLowerCase();

  try {
    const trades = await prisma.recentTrade.findMany({
      where,
      orderBy: { blockTimestamp: "desc" },
      take:    limit,
      select: {
        id:             true,
        txHash:         true,
        tokenOne:       true,
        tokenTwo:       true,
        fillerAddress:  true,
        creatorAddress: true,
        orderId:        true,
        orderType:      true,
        fillAmount:     true,
        pricePerToken:  true,
        totalValue:     true,
        fullyFilled:    true,
        remainingAmt:   true,
        blockTimestamp: true,
      },
    });
    return res.json({ trades, count: trades.length });
  } catch (err: unknown) {
    log.error("Recent trades fetch failed", { error: err instanceof Error ? err.message : String(err) });
    return res.status(500).json({ error: "Failed to fetch recent trades" });
  }
});

// ─── GET /api/trades/closed-orders ───────────────────────────────────────────
// Closed orders (filled + cancelled) for a pair or user.
// Query: tokenOne, tokenTwo, creatorAddress, closedStatus, limit
router.get("/closed-orders", validatePairQuery, async (req: Request, res: Response) => {
  const { tokenOne, tokenTwo, creatorAddress, closedStatus, limit: lim } = req.query;
  const limit = Math.min(Number.parseInt(lim as string) || 50, 200);

  const where: Record<string, unknown> = {};
  if (tokenOne)       where["tokenOne"]       = (tokenOne as string).toLowerCase();
  if (tokenTwo)       where["tokenTwo"]       = (tokenTwo as string).toLowerCase();
  if (creatorAddress) where["creatorAddress"] = (creatorAddress as string).toLowerCase();
  if (closedStatus)   where["closedStatus"]   = closedStatus as string;

  try {
    const orders = await prisma.closedOrder.findMany({
      where,
      orderBy: { closedAt: "desc" },
      take:    limit,
    });
    return res.json({ orders, count: orders.length });
  } catch (err: unknown) {
    log.error("Closed orders fetch failed", { error: err instanceof Error ? err.message : String(err) });
    return res.status(500).json({ error: "Failed to fetch closed orders" });
  }
});

// ─── GET /api/trades/activity ─────────────────────────────────────────────────
// P2P activity for a user (all create/fill/cancel actions).
// Query: walletAddress OR userId, activityType, limit
router.get("/activity", async (req: Request, res: Response) => {
  const { walletAddress, activityType, limit: lim } = req.query;
  const limit = Math.min(Number.parseInt(lim as string) || 50, 500);

  if (!walletAddress) {
    return res.status(400).json({ error: "walletAddress is required" });
  }

  const where: Record<string, unknown> = {};
  if (walletAddress) where["walletAddress"] = (walletAddress as string).toLowerCase();
  if (activityType)  where["activityType"]  = activityType as string;

  try {
    const activities = await prisma.p2PActivity.findMany({
      where,
      orderBy: { blockTimestamp: "desc" },
      take:    limit,
    });
    return res.json({ activities, count: activities.length });
  } catch (err: unknown) {
    log.error("Activity fetch failed", { error: err instanceof Error ? err.message : String(err) });
    return res.status(500).json({ error: "Failed to fetch activity" });
  }
});

// ─── GET /api/trades/report/:walletAddress ────────────────────────────────────
// Summary report: counts by activity type + total volume for a wallet.
router.get(
  "/report/:walletAddress",
  validateAddressParam("walletAddress"),
  async (req: Request, res: Response) => {
    const { walletAddress } = req.params;
    const addr = walletAddress.toLowerCase();

    try {
      const [activityCounts, totalTrades, volumeRows] = await Promise.all([
        prisma.p2PActivity.groupBy({
          by:    ["activityType"],
          where: { walletAddress: addr },
          _count: { activityType: true },
        }),
        prisma.recentTrade.count({
          where: { fillerAddress: addr },
        }),
        prisma.recentTrade.findMany({
          where:  { fillerAddress: addr },
          select: { totalValue: true },
        }),
      ]);

      const totalValueFilled = volumeRows
        .reduce((sum:any, t:any) => {
          try { return sum + BigInt(t.totalValue); } catch { return sum; }
        }, BigInt(0))
        .toString();

      return res.json({
        walletAddress:       addr,
        ordersCreated:       activityCounts.find((a:any) => a.activityType === "create_order")?._count.activityType  ?? 0,
        ordersFilled:        activityCounts.find((a:any) => a.activityType === "fill_order")?._count.activityType    ?? 0,
        ordersCancelled:     activityCounts.find((a:any) => a.activityType === "cancel_order")?._count.activityType  ?? 0,
        totalTradesAsFiller: totalTrades,
        totalValueFilled,
      });
    } catch (err: unknown) {
      log.error("Report fetch failed", { error: err instanceof Error ? err.message : String(err) });
      return res.status(500).json({ error: "Failed to fetch report" });
    }
  }
);

export default router;
