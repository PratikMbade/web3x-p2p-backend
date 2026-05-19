import { Router, Request, Response } from "express";
import { prisma }       from "../lib/prisma";
import { createLogger } from "../lib/logger";
import { validatePairQuery, validateAddressParam } from "../lib/validate";

const log    = createLogger("orders-api");
const router = Router();

// ─── GET /api/orders ──────────────────────────────────────────────────────────
// List orders with optional filters.
// Query: tokenOne, tokenTwo, status, creator, orderType, limit, cursor (orderId for pagination)
router.get("/", validatePairQuery, async (req: Request, res: Response) => {
  const {
    tokenOne, tokenTwo, status, creator,
    orderType, limit: limitRaw, cursor,
  } = req.query;

  const limit = Math.min(parseInt(limitRaw as string) || 50, 200);

  const where: Record<string, unknown> = {};
  if (tokenOne)  where["tokenOne"]  = (tokenOne  as string).toLowerCase();
  if (tokenTwo)  where["tokenTwo"]  = (tokenTwo  as string).toLowerCase();
  if (status)    where["status"]    = status    as string;
  if (creator)   where["creator"]   = (creator   as string).toLowerCase();
  if (orderType) where["orderType"] = orderType as string;
  if (cursor)    where["orderId"]   = { gt: cursor as string };

  try {
    const orders = await prisma.orderCreatedEvent.findMany({
      where,
      orderBy: { blockTimestamp: "desc" },
      take:    limit,
    });
    return res.json({ orders, count: orders.length });
  } catch (err: unknown) {
    log.error("Orders fetch failed", { error: err instanceof Error ? err.message : String(err) });
    return res.status(500).json({ error: "Failed to fetch orders" });
  }
});

// ─── GET /api/orders/book ─────────────────────────────────────────────────────
// Aggregated order book depth for a pair.
// Returns bids (buy) sorted price DESC, asks (sell) sorted price ASC.
// Query: tokenOne (required), tokenTwo (required), depth (default 50, max 100)
router.get("/book", validatePairQuery, async (req: Request, res: Response) => {
  const { tokenOne, tokenTwo, depth: depthRaw } = req.query;

  if (!tokenOne || !tokenTwo) {
    return res.status(400).json({ error: "tokenOne and tokenTwo are required" });
  }

  const depth = Math.min(parseInt(depthRaw as string) || 50, 100);
  const t1    = (tokenOne as string).toLowerCase();
  const t2    = (tokenTwo as string).toLowerCase();

  try {
    // Raw SQL to SUM the wei-string amounts at each price level.
    // Casting to numeric is safe in Postgres for uint256-range integers.
    const [bids, asks] = await Promise.all([
      prisma.$queryRaw<{ pricePerToken: string; totalAmt: string; orderCount: number }[]>`
        SELECT
          "pricePerToken",
          SUM("remainingAmt"::numeric)::text AS "totalAmt",
          COUNT(*)::int                      AS "orderCount"
        FROM "OrderCreatedEvent"
        WHERE "tokenOne" = ${t1}
          AND "tokenTwo" = ${t2}
          AND "status"    = 'open'
          AND "orderType" = 'buy'
        GROUP BY "pricePerToken"
        ORDER BY "pricePerToken"::numeric DESC
        LIMIT ${depth}
      `,
      prisma.$queryRaw<{ pricePerToken: string; totalAmt: string; orderCount: number }[]>`
        SELECT
          "pricePerToken",
          SUM("remainingAmt"::numeric)::text AS "totalAmt",
          COUNT(*)::int                      AS "orderCount"
        FROM "OrderCreatedEvent"
        WHERE "tokenOne" = ${t1}
          AND "tokenTwo" = ${t2}
          AND "status"    = 'open'
          AND "orderType" = 'sell'
        GROUP BY "pricePerToken"
        ORDER BY "pricePerToken"::numeric ASC
        LIMIT ${depth}
      `,
    ]);

    // Best bid / best ask (spread)
    const bestBid  = bids[0]?.pricePerToken  ?? null;
    const bestAsk  = asks[0]?.pricePerToken  ?? null;

    return res.json({ tokenOne: t1, tokenTwo: t2, bids, asks, bestBid, bestAsk });
  } catch (err: unknown) {
    log.error("Order book fetch failed", { error: err instanceof Error ? err.message : String(err) });
    return res.status(500).json({ error: "Failed to fetch order book" });
  }
});

// ─── GET /api/orders/history ──────────────────────────────────────────────────
// Filled + cancelled orders for a pair.
// Query: tokenOne, tokenTwo, limit
router.get("/history", validatePairQuery, async (req: Request, res: Response) => {
  const { tokenOne, tokenTwo, limit: limitRaw } = req.query;
  const limit = Math.min(parseInt(limitRaw as string) || 50, 200);

  const where: Record<string, unknown> = {
    status: { in: ["filled", "cancelled"] },
  };
  if (tokenOne) where["tokenOne"] = (tokenOne as string).toLowerCase();
  if (tokenTwo) where["tokenTwo"] = (tokenTwo as string).toLowerCase();

  try {
    const orders = await prisma.orderCreatedEvent.findMany({
      where,
      orderBy: { blockTimestamp: "desc" },
      take:    limit,
    });
    return res.json({ orders, count: orders.length });
  } catch (err: unknown) {
    log.error("Order history fetch failed", { error: err instanceof Error ? err.message : String(err) });
    return res.status(500).json({ error: "Failed to fetch order history" });
  }
});

// ─── GET /api/orders/user/:walletAddress ──────────────────────────────────────
// All orders created by a wallet. Supports status and orderType filtering.
// Query: status, orderType, limit
router.get(
  "/user/:walletAddress",
  validateAddressParam("walletAddress"),
  async (req: Request, res: Response) => {
    const { walletAddress } = req.params;
    const { status, orderType, limit: limitRaw } = req.query;
    const limit = Math.min(parseInt(limitRaw as string) || 50, 200);

    const where: Record<string, unknown> = {
      creator: walletAddress.toLowerCase(),
    };
    if (status)    where["status"]    = status    as string;
    if (orderType) where["orderType"] = orderType as string;

    try {
      const [orders, summary] = await Promise.all([
        prisma.orderCreatedEvent.findMany({
          where,
          orderBy: { blockTimestamp: "desc" },
          take:    limit,
        }),
        prisma.orderCreatedEvent.groupBy({
          by:    ["status"],
          where: { creator: walletAddress.toLowerCase() },
          _count: { status: true },
        }),
      ]);

      const counts = {
        open:      summary.find((s: { status: string; }) => s.status === "open")?._count.status      ?? 0,
        filled:    summary.find((s: { status: string; }) => s.status === "filled")?._count.status    ?? 0,
        cancelled: summary.find((s: { status: string; }) => s.status === "cancelled")?._count.status ?? 0,
      };

      return res.json({ walletAddress: walletAddress.toLowerCase(), counts, orders, count: orders.length });
    } catch (err: unknown) {
      log.error("User orders fetch failed", { error: err instanceof Error ? err.message : String(err) });
      return res.status(500).json({ error: "Failed to fetch user orders" });
    }
  }
);

// ─── GET /api/orders/:orderId ─────────────────────────────────────────────────
// Single order by orderId. Optionally filter by tokenOne/tokenTwo pair.
// Also returns all fill events for this order.
router.get("/:orderId", validatePairQuery, async (req: Request, res: Response) => {
  const { orderId } = req.params;
  const { tokenOne, tokenTwo } = req.query;

  const where: Record<string, unknown> = { orderId };
  if (tokenOne) where["tokenOne"] = (tokenOne as string).toLowerCase();
  if (tokenTwo) where["tokenTwo"] = (tokenTwo as string).toLowerCase();

  try {
    const [order, fills] = await Promise.all([
      prisma.orderCreatedEvent.findFirst({ where }),
      prisma.fillEvent.findMany({
        where:   { orderId },
        orderBy: { blockTimestamp: "asc" },
      }),
    ]);

    if (!order) return res.status(404).json({ error: "Order not found" });
    return res.json({ order, fills });
  } catch (err: unknown) {
    log.error("Order fetch failed", { error: err instanceof Error ? err.message : String(err) });
    return res.status(500).json({ error: "Failed to fetch order" });
  }
});

export default router;
