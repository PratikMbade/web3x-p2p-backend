import { Router, Request, Response } from "express";
import { prisma }       from "../lib/prisma";
import { createLogger } from "../lib/logger";
import { validateAddressParam } from "../lib/validate";

const log    = createLogger("wallet-api");
const router = Router();

// ─── GET /api/wallet/:address/summary ────────────────────────────────────────
// Per-wallet summary: order counts, fill counts, and total volume as filler.
// All data comes from the DB — no chain call needed.
router.get(
  "/:address/summary",
  validateAddressParam("address"),
  async (req: Request, res: Response) => {
    const addr = req.params["address"].toLowerCase();

    try {
      const since24h = new Date(Date.now() - 24 * 60 * 60 * 1000);

      const [
        orderStatusCounts,
        orderTypeCounts,
        totalFillsAsFiller,
        fills24h,
        volumeRowsAsFiller,
        volumeRows24h,
        recentActivity,
      ] = await Promise.all([
        // How many orders by status
        prisma.orderCreatedEvent.groupBy({
          by:    ["status"],
          where: { creator: addr },
          _count: { status: true },
        }),
        // Buy vs sell breakdown
        prisma.orderCreatedEvent.groupBy({
          by:    ["orderType"],
          where: { creator: addr },
          _count: { orderType: true },
        }),
        // Times acted as filler
        prisma.recentTrade.count({ where: { fillerAddress: addr } }),
        // Fills in last 24h
        prisma.recentTrade.count({ where: { fillerAddress: addr, blockTimestamp: { gte: since24h } } }),
        // All-time value filled
        prisma.recentTrade.findMany({ where: { fillerAddress: addr }, select: { totalValue: true } }),
        // 24h value filled
        prisma.recentTrade.findMany({ where: { fillerAddress: addr, blockTimestamp: { gte: since24h } }, select: { totalValue: true } }),
        // Last 5 activities
        prisma.p2PActivity.findMany({
          where:   { walletAddress: addr },
          orderBy: { blockTimestamp: "desc" },
          take:    5,
          select:  { activityType: true, orderId: true, tokenOne: true, tokenTwo: true, txHash: true, blockTimestamp: true },
        }),
      ]);

      const sumBigInt = (rows: { totalValue: string }[]) =>
        rows.reduce((s, r) => { try { return s + BigInt(r.totalValue); } catch { return s; } }, BigInt(0)).toString();

      const orders = {
        total:     orderStatusCounts.reduce((s: any, r: { _count: { status: any; }; }) => s + r._count.status, 0),
        open:      orderStatusCounts.find((r: { status: string; }) => r.status === "open")?._count.status      ?? 0,
        filled:    orderStatusCounts.find((r: { status: string; }) => r.status === "filled")?._count.status    ?? 0,
        cancelled: orderStatusCounts.find((r: { status: string; }) => r.status === "cancelled")?._count.status ?? 0,
        buyOrders: orderTypeCounts.find((r: { orderType: string; }) => r.orderType === "buy")?._count.orderType   ?? 0,
        sellOrders: orderTypeCounts.find((r: { orderType: string; }) => r.orderType === "sell")?._count.orderType ?? 0,
      };

      return res.json({
        walletAddress: addr,
        orders,
        fills: {
          total:           totalFillsAsFiller,
          last24h:         fills24h,
          totalValueFilled: sumBigInt(volumeRowsAsFiller),
          value24h:         sumBigInt(volumeRows24h),
        },
        recentActivity,
      });
    } catch (err: unknown) {
      log.error("Wallet summary failed", { error: err instanceof Error ? err.message : String(err) });
      return res.status(500).json({ error: "Failed to fetch wallet summary" });
    }
  }
);

// ─── GET /api/wallet/:address/trades ─────────────────────────────────────────
// All trades where the wallet was filler or creator.
// Query: role (filler | creator | both — default both), limit
router.get(
  "/:address/trades",
  validateAddressParam("address"),
  async (req: Request, res: Response) => {
    const addr  = req.params["address"].toLowerCase();
    const { role, limit: limitRaw } = req.query;
    const limit = Math.min(Number.parseInt(limitRaw as string) || 50, 200);

    const orClauses: Record<string, string>[] = [];
    if (!role || role === "both" || role === "creator") orClauses.push({ creatorAddress: addr });
    if (!role || role === "both" || role === "filler")  orClauses.push({ fillerAddress:  addr });

    try {
      const trades = await prisma.recentTrade.findMany({
        where:   { OR: orClauses },
        orderBy: { blockTimestamp: "desc" },
        take:    limit,
        select: {
          id: true, txHash: true, tokenOne: true, tokenTwo: true,
          fillerAddress: true, creatorAddress: true,
          orderId: true, orderType: true, fillAmount: true,
          pricePerToken: true, totalValue: true, fullyFilled: true,
          remainingAmt: true, blockTimestamp: true,
        },
      });
      return res.json({ walletAddress: addr, trades, count: trades.length });
    } catch (err: unknown) {
      log.error("Wallet trades fetch failed", { error: err instanceof Error ? err.message : String(err) });
      return res.status(500).json({ error: "Failed to fetch wallet trades" });
    }
  }
);

export default router;
