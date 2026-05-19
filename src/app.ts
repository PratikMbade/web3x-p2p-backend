import express, { Request, Response, NextFunction } from "express";
import helmet     from "helmet";
import cors       from "cors";
import rateLimit  from "express-rate-limit";
import { config }       from "./config";
import { createLogger } from "./lib/logger";
import healthRouter from "./routes/health";
import dlqRouter    from "./routes/dlq";
import ordersRouter from "./routes/orders";
import tradesRouter from "./routes/trades";
import statsRouter  from "./routes/stats";
import walletRouter from "./routes/wallet";

const log = createLogger("app");
const app = express();

// ─── Security middleware ──────────────────────────────────────────────────────
app.use(helmet());
const allowedOrigins = new Set([
  "https://p2p.web3x.space",
  ...(process.env["FRONTEND_URL"] ? [process.env["FRONTEND_URL"]] : []),
]);

app.use(cors({
  origin: config.isProd
    ? (origin, cb) => {
        if (!origin || allowedOrigins.has(origin)) cb(null, true);
        else cb(new Error("Not allowed by CORS"));
      }
    : "*",
  methods: ["GET", "POST", "DELETE"],
}));
app.use(express.json({ limit: "1mb" }));

// ─── Rate limiting ────────────────────────────────────────────────────────────
// General API limit: 120 req/min per IP
const apiLimiter = rateLimit({
  windowMs:        60 * 1000,
  max:             120,
  standardHeaders: true,
  legacyHeaders:   false,
  message:         { error: "Too many requests — please slow down." },
});

// Tighter limit for heavy aggregation endpoints
const heavyLimiter = rateLimit({
  windowMs:        60 * 1000,
  max:             30,
  standardHeaders: true,
  legacyHeaders:   false,
  message:         { error: "Too many requests — please slow down." },
});

app.use("/api", apiLimiter);
app.use("/api/orders/book",  heavyLimiter);
app.use("/api/stats",        heavyLimiter);
app.use("/api/wallet",       heavyLimiter);

// ─── Request logging ──────────────────────────────────────────────────────────
app.use((req: Request, _res: Response, next: NextFunction) => {
  log.debug("Incoming request", { method: req.method, path: req.path, ip: req.ip });
  next();
});

// ─── Routes ───────────────────────────────────────────────────────────────────
app.use("/health",       healthRouter);
app.use("/api/dlq",      dlqRouter);
app.use("/api/orders",   ordersRouter);
app.use("/api/trades",   tradesRouter);
app.use("/api/stats",    statsRouter);
app.use("/api/wallet",   walletRouter);

// ─── 404 handler ─────────────────────────────────────────────────────────────
app.use((_req: Request, res: Response) => {
  res.status(404).json({ error: "Not found" });
});

// ─── Global error handler ─────────────────────────────────────────────────────
app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
  log.error("Unhandled request error", { error: err.message, stack: err.stack });
  res.status(500).json({ error: "Internal server error" });
});

export default app;
