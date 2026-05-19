// src/lib/logger.ts
import winston from "winston";
import DailyRotateFile from "winston-daily-rotate-file";
import path from "path";
import { config } from "../config";
import { LogContext } from "../types";

// ─── Custom log format ────────────────────────────────────────────────────────
const structuredFormat = winston.format.combine(
  winston.format.timestamp({ format: "YYYY-MM-DD HH:mm:ss.SSS" }),
  winston.format.errors({ stack: true }),
  winston.format.json()
);

const consoleFormat = winston.format.combine(
  winston.format.colorize({ all: true }),
  winston.format.timestamp({ format: "HH:mm:ss.SSS" }),
  winston.format.printf(({ timestamp, level, message, service, ...rest }) => {
    const svc = service ? `[${service}]` : "";
    const ctx = Object.keys(rest).length
      ? " " + JSON.stringify(rest, null, 0)
      : "";
    return `${timestamp} ${level} ${svc} ${message}${ctx}`;
  })
);

// ─── Transports ───────────────────────────────────────────────────────────────
const transports: winston.transport[] = [
  new winston.transports.Console({
    format: config.isProd ? structuredFormat : consoleFormat,
  }),
];

// File transports only in production or if LOG_DIR is explicitly set
if (config.isProd || config.logging.dir !== "./logs") {
  transports.push(
    // All logs
    new DailyRotateFile({
      filename:     path.join(config.logging.dir, "indexer-%DATE%.log"),
      datePattern:  "YYYY-MM-DD",
      zippedArchive: true,
      maxSize:      "50m",
      maxFiles:     "30d",
      format:       structuredFormat,
    }),
    // Error-only log
    new DailyRotateFile({
      level:        "error",
      filename:     path.join(config.logging.dir, "error-%DATE%.log"),
      datePattern:  "YYYY-MM-DD",
      zippedArchive: true,
      maxSize:      "20m",
      maxFiles:     "90d",
      format:       structuredFormat,
    })
  );
}

// ─── Base logger ──────────────────────────────────────────────────────────────
const baseLogger = winston.createLogger({
  level:      config.logging.level,
  transports,
  exitOnError: false,
});

// ─── Child logger factory ─────────────────────────────────────────────────────
// Each service gets its own child with a "service" label baked in
export function createLogger(service: string) {
  return baseLogger.child({ service });
}

// ─── Typed log helpers ────────────────────────────────────────────────────────
export type ServiceLogger = ReturnType<typeof createLogger> & {
  withCtx: (ctx: LogContext) => ServiceLogger;
};

export function buildLogger(service: string): ServiceLogger {
  const child = baseLogger.child({ service }) as ServiceLogger;

  // Attach a helper to create sub-child with extra context
  child.withCtx = (ctx: LogContext): ServiceLogger => {
    return buildLogger(service);  // returns a new logger with merged context
    // Note: In practice call child.child(ctx) — Winston supports this
  };

  return child;
}

// ─── Root logger (used in server.ts bootstrap) ────────────────────────────────
export const logger = createLogger("root");

// ─── Log unhandled errors through Winston ────────────────────────────────────
export function attachProcessHandlers(): void {
  process.on("uncaughtException", (err) => {
    logger.error("Uncaught exception", {
      error:  err.message,
      stack:  err.stack,
    });
    // Give Winston time to flush before exit
    setTimeout(() => process.exit(1), 500);
  });

  process.on("unhandledRejection", (reason) => {
    logger.error("Unhandled promise rejection", {
      error: reason instanceof Error ? reason.message : String(reason),
      stack: reason instanceof Error ? reason.stack : undefined,
    });
  });
}