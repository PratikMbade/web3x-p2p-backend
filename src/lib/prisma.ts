// src/lib/prisma.ts
import { PrismaClient } from "../generated/prisma";
import { config } from "../config";
import { createLogger } from "./logger";

const log = createLogger("prisma");

// ─── Singleton ────────────────────────────────────────────────────────────────
declare global {
  // eslint-disable-next-line no-var
  var __prisma: PrismaClient | undefined;
}

export const prisma: PrismaClient =
  global.__prisma ??
  new PrismaClient({
    log: config.isDev
      ? [
          { emit: "event", level: "query"  },
          { emit: "event", level: "warn"   },
          { emit: "event", level: "error"  },
        ]
      : [
          { emit: "event", level: "warn"  },
          { emit: "event", level: "error" },
        ],
  });

if (config.isDev) {
  global.__prisma = prisma;
}

// Forward Prisma events to Winston
(prisma as any).$on("warn",  (e: { message: string }) => log.warn(e.message));
(prisma as any).$on("error", (e: { message: string }) => log.error(e.message));
(prisma as any).$on("query", (e: { query: string; duration: number }) => {
  if (config.isDev) {
    log.debug("Query", { query: e.query, durationMs: e.duration });
  }
});

// ─── Connection helpers ────────────────────────────────────────────────────────
export async function connectDB(): Promise<void> {
  await prisma.$connect();
  log.info("Postgres connected");
}

export async function disconnectDB(): Promise<void> {
  await prisma.$disconnect();
  log.info("Postgres disconnected");
}