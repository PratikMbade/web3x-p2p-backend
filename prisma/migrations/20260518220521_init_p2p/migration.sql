-- CreateTable
CREATE TABLE "FillEvent" (
    "id" TEXT NOT NULL,
    "txHash" TEXT NOT NULL,
    "blockNumber" INTEGER NOT NULL,
    "logIndex" INTEGER NOT NULL,
    "blockTimestamp" TIMESTAMP(3) NOT NULL,
    "tokenOne" TEXT NOT NULL,
    "tokenTwo" TEXT NOT NULL,
    "orderId" TEXT NOT NULL,
    "filler" TEXT NOT NULL,
    "fillAmount" TEXT NOT NULL,
    "pricePerToken" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "FillEvent_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "OrderCreatedEvent" (
    "id" TEXT NOT NULL,
    "txHash" TEXT NOT NULL,
    "blockNumber" INTEGER NOT NULL,
    "logIndex" INTEGER NOT NULL,
    "blockTimestamp" TIMESTAMP(3) NOT NULL,
    "creator" TEXT NOT NULL,
    "tokenOne" TEXT NOT NULL,
    "tokenTwo" TEXT NOT NULL,
    "orderId" TEXT NOT NULL,
    "orderType" TEXT NOT NULL,
    "amount" TEXT NOT NULL,
    "pricePerToken" TEXT NOT NULL,
    "remainingAmt" TEXT NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'open',
    "filledAt" TIMESTAMP(3),
    "cancelledAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "OrderCreatedEvent_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "OrderCancelledEvent" (
    "id" TEXT NOT NULL,
    "txHash" TEXT NOT NULL,
    "blockNumber" INTEGER NOT NULL,
    "logIndex" INTEGER NOT NULL,
    "blockTimestamp" TIMESTAMP(3) NOT NULL,
    "canceller" TEXT NOT NULL,
    "tokenOne" TEXT NOT NULL,
    "tokenTwo" TEXT NOT NULL,
    "orderId" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "OrderCancelledEvent_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Candle" (
    "id" TEXT NOT NULL,
    "tokenOne" TEXT NOT NULL,
    "tokenTwo" TEXT NOT NULL,
    "interval" TEXT NOT NULL,
    "openTime" TIMESTAMP(3) NOT NULL,
    "closeTime" TIMESTAMP(3) NOT NULL,
    "open" TEXT NOT NULL,
    "high" TEXT NOT NULL,
    "low" TEXT NOT NULL,
    "close" TEXT NOT NULL,
    "volume" TEXT NOT NULL,
    "tradeCount" INTEGER NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Candle_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "IndexerState" (
    "id" TEXT NOT NULL DEFAULT 'singleton',
    "lastSafeBlock" INTEGER NOT NULL DEFAULT 0,
    "lastUpdatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "IndexerState_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "P2PActivity" (
    "id" TEXT NOT NULL,
    "walletAddress" TEXT NOT NULL,
    "activityType" TEXT NOT NULL,
    "orderId" TEXT NOT NULL,
    "tokenOne" TEXT NOT NULL,
    "tokenTwo" TEXT NOT NULL,
    "amount" TEXT NOT NULL,
    "pricePerToken" TEXT NOT NULL,
    "totalCost" TEXT,
    "orderType" TEXT,
    "txHash" TEXT NOT NULL,
    "blockNumber" INTEGER NOT NULL,
    "blockTimestamp" TIMESTAMP(3) NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "P2PActivity_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "RecentTrade" (
    "id" TEXT NOT NULL,
    "tokenOne" TEXT NOT NULL,
    "tokenTwo" TEXT NOT NULL,
    "fillerAddress" TEXT NOT NULL,
    "creatorAddress" TEXT NOT NULL,
    "orderId" TEXT NOT NULL,
    "orderType" TEXT NOT NULL,
    "fillAmount" TEXT NOT NULL,
    "pricePerToken" TEXT NOT NULL,
    "totalValue" TEXT NOT NULL,
    "fullyFilled" BOOLEAN NOT NULL DEFAULT false,
    "remainingAmt" TEXT NOT NULL,
    "txHash" TEXT NOT NULL,
    "blockNumber" INTEGER NOT NULL,
    "blockTimestamp" TIMESTAMP(3) NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "RecentTrade_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ClosedOrder" (
    "id" TEXT NOT NULL,
    "orderId" TEXT NOT NULL,
    "tokenOne" TEXT NOT NULL,
    "tokenTwo" TEXT NOT NULL,
    "creatorAddress" TEXT NOT NULL,
    "orderType" TEXT NOT NULL,
    "amount" TEXT NOT NULL,
    "pricePerToken" TEXT NOT NULL,
    "closedStatus" TEXT NOT NULL,
    "closedAt" TIMESTAMP(3) NOT NULL,
    "closedTxHash" TEXT NOT NULL,
    "filledByAddress" TEXT,
    "totalFilled" TEXT NOT NULL DEFAULT '0',
    "createdTxHash" TEXT NOT NULL,
    "orderCreatedAt" TIMESTAMP(3) NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "ClosedOrder_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "FillEvent_txHash_key" ON "FillEvent"("txHash");

-- CreateIndex
CREATE INDEX "FillEvent_tokenOne_tokenTwo_idx" ON "FillEvent"("tokenOne", "tokenTwo");

-- CreateIndex
CREATE INDEX "FillEvent_blockNumber_idx" ON "FillEvent"("blockNumber");

-- CreateIndex
CREATE INDEX "FillEvent_blockTimestamp_idx" ON "FillEvent"("blockTimestamp");

-- CreateIndex
CREATE INDEX "FillEvent_orderId_idx" ON "FillEvent"("orderId");

-- CreateIndex
CREATE INDEX "FillEvent_tokenOne_tokenTwo_blockTimestamp_idx" ON "FillEvent"("tokenOne", "tokenTwo", "blockTimestamp");

-- CreateIndex
CREATE UNIQUE INDEX "OrderCreatedEvent_txHash_key" ON "OrderCreatedEvent"("txHash");

-- CreateIndex
CREATE INDEX "OrderCreatedEvent_tokenOne_tokenTwo_idx" ON "OrderCreatedEvent"("tokenOne", "tokenTwo");

-- CreateIndex
CREATE INDEX "OrderCreatedEvent_tokenOne_tokenTwo_status_idx" ON "OrderCreatedEvent"("tokenOne", "tokenTwo", "status");

-- CreateIndex
CREATE INDEX "OrderCreatedEvent_creator_idx" ON "OrderCreatedEvent"("creator");

-- CreateIndex
CREATE INDEX "OrderCreatedEvent_orderId_idx" ON "OrderCreatedEvent"("orderId");

-- CreateIndex
CREATE INDEX "OrderCreatedEvent_blockTimestamp_idx" ON "OrderCreatedEvent"("blockTimestamp");

-- CreateIndex
CREATE INDEX "OrderCreatedEvent_status_idx" ON "OrderCreatedEvent"("status");

-- CreateIndex
CREATE UNIQUE INDEX "OrderCancelledEvent_txHash_key" ON "OrderCancelledEvent"("txHash");

-- CreateIndex
CREATE INDEX "OrderCancelledEvent_tokenOne_tokenTwo_idx" ON "OrderCancelledEvent"("tokenOne", "tokenTwo");

-- CreateIndex
CREATE INDEX "OrderCancelledEvent_canceller_idx" ON "OrderCancelledEvent"("canceller");

-- CreateIndex
CREATE INDEX "OrderCancelledEvent_orderId_idx" ON "OrderCancelledEvent"("orderId");

-- CreateIndex
CREATE INDEX "OrderCancelledEvent_blockTimestamp_idx" ON "OrderCancelledEvent"("blockTimestamp");

-- CreateIndex
CREATE INDEX "Candle_tokenOne_tokenTwo_interval_openTime_idx" ON "Candle"("tokenOne", "tokenTwo", "interval", "openTime");

-- CreateIndex
CREATE INDEX "Candle_tokenOne_tokenTwo_interval_idx" ON "Candle"("tokenOne", "tokenTwo", "interval");

-- CreateIndex
CREATE INDEX "Candle_openTime_idx" ON "Candle"("openTime");

-- CreateIndex
CREATE UNIQUE INDEX "Candle_tokenOne_tokenTwo_interval_openTime_key" ON "Candle"("tokenOne", "tokenTwo", "interval", "openTime");

-- CreateIndex
CREATE UNIQUE INDEX "P2PActivity_txHash_key" ON "P2PActivity"("txHash");

-- CreateIndex
CREATE INDEX "P2PActivity_walletAddress_idx" ON "P2PActivity"("walletAddress");

-- CreateIndex
CREATE INDEX "P2PActivity_walletAddress_activityType_idx" ON "P2PActivity"("walletAddress", "activityType");

-- CreateIndex
CREATE INDEX "P2PActivity_tokenOne_tokenTwo_idx" ON "P2PActivity"("tokenOne", "tokenTwo");

-- CreateIndex
CREATE INDEX "P2PActivity_tokenOne_tokenTwo_activityType_idx" ON "P2PActivity"("tokenOne", "tokenTwo", "activityType");

-- CreateIndex
CREATE INDEX "P2PActivity_blockTimestamp_idx" ON "P2PActivity"("blockTimestamp");

-- CreateIndex
CREATE INDEX "P2PActivity_orderId_idx" ON "P2PActivity"("orderId");

-- CreateIndex
CREATE UNIQUE INDEX "RecentTrade_txHash_key" ON "RecentTrade"("txHash");

-- CreateIndex
CREATE INDEX "RecentTrade_tokenOne_tokenTwo_idx" ON "RecentTrade"("tokenOne", "tokenTwo");

-- CreateIndex
CREATE INDEX "RecentTrade_tokenOne_tokenTwo_blockTimestamp_idx" ON "RecentTrade"("tokenOne", "tokenTwo", "blockTimestamp");

-- CreateIndex
CREATE INDEX "RecentTrade_fillerAddress_idx" ON "RecentTrade"("fillerAddress");

-- CreateIndex
CREATE INDEX "RecentTrade_creatorAddress_idx" ON "RecentTrade"("creatorAddress");

-- CreateIndex
CREATE INDEX "RecentTrade_orderId_idx" ON "RecentTrade"("orderId");

-- CreateIndex
CREATE INDEX "RecentTrade_blockTimestamp_idx" ON "RecentTrade"("blockTimestamp");

-- CreateIndex
CREATE UNIQUE INDEX "ClosedOrder_closedTxHash_key" ON "ClosedOrder"("closedTxHash");

-- CreateIndex
CREATE INDEX "ClosedOrder_tokenOne_tokenTwo_idx" ON "ClosedOrder"("tokenOne", "tokenTwo");

-- CreateIndex
CREATE INDEX "ClosedOrder_tokenOne_tokenTwo_closedStatus_idx" ON "ClosedOrder"("tokenOne", "tokenTwo", "closedStatus");

-- CreateIndex
CREATE INDEX "ClosedOrder_creatorAddress_idx" ON "ClosedOrder"("creatorAddress");

-- CreateIndex
CREATE INDEX "ClosedOrder_closedAt_idx" ON "ClosedOrder"("closedAt");

-- CreateIndex
CREATE INDEX "ClosedOrder_closedStatus_idx" ON "ClosedOrder"("closedStatus");

-- CreateIndex
CREATE INDEX "ClosedOrder_orderId_idx" ON "ClosedOrder"("orderId");
