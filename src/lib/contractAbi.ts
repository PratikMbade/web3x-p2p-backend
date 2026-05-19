// src/lib/contractAbi.ts
//
// ─── Generated from your actual deployed contract ABI ────────────────────────
//
// KEY FINDINGS from your real ABI:
//
//   OrderFilled does NOT contain pricePerToken.
//   It emits: filler, tokenToBuy, tokenToExchange, orderId,
//             filledAmount, remainingAmt, fullyFilled, timestamp
//
//   This means the indexer MUST call orderDetails() via HTTP after every fill
//   to get pricePerToken. This is Pattern B — the fallback path.
//
//   The on-chain timestamp in the event is used directly as blockTimestamp,
//   which is more reliable than fetching the block separately.

export const P2P_ABI = [

  // ── Events ───────────────────────────────────────────────────────────────────

  // Emitted by fillorder()
  // NOTE: pricePerToken is NOT here — fetched via orderDetails() after fill
  // NOTE: indexed params are filler, tokenToBuy, tokenToExchange (in that order)
  "event OrderFilled(address indexed filler, address indexed tokenToBuy, address indexed tokenToExchange, uint256 orderId, uint256 filledAmount, uint256 remainingAmt, bool fullyFilled, uint256 timestamp)",

  // Emitted by createOrder()
  // Contains pricePerToken — useful to cache so we don't need orderDetails()
  "event OrderCreated(address indexed creator, address indexed tokenToBuy, address indexed tokenToExchange, uint256 orderId, uint256 orderType, uint256 amount, uint256 pricePerToken, uint256 timestamp)",

  // Emitted by cancelOrder()
  "event OrderCancelled(address indexed canceller, address indexed tokenToBuy, address indexed tokenToExchange, uint256 orderId, uint256 timestamp)",

  // ── Read functions ────────────────────────────────────────────────────────────

  // PRIMARY PRICE SOURCE — called after every OrderFilled to get pricePerToken
  // Pass: tokenToBuy as tokenOne, tokenToExchange as tokenTwo, orderId
  "function orderDetails(address tokenOne, address tokenTwo, uint256 orderId) view returns (address creator, address tokenOne, address tokenTwo, uint256 orderType, uint256 tokenAmt, uint256 pricePerToken, uint256 remainingAmt, uint256 orderTime, uint256 orderStatus)",

  // Used by health checks
  "function orderCount(address tokenOne, address tokenTwo) view returns (uint256)",

  // Used by health + order book routes
  "function getOpenOrders() view returns (tuple(address tokenOne, address tokenTwo, uint256 index)[])",
  "function getClosedOrders() view returns (tuple(address tokenOne, address tokenTwo, uint256 index)[])",
  "function getCancelledOrders() view returns (tuple(address tokenOne, address tokenTwo, uint256 index)[])",

] as const;

// ─── Exact event arg names from your ABI ─────────────────────────────────────
// Single source of truth — use these everywhere in the indexer.

export const EVENT_ARGS = {
  ORDER_FILLED: {
    filler:          "filler",          // indexed — who called fillorder()
    tokenToBuy:      "tokenToBuy",      // indexed — token the order creator wanted
    tokenToExchange: "tokenToExchange", // indexed — token the filler pays with
    orderId:         "orderId",         // uint256
    filledAmount:    "filledAmount",    // uint256 wei — volume for candle
    remainingAmt:    "remainingAmt",    // uint256 wei — remaining after this fill
    fullyFilled:     "fullyFilled",     // bool
    timestamp:       "timestamp",       // uint256 unix seconds — use as blockTimestamp
  },
  ORDER_CREATED: {
    creator:         "creator",         // indexed
    tokenToBuy:      "tokenToBuy",      // indexed
    tokenToExchange: "tokenToExchange", // indexed
    orderId:         "orderId",
    orderType:       "orderType",       // 0 = BUY, 1 = SELL
    amount:          "amount",
    pricePerToken:   "pricePerToken",   // wei — price set by creator (cache this)
    timestamp:       "timestamp",
  },
  ORDER_CANCELLED: {
    canceller:       "canceller",       // indexed
    tokenToBuy:      "tokenToBuy",      // indexed
    tokenToExchange: "tokenToExchange", // indexed
    orderId:         "orderId",
    timestamp:       "timestamp",
  },
} as const;

// ─── Event signatures (keccak256 topic0) ─────────────────────────────────────
// Must exactly match ABI param types and order.

export const EVENT_SIGNATURES = {
  ORDER_FILLED:
    "OrderFilled(address,address,address,uint256,uint256,uint256,bool,uint256)",
  ORDER_CREATED:
    "OrderCreated(address,address,address,uint256,uint256,uint256,uint256,uint256)",
  ORDER_CANCELLED:
    "OrderCancelled(address,address,address,uint256,uint256)",
} as const;

// ─── Indexer behaviour per event ─────────────────────────────────────────────
//
//  OrderFilled
//    pricePerToken NOT in event → call orderDetails(tokenToBuy, tokenToExchange, orderId)
//    use event.timestamp        → blockTimestamp (saves a getBlock() RPC call)
//    use event.filledAmount     → candle volume
//    use orderDetails.pricePerToken → candle price
//
//  OrderCreated
//    pricePerToken IS in event  → cache it in Redis so OrderFilled handler
//                                  can skip the orderDetails() call if the
//                                  order is filled in the same session
//    no candle created          → only order book update (Step 3)
//
//  OrderCancelled
//    no price, no candle        → only order book update (Step 3)