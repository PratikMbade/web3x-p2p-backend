// src/lib/tradingHelpers.ts
// Shared helpers used by fillWorker and indexer

// ─── Calculate total value exchanged ─────────────────────────────────────────
// (fillAmount * pricePerToken) / 1e18
// Both inputs are wei strings — result is also wei string
export function calculateTotalValue(
  fillAmount:    string,
  pricePerToken: string
): string {
  try {
    const fill  = BigInt(fillAmount);
    const price = BigInt(pricePerToken);
    return ((fill * price) / BigInt("1000000000000000000")).toString();
  } catch {
    return "0";
  }
}