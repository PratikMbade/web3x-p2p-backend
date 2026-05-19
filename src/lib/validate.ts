import { Request, Response, NextFunction } from "express";
import { CANDLE_INTERVALS } from "../types";

export function isValidAddress(addr: unknown): addr is string {
  return typeof addr === "string" && /^0x[a-fA-F0-9]{40}$/i.test(addr);
}

export function isValidInterval(iv: unknown): iv is string {
  return typeof iv === "string" && (CANDLE_INTERVALS as readonly string[]).includes(iv);
}

// Middleware: validates ?tokenOne and ?tokenTwo query params if present
export function validatePairQuery(req: Request, res: Response, next: NextFunction): void {
  const { tokenOne, tokenTwo } = req.query;
  if (tokenOne && !isValidAddress(tokenOne)) {
    res.status(400).json({ error: "Invalid tokenOne address" });
    return;
  }
  if (tokenTwo && !isValidAddress(tokenTwo)) {
    res.status(400).json({ error: "Invalid tokenTwo address" });
    return;
  }
  next();
}

// Middleware: validates :address or :walletAddress route param
export function validateAddressParam(paramName: string) {
  return (req: Request, res: Response, next: NextFunction): void => {
    const addr = req.params[paramName];
    if (!isValidAddress(addr)) {
      res.status(400).json({ error: `Invalid Ethereum address: ${paramName}` });
      return;
    }
    next();
  };
}
