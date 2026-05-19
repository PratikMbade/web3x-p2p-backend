import { ethers } from "ethers";

const BSC_RPC = process.env.ALCHEMY_HTTP || "https://bsc-dataseed.binance.org"; 
const provider = new ethers.providers.JsonRpcProvider(BSC_RPC);

// PancakeSwap V2 Router
const ROUTER_ADDRESS = "0x10ED43C718714eb63d5aA57B78B54704E256024E";  
const ROUTER_ABI = [
  "function getAmountsOut(uint amountIn, address[] calldata path) external view returns (uint[] memory amounts)"
];

// Correct token addresses for BSC
const WBNB = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c";  // Wrapped BNB
const USDT = "0x55d398326f99059fF775485246999027B3197955";  // Tether USD (BSC-Pegged)

export async function wbnbToUsdt(amount: string) {
  try {
    const router = new ethers.Contract(ROUTER_ADDRESS, ROUTER_ABI, provider);

    // Parse amount (in wei, assuming amount = human readable string like "0.000000583294489125")
    const amountIn = ethers.utils.parseUnits(amount, 18);  

    const path = [WBNB, USDT];

    const amountsOut = await router.getAmountsOut(amountIn, path);

    // amountsOut[1] = expected USDT output (USDT has 18 decimals on BSC)
    const usdtOut = ethers.utils.formatUnits(amountsOut[1], 18);

    console.log(`${amount} WBNB ≈ ${usdtOut} USDT`);
    return usdtOut;
  } catch (error) {
    console.error("Error fetching WBNB to USDT:", error);
    throw error;
  }
}

// Alternative function with better error handling and validation
export async function wbnbToUsdtWithValidation(amount: string) {
  try {
    // Validate input
    if (!amount || isNaN(parseFloat(amount))) {
      throw new Error("Invalid amount provided");
    }

    const router = new ethers.Contract(ROUTER_ADDRESS, ROUTER_ABI, provider);
    const amountIn = ethers.utils.parseUnits(amount, 18);  

    // Check if amount is greater than 0
    if (amountIn.isZero()) {
      throw new Error("Amount must be greater than 0");
    }

    const path = [WBNB, USDT];

    console.log(`Fetching price for ${amount} WBNB...`);
    console.log(`Path: WBNB (${WBNB}) -> USDT (${USDT})`);

    const amountsOut = await router.getAmountsOut(amountIn, path);
    const usdtOut = ethers.utils.formatUnits(amountsOut[1], 18);

    console.log(`${amount} WBNB ≈ ${usdtOut} USDT`);
    return {
      success: true,
      amountIn: amount,
      amountOut: usdtOut,
      path: ['WBNB', 'USDT']
    };
  } catch (error: any) {
    console.error("Error fetching WBNB to USDT:", error.message);
    
    // Handle specific error cases
    if (error.reason === "PancakeLibrary: INSUFFICIENT_LIQUIDITY") {
      console.error("Insufficient liquidity for this trading pair or amount");
    } else if (error.code === "CALL_EXCEPTION") {
      console.error("Smart contract call failed - check token addresses and network");
    }
    
    return {
      success: false,
      error: error.message,
      amountIn: amount
    };
  }
}


export function fromWeiToWbnb(value: string | number | bigint): string {
  // Convert to string with 18 decimals
  return ethers.utils.formatUnits(value.toString(), 18);
}
