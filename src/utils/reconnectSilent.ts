// src/utils/reconnectSilent.ts
import { ethers } from 'ethers';

export type ListenerInitFn = (
  provider: ethers.providers.WebSocketProvider,
  contract: ethers.Contract
) => void;

export function reconnectSilent({
  wssUrl,
  contractAddress,
  abi,
  label,
  onReady,
  reconnectDelay = 5000,
}: {
  wssUrl: string;
  contractAddress: string;
  abi: any;
  label: string;
  onReady: ListenerInitFn;
  reconnectDelay?: number;
}) {
  let provider: ethers.providers.WebSocketProvider;
  let contract: ethers.Contract;

  const connect = () => {
    provider = new ethers.providers.WebSocketProvider(wssUrl);
    contract = new ethers.Contract(contractAddress, abi, provider);

    provider._websocket.on('open', () => {
      console.log(`✅ ${label} WebSocket OPENED`);
      onReady(provider, contract);
    });

    provider._websocket.on('close', (code: number) => {
      console.warn(
        `❌ ${label} WebSocket CLOSED (code: ${code}). Reconnecting in ${reconnectDelay}ms...`
      );
      reconnect();
    });

    provider._websocket.on('error', (err: any) => {
      console.error(`⚠️ ${label} WebSocket ERROR:`, err.message);
      reconnect();
    });
  };

  const reconnect = () => {
    try {
      provider._websocket?.terminate?.();
    } catch (err:any) {
      console.warn(`⚠️ ${label} Terminate error:`, err.message);
    }

    setTimeout(() => {
      console.log(`🔁 Reconnecting ${label} WebSocket...`);
      connect();
    }, reconnectDelay);
  };

  connect();
}
