import async from 'async';

type Task = () => Promise<void>;

export const userQueues = new Map<
  string,
  { queue: async.AsyncQueue<Task>; hasPackageBuy: boolean }
>();

export function getUserQueue(txHash: string, user: string) {
  const key = `${txHash.toLowerCase()}_${user.toLowerCase()}`;

  if (!userQueues.has(key)) {
    // Create a serial queue with concurrency 1
    const queue = async.queue(async (task: Task, callback) => {
      try {
        await task();
        callback(); // notify done
      } catch (err) {
        callback(err as Error);
      }
    }, 1);

    userQueues.set(key, { queue, hasPackageBuy: false });

    // Auto-cleanup after 1 min
    setTimeout(() => userQueues.delete(key), 60000);
  }

  return userQueues.get(key)!;
}

export async function waitForPackageBuy(
  txHash: string,
  user: string,
  timeoutMs = 5000
): Promise<void> {
  const start = Date.now();

  return new Promise((resolve, reject) => {
    const interval = setInterval(() => {
      const lock = getUserQueue(txHash, user);
      if (lock.hasPackageBuy) {
        clearInterval(interval);
        resolve();
      } else if (Date.now() - start > timeoutMs) {
        clearInterval(interval);
        reject(new Error(`⏰ Timeout waiting for PackageBuy for ${user}`));
      }
    }, 100); // check every 100ms
  });
}

export function getAllUserQueues() {
  return userQueues;
}
