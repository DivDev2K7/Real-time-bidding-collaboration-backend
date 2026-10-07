import { createApp } from '../src/server.js';
import { createAuction } from '../src/db.js';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

export async function startApp(opts = {}) {
  const dir = mkdtempSync(path.join(tmpdir(), 'auction-'));
  const dbPath = path.join(dir, 'test.db');
  const app = createApp({ dbPath, tickMs: 50, ...opts });
  const port = await app.listen(0);
  return { app, port, dbPath };
}

export { createAuction };

/** Tiny WS test client; waitFor() searches history first, then waits. */
export function connect(port) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://127.0.0.1:${port}/ws`);
    const msgs = [], waiters = [];
    ws.onmessage = (e) => {
      const m = JSON.parse(e.data);
      msgs.push(m);
      for (const w of [...waiters]) if (w.pred(m)) { waiters.splice(waiters.indexOf(w), 1); clearTimeout(w.t); w.resolve(m); }
    };
    ws.onerror = reject;
    ws.onopen = () => resolve({
      ws, msgs,
      send: (o) => ws.send(JSON.stringify(o)),
      close: () => new Promise((r) => { ws.onclose = r; ws.close(); }),
      waitFor: (pred, ms = 5000) => {
        const hit = msgs.find(pred);
        if (hit) return Promise.resolve(hit);
        return new Promise((res, rej) => {
          const w = { pred, resolve: res, t: setTimeout(() => rej(new Error('timeout waiting for message')), ms) };
          waiters.push(w);
        });
      },
    });
  });
}
export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
