import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { upgrade } from './ws.js';
import { openDb, createAuction, listAuctions, getState, placeBid, closeExpired } from './db.js';

const PUBLIC = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'public');
const MAX_BACKLOG = 1 << 20; // drop clients that can't keep up (1 MiB unsent)

export function createApp({ dbPath = './auction.db', tickMs = 250, heartbeatMs = 15000 } = {}) {
  const db = openDb(dbPath);
  const subs = new Map();     // auctionId -> Set<Conn>
  const conns = new Set();

  // ---------- broadcast ----------
  // Called synchronously right after a COMMIT. Because Node runs this in the same tick,
  // events for one auction go out in exactly commit order (version N before N+1).
  function broadcast(auctionId, payload) {
    const set = subs.get(auctionId);
    if (!set) return;
    const msg = JSON.stringify(payload);
    for (const c of set) {
      if (c.bufferedAmount > MAX_BACKLOG) { c.terminate(); continue; }
      c.send(msg);
    }
  }

  // ---------- websocket protocol ----------
  function onConnection(conn) {
    conn.subs = new Set();
    conns.add(conn);
    conn.on('close', () => {                      // disconnect: only in-memory routing is dropped;
      for (const id of conn.subs) subs.get(id)?.delete(conn); // all auction state lives in SQLite
      conns.delete(conn);
    });
    conn.on('message', (raw) => {
      let m;
      try { m = JSON.parse(raw); } catch { return err(conn, 'BAD_JSON'); }
      if (!m || typeof m !== 'object') return err(conn, 'BAD_MESSAGE');
      try {
        if (m.type === 'subscribe') return subscribe(conn, m);
        if (m.type === 'unsubscribe') { subs.get(m.auctionId)?.delete(conn); conn.subs.delete(m.auctionId); return; }
        if (m.type === 'bid') return bid(conn, m);
        err(conn, 'UNKNOWN_TYPE');
      } catch (e) {
        console.error(e);
        err(conn, 'INTERNAL', m.msgId);
      }
    });
  }

  const err = (conn, reason, msgId) => conn.send(JSON.stringify({ type: 'error', reason, msgId }));

  function subscribe(conn, { auctionId }) {
    const state = getState(db, String(auctionId));
    if (!state) return err(conn, 'NOT_FOUND');
    // Read + register happen in one synchronous tick, so no commit can slip between the
    // snapshot and the first live event: the client never misses an update.
    if (!subs.has(auctionId)) subs.set(auctionId, new Set());
    subs.get(auctionId).add(conn);
    conn.subs.add(auctionId);
    conn.send(JSON.stringify({ type: 'snapshot', ...state }));
  }

  function bid(conn, m) {
    const { auctionId, bidder, amount, msgId } = m;
    if (typeof auctionId !== 'string' || typeof msgId !== 'string' || msgId.length > 64 ||
        typeof bidder !== 'string' || !bidder.trim() || bidder.length > 40 ||
        !Number.isSafeInteger(amount) || amount <= 0) {
      return err(conn, 'BAD_BID', msgId);
    }
    const r = placeBid(db, { auctionId, bidder: bidder.trim(), amount, msgId });
    conn.send(JSON.stringify({
      type: 'ack', msgId, ok: r.ok, reason: r.reason, duplicate: r.duplicate,
      minAcceptable: r.minAcceptable, bid: r.bid, auction: r.auction,
    }));
    if (r.changed) {
      broadcast(auctionId, { type: 'update', event: r.bid ? 'bid' : 'closed', auction: r.auction, bid: r.bid });
    }
  }

  // ---------- timers ----------
  const ticker = setInterval(() => {
    for (const a of closeExpired(db)) broadcast(a.id, { type: 'update', event: 'closed', auction: a });
  }, tickMs);
  // Heartbeat: detect half-open TCP connections and clean them up.
  const heartbeat = setInterval(() => {
    for (const c of conns) {
      if (!c.alive) { c.terminate(); continue; }
      c.alive = false;
      c.ping();
    }
  }, heartbeatMs);
  ticker.unref(); heartbeat.unref();

  // ---------- http ----------
  const json = (res, code, body) => {
    res.writeHead(code, { 'content-type': 'application/json' });
    res.end(JSON.stringify(body));
  };
  const readBody = (req) => new Promise((resolve, reject) => {
    let s = '';
    req.on('data', (d) => { s += d; if (s.length > 10_000) { reject(new Error('too large')); req.destroy(); } });
    req.on('end', () => { try { resolve(JSON.parse(s || '{}')); } catch (e) { reject(e); } });
  });

  const server = http.createServer(async (req, res) => {
    const { pathname } = new URL(req.url, 'http://x');
    try {
      if (req.method === 'GET' && pathname === '/api/auctions') return json(res, 200, listAuctions(db));
      if (req.method === 'POST' && pathname === '/api/auctions') {
        const b = await readBody(req);
        if (!b.title) return json(res, 400, { error: 'title required' });
        return json(res, 201, createAuction(db, b));
      }
      const m = pathname.match(/^\/api\/auctions\/([\w-]+)$/);
      if (req.method === 'GET' && m) {
        const s = getState(db, m[1]);
        return s ? json(res, 200, s) : json(res, 404, { error: 'not found' });
      }
      if (req.method === 'GET' && (pathname === '/' || pathname === '/index.html')) {
        res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
        return res.end(fs.readFileSync(path.join(PUBLIC, 'index.html')));
      }
      json(res, 404, { error: 'not found' });
    } catch (e) {
      json(res, 400, { error: String(e.message) });
    }
  });
  server.on('upgrade', (req, socket) => {
    if (new URL(req.url, 'http://x').pathname !== '/ws') return socket.destroy();
    upgrade(req, socket, onConnection);
  });

  return {
    db, server, conns, subsFor: (id) => subs.get(id),
    listen: (port = 3000) => new Promise((r) => server.listen(port, () => r(server.address().port))),
    close: async () => {
      clearInterval(ticker); clearInterval(heartbeat);
      for (const c of conns) c.close(1001);
      await new Promise((r) => server.close(r));
      db.close();
    },
  };
}

// ---------- entrypoint ----------
if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) {
  const app = createApp({ dbPath: process.env.DB_PATH ?? './auction.db' });
  if (listAuctions(app.db).length === 0) {
    createAuction(app.db, { title: 'Vintage mechanical watch', startPrice: 5000, minIncrement: 500, durationSec: 600 });
  }
  const port = await app.listen(Number(process.env.PORT ?? 3000));
  console.log(`auction server on http://localhost:${port}`);
  for (const sig of ['SIGINT', 'SIGTERM']) process.on(sig, async () => { await app.close(); process.exit(0); });
}
