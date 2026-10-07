// Persistence + the race-sensitive bid transaction.
// Zero dependencies: uses Node's built-in SQLite (node:sqlite).
import { DatabaseSync } from 'node:sqlite';
import { randomUUID } from 'node:crypto';

const SCHEMA = `
CREATE TABLE IF NOT EXISTS auctions (
  id             TEXT PRIMARY KEY,
  title          TEXT NOT NULL,
  start_price    INTEGER NOT NULL CHECK (start_price >= 0),
  min_increment  INTEGER NOT NULL CHECK (min_increment > 0),
  ends_at        INTEGER NOT NULL,                 -- epoch ms, server clock
  status         TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','closed')),
  current_bid    INTEGER,
  current_bidder TEXT,
  bid_count      INTEGER NOT NULL DEFAULT 0,
  version        INTEGER NOT NULL DEFAULT 0        -- +1 on every state change
);
CREATE TABLE IF NOT EXISTS bids (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  auction_id    TEXT NOT NULL REFERENCES auctions(id),
  bidder        TEXT NOT NULL,
  amount        INTEGER NOT NULL,
  client_msg_id TEXT NOT NULL,
  created_at    INTEGER NOT NULL,
  UNIQUE (auction_id, bidder, client_msg_id),      -- idempotent retries
  UNIQUE (auction_id, amount)                      -- DB-level backstop: no ties
);
CREATE INDEX IF NOT EXISTS bids_by_auction ON bids (auction_id, id);
`;

export function openDb(path) {
  const db = new DatabaseSync(path);
  // WAL: readers never block the writer. busy_timeout: a second connection/process
  // waiting for the write lock queues up instead of failing with SQLITE_BUSY.
  db.exec('PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000; PRAGMA synchronous = NORMAL; PRAGMA foreign_keys = ON;');
  db.exec(SCHEMA);
  return db;
}

/** Run fn inside a transaction. IMMEDIATE takes the write lock up front, so the
 *  read-check-write sequence inside fn cannot interleave with any other writer. */
function tx(db, mode, fn) {
  db.exec(`BEGIN ${mode}`);
  try {
    const out = fn();
    db.exec('COMMIT');
    return out;
  } catch (e) {
    try { db.exec('ROLLBACK'); } catch { /* already rolled back */ }
    throw e;
  }
}

const toAuction = (r) => r && ({
  id: r.id, title: r.title, startPrice: r.start_price, minIncrement: r.min_increment,
  endsAt: r.ends_at, status: r.status, currentBid: r.current_bid, currentBidder: r.current_bidder,
  bidCount: r.bid_count, version: r.version,
  minNextBid: r.current_bid == null ? r.start_price : r.current_bid + r.min_increment,
});
const toBid = (r) => r && ({
  id: r.id, auctionId: r.auction_id, bidder: r.bidder, amount: r.amount, msgId: r.client_msg_id, at: r.created_at,
});

const loadAuction = (db, id) => toAuction(db.prepare('SELECT * FROM auctions WHERE id = ?').get(id));

export function createAuction(db, { title, startPrice = 1000, minIncrement = 100, durationSec = 300, now = Date.now() }) {
  const id = randomUUID().slice(0, 8);
  db.prepare('INSERT INTO auctions (id,title,start_price,min_increment,ends_at) VALUES (?,?,?,?,?)')
    .run(id, String(title).slice(0, 120), startPrice, minIncrement, now + durationSec * 1000);
  return loadAuction(db, id);
}

export const listAuctions = (db) =>
  db.prepare('SELECT * FROM auctions ORDER BY ends_at DESC').all().map(toAuction);

/** Consistent snapshot (auction row + recent bids read in ONE read transaction). */
export function getState(db, id, limit = 50) {
  return tx(db, 'DEFERRED', () => {
    const auction = loadAuction(db, id);
    if (!auction) return null;
    const bids = db.prepare('SELECT * FROM bids WHERE auction_id = ? ORDER BY id DESC LIMIT ?').all(id, limit).map(toBid);
    return { auction, bids };
  });
}

/**
 * Place a bid atomically.
 * Returns { ok, reason?, duplicate?, changed, auction, bid?, minAcceptable? }.
 *  - changed=true  => state mutated and a broadcast is due.
 */
export function placeBid(db, { auctionId, bidder, amount, msgId, now = Date.now() }) {
  return tx(db, 'IMMEDIATE', () => {
    // Everything below runs while holding the write lock. No other connection or
    // process can modify this auction until we COMMIT, so check-then-write is safe.
    const a = loadAuction(db, auctionId);
    if (!a) return { ok: false, reason: 'NOT_FOUND', changed: false };

    // Idempotency: a client retrying after a reconnect gets the original outcome.
    const prior = db.prepare('SELECT * FROM bids WHERE auction_id=? AND bidder=? AND client_msg_id=?')
      .get(auctionId, bidder, msgId);
    if (prior) return { ok: true, duplicate: true, changed: false, auction: a, bid: toBid(prior) };

    // Lazy close: bids are judged against the server clock, never the client's.
    if (a.status === 'open' && now >= a.endsAt) {
      db.prepare("UPDATE auctions SET status='closed', version=version+1 WHERE id=?").run(auctionId);
      return { ok: false, reason: 'AUCTION_CLOSED', changed: true, auction: loadAuction(db, auctionId) };
    }
    if (a.status !== 'open') return { ok: false, reason: 'AUCTION_CLOSED', changed: false, auction: a };

    if (a.currentBidder === bidder)
      return { ok: false, reason: 'ALREADY_HIGHEST', changed: false, auction: a };
    if (amount < a.minNextBid)
      return { ok: false, reason: 'TOO_LOW', minAcceptable: a.minNextBid, changed: false, auction: a };

    const { lastInsertRowid } = db.prepare(
      'INSERT INTO bids (auction_id,bidder,amount,client_msg_id,created_at) VALUES (?,?,?,?,?)'
    ).run(auctionId, bidder, amount, msgId, now);
    db.prepare('UPDATE auctions SET current_bid=?, current_bidder=?, bid_count=bid_count+1, version=version+1 WHERE id=?')
      .run(amount, bidder, auctionId);

    const bid = toBid(db.prepare('SELECT * FROM bids WHERE id=?').get(lastInsertRowid));
    return { ok: true, changed: true, auction: loadAuction(db, auctionId), bid };
  });
}

/** Close every auction whose time is up. Returns the auctions that changed. */
export function closeExpired(db, now = Date.now()) {
  const due = db.prepare("SELECT id FROM auctions WHERE status='open' AND ends_at <= ?").all(now);
  const closed = [];
  for (const { id } of due) {
    tx(db, 'IMMEDIATE', () => {
      // Re-check under the lock: a concurrent process may have closed it already.
      const r = db.prepare("UPDATE auctions SET status='closed', version=version+1 WHERE id=? AND status='open'").run(id);
      if (r.changes) closed.push(loadAuction(db, id));
    });
  }
  return closed;
}
