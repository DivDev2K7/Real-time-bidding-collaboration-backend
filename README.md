# Live Auction Backend

Real-time auction: WebSockets for live push, SQLite (WAL) for persistence and locking. Zero npm dependencies (Node >= 22.13: built-in `node:sqlite`, hand-rolled RFC 6455 server in `src/ws.js`).

    npm start            # http://localhost:3000  (open in several tabs)
    npm test             # 6 tests incl. 40 concurrent bidders + 8 racing OS processes

## Protocol (JSON over `/ws`)
| client -> server | |
|---|---|
| `{type:"subscribe", auctionId}` | server replies `snapshot` (auction + last 50 bids), then live `update`s |
| `{type:"bid", auctionId, bidder, amount, msgId}` | amount = integer minor units; `msgId` = client-generated unique id |

| server -> client | |
|---|---|
| `snapshot` | `{auction, bids}` read in one consistent transaction |
| `update` | `{event:"bid"\|"closed", auction, bid?}` broadcast to all subscribers |
| `ack` | reply to the bidder: `{ok, reason?, duplicate?, minAcceptable?, auction}` |
| `error` | `BAD_JSON`, `BAD_BID`, `NOT_FOUND`, ... |

Reject reasons: `TOO_LOW` (below current + increment, includes ties), `ALREADY_HIGHEST`, `AUCTION_CLOSED`, `NOT_FOUND`.

HTTP: `GET /api/auctions`, `GET /api/auctions/:id`, `POST /api/auctions {title,startPrice,minIncrement,durationSec}`.

## How correctness is achieved
- **Race resolution:** `placeBid` runs in `BEGIN IMMEDIATE`: it takes SQLite's write lock *before* reading the current price, so read-check-write is atomic across connections and processes. A second writer waits (`busy_timeout`) and then sees the new price, so a stale/lower bid is rejected rather than overwriting. `UNIQUE(auction_id, amount)` is a DB-level backstop.
- **Persistence:** all state (auction row, bid ledger) is in SQLite. Memory holds only socket routing. Page loads / reconnects read the DB.
- **Ordering + gap detection:** each auction has a `version` incremented in the same transaction as the change. Broadcasts happen synchronously after COMMIT, so they leave in commit order; clients drop stale versions and re-subscribe on a gap.
- **No missed events on (re)subscribe:** snapshot read and subscription registration occur in the same synchronous tick.
- **Disconnects:** close/error removes the socket from the registry; heartbeat pings terminate half-open connections; slow consumers (>1 MiB backlog) are dropped. None of this touches persisted state.
- **Retries:** `msgId` is unique per (auction, bidder). A client resending after a reconnect gets `duplicate:true` with the original result, never a double bid.
- **Time:** the server clock decides when bids stop; an interval timer closes expired auctions and broadcasts `closed`.

## Known limits / next steps
- One Node process owns broadcasting. The DB lock is safe with multiple processes (tested), but cross-process fan-out needs Redis pub/sub or Postgres LISTEN/NOTIFY.
- No authentication: `bidder` is a self-declared name. Add auth and derive identity from the session.
- No per-connection rate limiting; no anti-sniping extension.
