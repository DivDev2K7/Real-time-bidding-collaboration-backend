import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import net from 'node:net';
import { startApp, connect, createAuction, sleep } from './helpers.js';
import { openDb } from '../src/db.js';

const shuffle = (a) => { for (let i = a.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [a[i], a[j]] = [a[j], a[i]]; } return a; };
const rows = (db, id) => db.prepare('SELECT * FROM bids WHERE auction_id=? ORDER BY id').all(id);
const assertLedgerValid = (db, id, inc) => {
  const b = rows(db, id);
  for (let i = 1; i < b.length; i++) assert.ok(b[i].amount >= b[i - 1].amount + inc, `bid ${i} not >= prev + increment`);
  const a = db.prepare('SELECT * FROM auctions WHERE id=?').get(id);
  assert.equal(a.bid_count, b.length);
  assert.equal(a.version, b.length);
  if (b.length) { assert.equal(a.current_bid, b.at(-1).amount); assert.equal(a.current_bidder, b.at(-1).bidder); }
};

test('40 simultaneous WebSocket bidders: highest wins, ledger strictly increasing, all clients converge', async () => {
  const { app, port } = await startApp();
  const a = createAuction(app.db, { title: 't', startPrice: 1000, minIncrement: 100, durationSec: 60 });
  const N = 40;
  const clients = await Promise.all(Array.from({ length: N }, () => connect(port)));
  await Promise.all(clients.map((c) => (c.send({ type: 'subscribe', auctionId: a.id }), c.waitFor((m) => m.type === 'snapshot'))));

  const amounts = shuffle(Array.from({ length: N }, (_, i) => 1000 + i * 100));
  // fire all bids "at once"
  clients.forEach((c, i) => c.send({ type: 'bid', auctionId: a.id, bidder: `u${amounts[i]}`, amount: amounts[i], msgId: `m${i}` }));
  const acks = await Promise.all(clients.map((c, i) => c.waitFor((m) => m.type === 'ack' && m.msgId === `m${i}`)));

  const max = Math.max(...amounts);
  const finalRow = app.db.prepare('SELECT * FROM auctions WHERE id=?').get(a.id);
  assert.equal(finalRow.current_bid, max);
  assert.equal(finalRow.current_bidder, `u${max}`);
  assertLedgerValid(app.db, a.id, 100);

  const accepted = acks.filter((x) => x.ok).length;
  assert.equal(accepted, rows(app.db, a.id).length);
  for (const x of acks.filter((x) => !x.ok)) assert.equal(x.reason, 'TOO_LOW');

  // every client ends on the same final version
  await sleep(100);
  for (const c of clients) {
    const last = c.msgs.filter((m) => m.type === 'update').at(-1);
    assert.equal(last?.auction.version ?? 0, finalRow.version);
    const versions = c.msgs.filter((m) => m.type === 'update').map((m) => m.auction.version);
    assert.deepEqual(versions, [...versions].sort((x, y) => x - y), 'events arrived out of order');
  }
  await Promise.all(clients.map((c) => c.close()));
  await app.close();
});

test('real lock contention: 8 OS processes with separate connections racing on one DB file', async () => {
  const { app, dbPath } = await startApp();
  const a = createAuction(app.db, { title: 't', startPrice: 1000, minIncrement: 100, durationSec: 60 });
  const all = shuffle(Array.from({ length: 200 }, (_, i) => 1000 + i * 100));
  const parts = Array.from({ length: 8 }, (_, w) => all.filter((_, i) => i % 8 === w));
  const out = await Promise.all(parts.map((p, w) => new Promise((resolve, reject) => {
    const cp = spawn(process.execPath, ['--disable-warning=ExperimentalWarning', 'test/worker.js', dbPath, a.id, `w${w}`, ...p], { stdio: ['ignore', 'pipe', 'inherit'] });
    let s = ''; cp.stdout.on('data', (d) => (s += d));
    cp.on('exit', (code) => (code === 0 ? resolve(JSON.parse(s)) : reject(new Error(`worker ${w} exit ${code}`))));
  })));
  assert.equal(out.reduce((n, o) => n + o.errors, 0), 0, 'no SQLITE_BUSY / lock errors');
  const check = openDb(dbPath);
  const max = Math.max(...all);
  assert.equal(check.prepare('SELECT current_bid c FROM auctions WHERE id=?').get(a.id).c, max);
  assertLedgerValid(check, a.id, 100);
  assert.equal(out.reduce((n, o) => n + o.accepted, 0), rows(check, a.id).length);
  check.close();
  await app.close();
});

test('rejections: low, equal, below increment, self-outbid', async () => {
  const { app, port } = await startApp();
  const a = createAuction(app.db, { title: 't', startPrice: 1000, minIncrement: 100, durationSec: 60 });
  const c = await connect(port);
  let n = 0;
  const bid = async (bidder, amount) => { const msgId = `x${n++}`; c.send({ type: 'bid', auctionId: a.id, bidder, amount, msgId }); return c.waitFor((m) => m.type === 'ack' && m.msgId === msgId); };
  assert.equal((await bid('alice', 999)).reason, 'TOO_LOW');           // below start
  assert.equal((await bid('alice', 1000)).ok, true);
  assert.equal((await bid('bob', 1000)).reason, 'TOO_LOW');            // tie
  const r = await bid('bob', 1050);                                    // under increment
  assert.equal(r.reason, 'TOO_LOW'); assert.equal(r.minAcceptable, 1100);
  assert.equal((await bid('alice', 5000)).reason, 'ALREADY_HIGHEST');
  assert.equal((await bid('bob', 1100)).ok, true);
  c.send({ type: 'bid', auctionId: a.id, bidder: 'x', amount: -5, msgId: 'bad' });
  assert.equal((await c.waitFor((m) => m.type === 'error' && m.msgId === 'bad')).reason, 'BAD_BID');
  assertLedgerValid(app.db, a.id, 100);
  await c.close(); await app.close();
});

test('reconnect: fresh client gets correct state; retried bid is idempotent; dead sockets are cleaned up', async () => {
  const { app, port } = await startApp();
  const a = createAuction(app.db, { title: 't', startPrice: 1000, minIncrement: 100, durationSec: 60 });
  const alice = await connect(port);
  alice.send({ type: 'subscribe', auctionId: a.id });
  await alice.waitFor((m) => m.type === 'snapshot');
  const bidMsg = { type: 'bid', auctionId: a.id, bidder: 'alice', amount: 1200, msgId: 'alice-1' };
  alice.send(bidMsg);
  await alice.waitFor((m) => m.type === 'ack' && m.ok);
  await alice.close();                                   // alice drops
  await sleep(50);
  assert.equal(app.conns.size, 0, 'closed connection removed from registry');

  const bob = await connect(port);                       // bob bids while alice is offline
  bob.send({ type: 'bid', auctionId: a.id, bidder: 'bob', amount: 1500, msgId: 'bob-1' });
  await bob.waitFor((m) => m.type === 'ack' && m.ok);

  const alice2 = await connect(port);                    // alice returns
  alice2.send({ type: 'subscribe', auctionId: a.id });
  const snap = await alice2.waitFor((m) => m.type === 'snapshot');
  assert.equal(snap.auction.currentBid, 1500);
  assert.equal(snap.auction.currentBidder, 'bob');
  assert.deepEqual(snap.bids.map((b) => b.amount), [1500, 1200]);

  alice2.send(bidMsg);                                   // client can't know if first send landed -> resend
  const ack = await alice2.waitFor((m) => m.type === 'ack' && m.msgId === 'alice-1');
  assert.equal(ack.ok, true); assert.equal(ack.duplicate, true);
  assert.equal(rows(app.db, a.id).length, 2, 'no double-apply');

  // fresh page load via HTTP agrees with the DB
  const http = await (await fetch(`http://127.0.0.1:${port}/api/auctions/${a.id}`)).json();
  assert.equal(http.auction.currentBid, 1500);
  await bob.close(); await alice2.close(); await app.close();
});

test('abrupt TCP drop (no close frame, bid in flight) does not corrupt state or leak subscriptions', async () => {
  const { app, port } = await startApp();
  try {
    const a = createAuction(app.db, { title: 't', startPrice: 1000, minIncrement: 100, durationSec: 60 });
    const maskedFrame = (obj) => {            // client->server frames must be masked
      const body = Buffer.from(JSON.stringify(obj)), mask = Buffer.from([1, 2, 3, 4]);
      const hdr = body.length < 126 ? Buffer.from([0x81, 0x80 | body.length]) : Buffer.from([0x81, 0x80 | 126, body.length >> 8, body.length & 255]);
      return Buffer.concat([hdr, mask, Buffer.from(body.map((b, i) => b ^ mask[i & 3]))]);
    };
    const sock = net.connect(port, '127.0.0.1');
    await new Promise((r) => sock.once('connect', r));
    sock.write(`GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n`);
    await new Promise((r) => sock.once('data', r));                       // 101 response
    sock.write(Buffer.concat([
      maskedFrame({ type: 'subscribe', auctionId: a.id }),
      maskedFrame({ type: 'bid', auctionId: a.id, bidder: 'carol', amount: 2000, msgId: 'c1' }),
    ]));
    sock.destroy();                                                        // yank the cable
    await sleep(200);
    assert.equal(app.conns.size, 0, 'dead connection removed');
    assert.equal([...(app.subsFor?.(a.id) ?? [])].length, 0);
    assertLedgerValid(app.db, a.id, 100);                                  // applied fully or not at all
  } finally { await app.close(); }
});

test('auction ends on server clock: closed event broadcast, later bids rejected', async () => {
  const { app, port } = await startApp();
  const a = createAuction(app.db, { title: 't', startPrice: 1000, minIncrement: 100, durationSec: 0.4 });
  const c = await connect(port);
  c.send({ type: 'subscribe', auctionId: a.id });
  await c.waitFor((m) => m.type === 'snapshot');
  c.send({ type: 'bid', auctionId: a.id, bidder: 'dave', amount: 1000, msgId: 'd1' });
  await c.waitFor((m) => m.type === 'ack' && m.ok);
  const closed = await c.waitFor((m) => m.type === 'update' && m.event === 'closed');
  assert.equal(closed.auction.status, 'closed');
  c.send({ type: 'bid', auctionId: a.id, bidder: 'erin', amount: 9999, msgId: 'e1' });
  assert.equal((await c.waitFor((m) => m.type === 'ack' && m.msgId === 'e1')).reason, 'AUCTION_CLOSED');
  assert.equal(rows(app.db, a.id).length, 1);
  await c.close(); await app.close();
});
