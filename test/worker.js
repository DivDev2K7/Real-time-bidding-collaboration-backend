// Child process: its OWN SQLite connection hammering the same DB file.
import { openDb, placeBid } from '../src/db.js';
const [, , dbPath, auctionId, who, ...amounts] = process.argv;
const db = openDb(dbPath);
let accepted = 0, rejected = 0, errors = 0;
for (const a of amounts) {
  try {
    const r = placeBid(db, { auctionId, bidder: `${who}-${a}`, amount: Number(a), msgId: `m-${who}-${a}` });
    r.ok ? accepted++ : rejected++;
  } catch (e) { errors++; console.error(e.message); }
}
console.log(JSON.stringify({ accepted, rejected, errors }));
