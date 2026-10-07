// Minimal RFC 6455 server-side WebSocket (text frames, ping/pong, close, fragmentation).
// Written dependency-free because the build sandbox is offline; in production you could
// swap this file for the `ws` package, since server.js only uses send/close/terminate/events.
import { createHash } from 'node:crypto';
import { EventEmitter } from 'node:events';

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MAX_PAYLOAD = 64 * 1024;

export function upgrade(req, socket, onConnection) {
  const key = req.headers['sec-websocket-key'];
  if (String(req.headers.upgrade).toLowerCase() !== 'websocket' || !key) {
    socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
    return;
  }
  const accept = createHash('sha1').update(key + GUID).digest('base64');
  socket.write([
    'HTTP/1.1 101 Switching Protocols',
    'Upgrade: websocket',
    'Connection: Upgrade',
    `Sec-WebSocket-Accept: ${accept}`,
    '', '',
  ].join('\r\n'));
  onConnection(new Conn(socket));
}

export class Conn extends EventEmitter {
  constructor(socket) {
    super();
    this.socket = socket;
    this.buf = Buffer.alloc(0);
    this.frag = null;        // in-progress fragmented message
    this.alive = true;       // heartbeat flag
    this.closed = false;
    this._closeEmitted = false;
    socket.setNoDelay(true);
    socket.on('data', (d) => { this.alive = true; this._onData(d); });
    socket.on('close', () => this._finish());
    socket.on('error', () => socket.destroy());
  }

  get bufferedAmount() { return this.socket.writableLength; }

  send(text) { this._write(0x1, Buffer.from(text)); }
  ping() { this._write(0x9, Buffer.alloc(0)); }

  close(code = 1000) {
    if (this.closed) return;
    const p = Buffer.alloc(2);
    p.writeUInt16BE(code);
    this._write(0x8, p);
    this.closed = true;
    this.socket.end();
    this._finish();
  }

  terminate() { this.closed = true; this.socket.destroy(); this._finish(); }

  _finish() {
    this.closed = true;
    if (this._closeEmitted) return;
    this._closeEmitted = true;
    this.emit('close');
  }

  _write(op, payload) {
    if (this.closed || this.socket.destroyed) return;
    const n = payload.length;
    let h;
    if (n < 126) h = Buffer.from([0x80 | op, n]);
    else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(n, 2); }
    else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); }
    this.socket.write(Buffer.concat([h, payload]));
  }

  _onData(d) {
    this.buf = Buffer.concat([this.buf, d]);
    for (;;) {
      if (this.closed || this.buf.length < 2) return;
      const b0 = this.buf[0], b1 = this.buf[1];
      const fin = !!(b0 & 0x80), op = b0 & 0x0f, masked = !!(b1 & 0x80);
      let len = b1 & 0x7f, off = 2;
      if (len === 126) {
        if (this.buf.length < 4) return;
        len = this.buf.readUInt16BE(2); off = 4;
      } else if (len === 127) {
        if (this.buf.length < 10) return;
        const big = this.buf.readBigUInt64BE(2);
        if (big > BigInt(MAX_PAYLOAD)) return this.close(1009);
        len = Number(big); off = 10;
      }
      if (!masked) return this.close(1002);        // clients MUST mask
      if (len > MAX_PAYLOAD) return this.close(1009);
      if (this.buf.length < off + 4 + len) return; // wait for the rest of the frame
      const mask = this.buf.subarray(off, off + 4);
      const payload = Buffer.from(this.buf.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < len; i++) payload[i] ^= mask[i & 3];
      this.buf = this.buf.subarray(off + 4 + len);
      this._frame(fin, op, payload);
    }
  }

  _frame(fin, op, payload) {
    switch (op) {
      case 0x8: return this.close(1000);                     // close handshake
      case 0x9: return this._write(0xA, payload);            // ping -> pong
      case 0xA: return;                                      // pong (alive already set)
      case 0x1: case 0x2: case 0x0: {
        if (op !== 0x0) {
          if (this.frag) return this.close(1002);
          this.frag = { op, chunks: [], size: 0 };
        } else if (!this.frag) return this.close(1002);
        this.frag.chunks.push(payload);
        this.frag.size += payload.length;
        if (this.frag.size > MAX_PAYLOAD) return this.close(1009);
        if (!fin) return;
        const { op: kind, chunks } = this.frag;
        this.frag = null;
        if (kind === 0x2) return this.close(1003);           // binary unsupported
        this.emit('message', Buffer.concat(chunks).toString('utf8'));
        return;
      }
      default: return this.close(1002);
    }
  }
}
