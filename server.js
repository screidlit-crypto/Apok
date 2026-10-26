'use strict';
const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const crypto = require('crypto');
const PORT = Number(process.env.PORT || 3000);
const rooms = new Map();
const clients = new Map();
const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    res.end(JSON.stringify({ ok: true, service: 'zona-apocalypse-websocket-relay', rooms: rooms.size }));
  } else { res.writeHead(404); res.end('Not found'); }
});
const wss = new WebSocketServer({ server, maxPayload: 1024 * 1024, perMessageDeflate: false });
function send(ws, obj) { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj)); }
function roomCode() { return crypto.randomBytes(3).toString('hex').toUpperCase(); }
function remove(ws) {
  const c = clients.get(ws); if (!c) return;
  clients.delete(ws);
  if (c.room) {
    const r = rooms.get(c.room);
    if (r) {
      if (r.host === ws) {
        if (r.guest) send(r.guest, { _mp: 'error', message: 'Хост отключился. Комната закрыта.' });
        rooms.delete(c.room);
      } else if (r.guest === ws) {
        r.guest = null;
        send(r.host, { _mp: 'peer-left' });
      }
    }
  }
}
wss.on('connection', ws => {
  clients.set(ws, { room: null, role: null });
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw.toString()); } catch { send(ws, { _mp: 'error', message: 'Некорректный JSON.' }); return; }
    const c = clients.get(ws); if (!c) return;
    if (m.type === 'create') {
      if (c.room) return;
      let code; do { code = roomCode(); } while (rooms.has(code));
      rooms.set(code, { host: ws, guest: null }); c.room = code; c.role = 'host';
      send(ws, { _mp: 'created', room: code }); return;
    }
    if (m.type === 'join') {
      if (c.room) return;
      const code = String(m.room || '').trim().toUpperCase(); const r = rooms.get(code);
      if (!r) { send(ws, { _mp: 'error', message: 'Комната не найдена. Проверь код и попроси хоста создать комнату заново.' }); return; }
      if (r.guest) { send(ws, { _mp: 'error', message: 'В комнате уже есть второй игрок.' }); return; }
      r.guest = ws; c.room = code; c.role = 'guest';
      send(ws, { _mp: 'joined', room: code }); send(r.host, { _mp: 'peer-joined' }); return;
    }
    if (m.type === 'game' && m.data && c.room) {
      const r = rooms.get(c.room); if (!r) return;
      const target = c.role === 'host' ? r.guest : r.host;
      if (!target || target.readyState !== WebSocket.OPEN) return;
      // Skip obsolete movement/world packets when a receiver is already backed up.
      if (target.bufferedAmount > 1_000_000 && ['state', 'world'].includes(m.data.type)) return;
      send(target, { type: 'game', data: m.data });
    }
  });
  ws.on('close', () => remove(ws));
  ws.on('error', () => remove(ws));
});
server.listen(PORT, '0.0.0.0', () => console.log(`Zona relay listening on ${PORT}`));
