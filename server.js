/**
 * Wandr — Anonymous Chat Server
 * Privacy: no chat logs, no IPs stored, no biometric data, ephemeral rooms only.
 */

const express = require('express');
const http    = require('http');
const { Server } = require('socket.io');
const helmet     = require('helmet');
const rateLimit  = require('express-rate-limit');
const cors       = require('cors');
const xss        = require('xss');
const { v4: uuidv4 } = require('uuid');
const mm = require('./src/matchmaking');

const app    = express();
const server = http.createServer(app);

const CLIENT_URL      = process.env.CLIENT_URL || 'http://localhost:5173';
const ALLOWED_ORIGINS = [CLIENT_URL, 'http://localhost:4173'];

// ── Security headers ──────────────────────────────────────────────────────────
app.use(helmet({ crossOriginEmbedderPolicy: false }));
app.set('trust proxy', 1);
app.use(cors({ origin: ALLOWED_ORIGINS, credentials: true }));
app.use(rateLimit({ windowMs: 60_000, max: 60, standardHeaders: true, legacyHeaders: false }));
app.use(express.json({ limit: '10kb' }));

// ── Track real online count ───────────────────────────────────────────────────
const onlineSockets = new Set(); // one entry per connected socket

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', ...mm.getStats(onlineSockets.size) });
});
app.get('/', (_req, res) => res.json({ service: 'Wandr', status: 'ok' }));

// ── Socket.io ─────────────────────────────────────────────────────────────────
const io = new Server(server, {
  cors: { origin: ALLOWED_ORIGINS, methods: ['GET', 'POST'] },
  maxHttpBufferSize: 5e6, // 5MB — needed for compressed image payloads
  connectTimeout: 10000,
  pingTimeout: 30000,
  pingInterval: 15000,
  transports: ['websocket', 'polling'],
});

// ── Per-socket flood protection ───────────────────────────────────────────────
const msgRates = new Map();
const isFlooding = (id) => {
  const now = Date.now();
  let e = msgRates.get(id) || { count: 0, resetAt: now + 1000 };
  if (now > e.resetAt) e = { count: 0, resetAt: now + 1000 };
  e.count++;
  msgRates.set(id, e);
  return e.count > 4;
};

// ── IP connection rate limit ──────────────────────────────────────────────────
const ipCounts = new Map();
io.use((socket, next) => {
  const ip  = socket.handshake.address;
  const now = Date.now();
  let e = ipCounts.get(ip) || { count: 0, resetAt: now + 60_000 };
  if (now > e.resetAt) e = { count: 0, resetAt: now + 60_000 };
  e.count++;
  ipCounts.set(ip, e);
  if (e.count > 20) return next(new Error('Too many connections'));
  next();
});

// ── Reports ───────────────────────────────────────────────────────────────────
const reports = [];

// ── Main connection handler ───────────────────────────────────────────────────
io.on('connection', (socket) => {
  onlineSockets.add(socket.id);

  let userGender   = null;
  let inQueue      = false;
  let queuedAt     = null;
  let matchInterval = null;

  const stopSearching = () => {
    if (matchInterval) { clearInterval(matchInterval); matchInterval = null; }
    inQueue  = false;
    queuedAt = null;
    mm.removeFromQueues(socket.id);
  };

  // Broadcast updated online count to everyone
  const broadcastOnline = () => {
    io.emit('online_count', { online: onlineSockets.size });
  };

  broadcastOnline();

  // ── Set gender ──────────────────────────────────────────────────────────────
  socket.on('set_gender', (data) => {
    if (!data || !['male', 'female'].includes(data.gender)) {
      socket.emit('error_msg', { message: 'Invalid gender' });
      return;
    }
    userGender = data.gender;
    socket.emit('gender_accepted', { gender: userGender });
  });

  // ── Find match ──────────────────────────────────────────────────────────────
  socket.on('find_match', () => {
    if (!userGender) { socket.emit('error_msg', { message: 'Set gender first' }); return; }
    if (inQueue) return;

    inQueue  = true;
    queuedAt = Date.now();

    const tryMatch = () => {
      // Stop if already matched by someone else
      if (mm.isInRoom(socket.id)) {
        stopSearching();
        return;
      }

      const matched = mm.findMatch(socket.id, userGender, queuedAt, io);
      if (matched) {
        stopSearching();
      } else {
        mm.addToQueue(socket.id, userGender);
        const stats = mm.getStats(onlineSockets.size);
        socket.emit('queue_update', {
          online:      stats.online,
          waitSeconds: Math.round((Date.now() - queuedAt) / 1000),
        });
      }
    };

    // Set interval BEFORE first call so stopSearching() can clear it immediately
    matchInterval = setInterval(tryMatch, 2500);
    tryMatch();
  });

  // ── Send message ────────────────────────────────────────────────────────────
  socket.on('send_message', (data) => {
    if (isFlooding(socket.id)) { socket.emit('rate_limited'); return; }

    const roomId = mm.getRoomId(socket.id);
    if (!roomId) return;

    let text = data?.text;
    if (typeof text !== 'string') return;
    text = xss(text.trim()).slice(0, 500);
    if (!text) return;

    // Send ONLY to partner (not back to sender — prevents double messages)
    socket.to(roomId).emit('message', {
      id:   uuidv4(),
      text,
      from: socket.id,
      ts:   Date.now(),
    });
  });

  // ── Send photo (one-time view) ───────────────────────────────────────────────
  socket.on('send_photo', (data) => {
    if (isFlooding(socket.id)) { socket.emit('rate_limited'); return; }

    const roomId = mm.getRoomId(socket.id);
    if (!roomId) return;

    const { imageData } = data || {};
    if (typeof imageData !== 'string') return;
    if (!imageData.startsWith('data:image/')) return;
    if (imageData.length > 4_500_000) { // ~3.4MB base64 limit
      socket.emit('error_msg', { message: 'Image too large. Max 3MB.' });
      return;
    }

    // Forward ONLY to partner — image is never stored, never logged
    socket.to(roomId).emit('photo_message', {
      id: uuidv4(),
      imageData,
      ts: Date.now(),
    });

    socket.emit('photo_sent', { id: uuidv4(), ts: Date.now() });
  });

  // ── Typing indicators ────────────────────────────────────────────────────────
  socket.on('typing',      () => { const r = mm.getRoomId(socket.id); if (r) socket.to(r).emit('partner_typing'); });
  socket.on('stop_typing', () => { const r = mm.getRoomId(socket.id); if (r) socket.to(r).emit('partner_stopped_typing'); });

  // ── Skip ─────────────────────────────────────────────────────────────────────
  socket.on('skip', () => {
    stopSearching();
    mm.leaveRoom(socket.id, io);
    socket.emit('skipped');
  });

  // ── Report ────────────────────────────────────────────────────────────────────
  socket.on('report', (data) => {
    const valid = ['harassment', 'nudity', 'spam', 'underage', 'other'];
    reports.push({ category: valid.includes(data?.category) ? data.category : 'other', ts: Date.now() });
    stopSearching();
    mm.leaveRoom(socket.id, io);
    socket.emit('skipped');
  });

  // ── Disconnect ────────────────────────────────────────────────────────────────
  socket.on('disconnect', () => {
    onlineSockets.delete(socket.id);
    stopSearching();
    mm.leaveRoom(socket.id, io);
    msgRates.delete(socket.id);
    broadcastOnline();
  });
});

// ── Periodic cleanup ──────────────────────────────────────────────────────────
setInterval(() => {
  mm.pruneStaleEntries(io);
  const now = Date.now();
  ipCounts.forEach((e, ip) => { if (now > e.resetAt) ipCounts.delete(ip); });
  msgRates.forEach((e, id) => { if (now > e.resetAt) msgRates.delete(id); });
}, 60_000); // every minute (was 5 min)

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => console.log(`Whisper server :${PORT}`));
