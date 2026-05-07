/**
 * Wandr — Anonymous Chat Server
 *
 * Privacy guarantees:
 *   - No chat messages are logged or stored
 *   - No IP addresses are persisted
 *   - Only gender string ('male'/'female') is accepted from clients
 *   - All rooms are ephemeral (in-memory only, cleared on server restart)
 *   - Face analysis runs entirely in the browser; this server never sees images
 */

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const cors = require('cors');
const xss = require('xss');
const { v4: uuidv4 } = require('uuid');
const mm = require('./src/matchmaking');

const app = express();
const server = http.createServer(app);

const CLIENT_URL = process.env.CLIENT_URL || 'http://localhost:5173';
const ALLOWED_ORIGINS = [CLIENT_URL, 'http://localhost:4173'];

// ── Security headers ─────────────────────────────────────────────────────────
app.use(
  helmet({
    crossOriginEmbedderPolicy: false, // camera API needs this off
    contentSecurityPolicy: {
      directives: {
        defaultSrc: ["'self'"],
        connectSrc: ["'self'", ...ALLOWED_ORIGINS],
      },
    },
  })
);

app.set('trust proxy', 1);

// ── CORS ──────────────────────────────────────────────────────────────────────
app.use(cors({ origin: ALLOWED_ORIGINS, credentials: true }));

// ── HTTP rate limit ───────────────────────────────────────────────────────────
app.use(
  rateLimit({
    windowMs: 60 * 1000,
    max: 60,
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: 'Too many requests' },
  })
);

app.use(express.json({ limit: '10kb' }));

// ── Health check (no sensitive info exposed) ──────────────────────────────────
app.get('/health', (_req, res) => {
  const { online, activeRooms } = mm.getStats();
  res.json({ status: 'ok', online, activeRooms });
});

app.get('/', (_req, res) => res.json({ service: 'Wandr Chat Server', status: 'ok' }));

// ── Socket.io ─────────────────────────────────────────────────────────────────
const io = new Server(server, {
  cors: { origin: ALLOWED_ORIGINS, methods: ['GET', 'POST'] },
  maxHttpBufferSize: 1e4,   // 10KB max per event payload
  connectTimeout: 10000,
  pingTimeout: 60000,
  pingInterval: 25000,
  transports: ['websocket', 'polling'],
});

// ── Per-socket message flood protection ───────────────────────────────────────
const msgRates = new Map(); // socketId -> { count, resetAt }

const isFlooding = (socketId) => {
  const now = Date.now();
  let entry = msgRates.get(socketId) || { count: 0, resetAt: now + 1000 };
  if (now > entry.resetAt) { entry = { count: 0, resetAt: now + 1000 }; }
  entry.count++;
  msgRates.set(socketId, entry);
  return entry.count > 4; // max 4 messages per second
};

// ── Connection rate limit per IP ───────────────────────────────────────────────
const ipConnects = new Map(); // ip -> { count, resetAt }

io.use((socket, next) => {
  const ip = socket.handshake.address;
  const now = Date.now();
  let entry = ipConnects.get(ip) || { count: 0, resetAt: now + 60000 };
  if (now > entry.resetAt) { entry = { count: 0, resetAt: now + 60000 }; }
  entry.count++;
  ipConnects.set(ip, entry);
  if (entry.count > 15) return next(new Error('Too many connections from this IP'));
  next();
});

// ── Reports (in-memory only, no message content stored) ───────────────────────
const reports = [];

// ── Main socket handler ───────────────────────────────────────────────────────
io.on('connection', (socket) => {
  let userGender = null;
  let inQueue = false;
  let queuedAt = null;
  let matchInterval = null;

  const stopSearching = () => {
    if (matchInterval) { clearInterval(matchInterval); matchInterval = null; }
    inQueue = false;
    queuedAt = null;
  };

  // ── 1. Client declares gender (only string accepted — no face data ever arrives here)
  socket.on('set_gender', (data) => {
    if (!data || !['male', 'female'].includes(data.gender)) {
      socket.emit('error_msg', { message: 'Invalid gender value' });
      return;
    }
    if (typeof data.confidence !== 'number' || data.confidence < 0.55) {
      socket.emit('error_msg', { message: 'Confidence too low — please try again' });
      return;
    }
    userGender = data.gender;
    socket.emit('gender_accepted', { gender: userGender });
  });

  // ── 2. Client requests matchmaking
  socket.on('find_match', () => {
    if (!userGender) { socket.emit('error_msg', { message: 'Set gender first' }); return; }
    if (inQueue) return;

    inQueue = true;
    queuedAt = Date.now();

    const tryMatch = () => {
      const matched = mm.findMatch(socket.id, userGender, queuedAt, io);
      if (matched) {
        stopSearching();
      } else {
        mm.addToQueue(socket.id, userGender);
        const stats = mm.getStats();
        const queuePos = userGender === 'male' ? stats.maleQueue : stats.femaleQueue;
        socket.emit('queue_update', {
          position: queuePos,
          waitSeconds: Math.round((Date.now() - queuedAt) / 1000),
          online: stats.online,
        });
      }
    };

    tryMatch();
    matchInterval = setInterval(tryMatch, 2500);
  });

  // ── 3. Send a chat message
  socket.on('send_message', (data) => {
    if (isFlooding(socket.id)) {
      socket.emit('rate_limited', { message: 'Slow down a bit!' });
      return;
    }

    const roomId = mm.getRoomId(socket.id);
    if (!roomId) return;

    let text = data?.text;
    if (typeof text !== 'string') return;

    // Sanitize: strip HTML/XSS, trim, limit length
    text = xss(text.trim()).slice(0, 500);
    if (!text) return;

    socket.to(roomId).emit('message', {
      id: uuidv4(),
      text,
      from: socket.id,
      ts: Date.now(),
    });
  });

  // ── 4. Typing indicators
  socket.on('typing', () => {
    const roomId = mm.getRoomId(socket.id);
    if (roomId) socket.to(roomId).emit('partner_typing');
  });

  socket.on('stop_typing', () => {
    const roomId = mm.getRoomId(socket.id);
    if (roomId) socket.to(roomId).emit('partner_stopped_typing');
  });

  // ── 5. Skip current partner → go back to queue
  socket.on('skip', () => {
    stopSearching();
    mm.leaveRoom(socket.id, io);
    mm.removeFromQueues(socket.id);
    socket.emit('skipped');
  });

  // ── 6. Report abuse (logs category only — no message content, no PII)
  socket.on('report', (data) => {
    const validCategories = ['harassment', 'nudity', 'spam', 'underage', 'other'];
    const category = validCategories.includes(data?.category) ? data.category : 'other';
    reports.push({ category, ts: Date.now() }); // no room ID, no socket ID
    socket.emit('report_received');
    // Auto-skip after report
    stopSearching();
    mm.leaveRoom(socket.id, io);
    mm.removeFromQueues(socket.id);
    socket.emit('skipped');
  });

  // ── 7. Clean disconnect
  socket.on('disconnect', () => {
    stopSearching();
    mm.leaveRoom(socket.id, io);
    mm.removeFromQueues(socket.id);
    msgRates.delete(socket.id);
  });
});

// ── Periodic cleanup ──────────────────────────────────────────────────────────
setInterval(() => {
  // Prune disconnected sockets from queue (safety net)
  mm.pruneStaleEntries(io);

  // Prune stale IP entries
  const now = Date.now();
  ipConnects.forEach((entry, ip) => { if (now > entry.resetAt) ipConnects.delete(ip); });
  msgRates.forEach((entry, id) => { if (now > entry.resetAt) msgRates.delete(id); });
}, 5 * 60 * 1000);

// ── Start ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`Wandr server running on :${PORT}`);
  console.log(`Allowed origins: ${ALLOWED_ORIGINS.join(', ')}`);
});
