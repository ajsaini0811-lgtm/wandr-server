const { v4: uuidv4 } = require('uuid');

const queues = { male: [], female: [] };
const rooms = new Map();        // roomId -> { sockets, createdAt }
const socketToRoom = new Map(); // socketId -> roomId

const MAX_QUEUE           = 1000;
const SAME_GENDER_WAIT_MS = 30_000;

const countShared = (a, b) => {
  if (!a?.length || !b?.length) return 0;
  return a.filter((i) => b.includes(i)).length;
};

const addToQueue = (socketId, gender, interests = []) => {
  const q = queues[gender];
  if (q.length >= MAX_QUEUE) return false;
  if (q.find((x) => x.socketId === socketId)) return true;
  q.push({ socketId, gender, interests, joinedAt: Date.now() });
  return true;
};

const removeFromQueues = (socketId) => {
  ['male', 'female'].forEach((g) => {
    const idx = queues[g].findIndex((x) => x.socketId === socketId);
    if (idx !== -1) queues[g].splice(idx, 1);
  });
};

const isInQueue = (socketId) =>
  queues.male.some((x) => x.socketId === socketId) ||
  queues.female.some((x) => x.socketId === socketId);

const isConnected = (socketId, io) => io.sockets.sockets.has(socketId);

const createRoom = (idA, idB, io) => {
  const roomId = uuidv4();
  rooms.set(roomId, { sockets: [idA, idB], createdAt: Date.now() });
  socketToRoom.set(idA, roomId);
  socketToRoom.set(idB, roomId);

  io.sockets.sockets.get(idA)?.join(roomId);
  io.sockets.sockets.get(idB)?.join(roomId);

  io.to(idA).emit('matched', { roomId });
  io.to(idB).emit('matched', { roomId });
};

// Pick the best candidate from q for this seeker (most shared interests).
// Removes the winner from the queue.
const pickBest = (q, seekerId, seekerInterests, io) => {
  let bestIdx   = -1;
  let bestScore = -1;

  for (let i = 0; i < q.length; i++) {
    const c = q[i];
    if (c.socketId === seekerId)          continue;
    if (!isConnected(c.socketId, io))    continue;
    if (socketToRoom.has(c.socketId))    continue;

    const score = countShared(seekerInterests, c.interests) * 100
                + (Date.now() - c.joinedAt) / 1000; // longer wait = slight priority

    if (score > bestScore) { bestScore = score; bestIdx = i; }
  }

  if (bestIdx === -1) return null;
  const [winner] = q.splice(bestIdx, 1);
  return winner;
};

// ── Global matchmaking tick ───────────────────────────────────────────────────
// Called once per interval by the server — handles ALL queued users at once.
// O(n) instead of O(n²) compared to per-socket intervals.
const processQueues = (io, onlineCount) => {
  pruneStaleEntries(io);
  const now = Date.now();

  const tryMatch = (seeker, targetQ) => {
    if (socketToRoom.has(seeker.socketId)) return true; // already matched elsewhere
    if (!isConnected(seeker.socketId, io)) return true; // dead socket, skip
    const partner = pickBest(targetQ, seeker.socketId, seeker.interests, io);
    if (partner) {
      createRoom(seeker.socketId, partner.socketId, io);
      return true;
    }
    return false;
  };

  // Phase 1: opposite-gender — iterate backwards so splice doesn't skip entries
  for (let i = queues.male.length - 1; i >= 0; i--) {
    if (tryMatch(queues.male[i], queues.female)) queues.male.splice(i, 1);
  }
  for (let i = queues.female.length - 1; i >= 0; i--) {
    if (tryMatch(queues.female[i], queues.male)) queues.female.splice(i, 1);
  }

  // Phase 2: same-gender fallback for long waiters
  for (const gender of ['male', 'female']) {
    for (let i = queues[gender].length - 1; i >= 0; i--) {
      const s = queues[gender][i];
      if (now - s.joinedAt < SAME_GENDER_WAIT_MS) continue;
      if (tryMatch(s, queues[gender])) queues[gender].splice(i, 1);
    }
  }

  // Phase 3: push live queue updates to all still-waiting sockets
  for (const gender of ['male', 'female']) {
    for (const entry of queues[gender]) {
      if (!isConnected(entry.socketId, io)) continue;
      io.to(entry.socketId).emit('queue_update', {
        online:      onlineCount,
        waitSeconds: Math.round((now - entry.joinedAt) / 1000),
      });
    }
  }
};

const leaveRoom = (socketId, io) => {
  const roomId = socketToRoom.get(socketId);
  if (!roomId) return;

  const room = rooms.get(roomId);
  if (room) {
    const partnerId = room.sockets.find((id) => id !== socketId);
    if (partnerId) io.to(partnerId).emit('partner_disconnected');
    room.sockets.forEach((id) => socketToRoom.delete(id));
    rooms.delete(roomId);
  }
};

const getRoomId = (socketId) => socketToRoom.get(socketId);
const isInRoom  = (socketId) => socketToRoom.has(socketId);

const getStats = (onlineCount) => ({
  maleQueue:   queues.male.length,
  femaleQueue: queues.female.length,
  activeRooms: rooms.size,
  online:      onlineCount,
});

function pruneStaleEntries(io) {
  ['male', 'female'].forEach((g) => {
    queues[g] = queues[g].filter(
      (e) => isConnected(e.socketId, io) && !socketToRoom.has(e.socketId)
    );
  });
}

module.exports = {
  addToQueue, removeFromQueues, isInQueue, processQueues,
  leaveRoom, getRoomId, isInRoom, getStats, pruneStaleEntries,
};
