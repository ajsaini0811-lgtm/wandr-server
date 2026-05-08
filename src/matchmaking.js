const { v4: uuidv4 } = require('uuid');

const queues = { male: [], female: [] };
const rooms = new Map();        // roomId -> { sockets, createdAt }
const socketToRoom = new Map(); // socketId -> roomId

const MAX_QUEUE       = 1000;
const SAME_GENDER_WAIT_MS = 30_000; // 30s before falling back to same gender

const countShared = (a, b) => {
  if (!a?.length || !b?.length) return 0;
  return a.filter((i) => b.includes(i)).length;
};

const addToQueue = (socketId, gender, interests = []) => {
  const q = queues[gender];
  if (q.length >= MAX_QUEUE) return false;
  if (q.find((x) => x.socketId === socketId)) return true; // already queued
  q.push({ socketId, gender, interests, joinedAt: Date.now() });
  return true;
};

const removeFromQueues = (socketId) => {
  ['male', 'female'].forEach((g) => {
    const idx = queues[g].findIndex((x) => x.socketId === socketId);
    if (idx !== -1) queues[g].splice(idx, 1);
  });
};

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

// Pick the best candidate from a queue based on shared interests.
// Removes the winner from the queue and returns them (or null if none found).
const pickBest = (q, socketId, seekerInterests, io) => {
  let bestIdx   = -1;
  let bestScore = -1;

  for (let i = 0; i < q.length; i++) {
    const c = q[i];
    if (c.socketId === socketId)          continue; // skip self
    if (!isConnected(c.socketId, io))    continue; // skip dead sockets
    if (socketToRoom.has(c.socketId))    continue; // skip already matched

    const shared = countShared(seekerInterests, c.interests);
    // More shared interests = higher score; older queue entries break ties
    const score  = shared * 100 + (Date.now() - c.joinedAt) / 1000;

    if (score > bestScore) {
      bestScore = score;
      bestIdx   = i;
    }
  }

  if (bestIdx === -1) return null;
  const [winner] = q.splice(bestIdx, 1);
  return winner;
};

const findMatch = (socketId, gender, interests, queuedAt, io) => {
  if (socketToRoom.has(socketId)) return true; // already matched

  const opposite = gender === 'male' ? 'female' : 'male';
  const waited   = Date.now() - queuedAt;

  // Priority 1: opposite gender — pick the one with most shared interests
  const oppMatch = pickBest(queues[opposite], socketId, interests, io);
  if (oppMatch) {
    createRoom(socketId, oppMatch.socketId, io);
    return true;
  }

  // Priority 2: same gender — only after 30s wait, no random gating
  if (waited >= SAME_GENDER_WAIT_MS) {
    const sameMatch = pickBest(queues[gender], socketId, interests, io);
    if (sameMatch) {
      createRoom(socketId, sameMatch.socketId, io);
      return true;
    }
  }

  return false;
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

const pruneStaleEntries = (io) => {
  ['male', 'female'].forEach((g) => {
    queues[g] = queues[g].filter(
      (e) => isConnected(e.socketId, io) && !socketToRoom.has(e.socketId)
    );
  });
};

module.exports = {
  addToQueue, removeFromQueues, findMatch,
  leaveRoom, getRoomId, isInRoom, getStats, pruneStaleEntries,
};
