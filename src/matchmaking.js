const { v4: uuidv4 } = require('uuid');

const queues = { male: [], female: [] };
const rooms = new Map();        // roomId -> { sockets, createdAt }
const socketToRoom = new Map(); // socketId -> roomId

const MAX_QUEUE = 1000;
const SAME_GENDER_WAIT_MS = 60000;

const addToQueue = (socketId, gender) => {
  const q = queues[gender];
  if (q.length >= MAX_QUEUE) return false;
  if (q.find((x) => x.socketId === socketId)) return true; // already in queue
  q.push({ socketId, gender, joinedAt: Date.now() });
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

const findMatch = (socketId, gender, queuedAt, io) => {
  // Already matched by someone else while we were waiting — stop searching
  if (socketToRoom.has(socketId)) return true;

  const opposite = gender === 'male' ? 'female' : 'male';
  const sameQ    = queues[gender];
  const oppQ     = queues[opposite];

  // Pull the first still-connected opposite-gender socket
  const pickFrom = (q) => {
    while (q.length > 0) {
      const candidate = q.shift();
      if (candidate.socketId === socketId) continue;       // skip self
      if (!isConnected(candidate.socketId, io)) continue; // skip dead sockets
      if (socketToRoom.has(candidate.socketId)) continue; // skip already matched
      return candidate;
    }
    return null;
  };

  // Primary: opposite gender
  const oppMatch = pickFrom(oppQ);
  if (oppMatch) {
    createRoom(socketId, oppMatch.socketId, io);
    return true;
  }

  // Fallback: same gender after long wait
  const waited = Date.now() - queuedAt > SAME_GENDER_WAIT_MS;
  if (waited && Math.random() < 0.3) {
    const sameMatch = pickFrom(sameQ);
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

const getRoomId   = (socketId) => socketToRoom.get(socketId);
const isInRoom    = (socketId) => socketToRoom.has(socketId);

const getStats = (onlineCount) => ({
  maleQueue:   queues.male.length,
  femaleQueue: queues.female.length,
  activeRooms: rooms.size,
  online:      onlineCount,
});

const pruneStaleEntries = (io) => {
  ['male', 'female'].forEach((g) => {
    queues[g] = queues[g].filter((e) => isConnected(e.socketId, io) && !socketToRoom.has(e.socketId));
  });
};

module.exports = {
  addToQueue, removeFromQueues, findMatch,
  leaveRoom, getRoomId, isInRoom, getStats, pruneStaleEntries,
};
