const { v4: uuidv4 } = require('uuid');

const queues = { male: [], female: [] };
const rooms = new Map();       // roomId -> { sockets, createdAt }
const socketToRoom = new Map(); // socketId -> roomId

const MAX_QUEUE = 1000;
const SAME_GENDER_WAIT_MS = 60000; // 60s before fallback to same gender

const addToQueue = (socketId, gender) => {
  const q = queues[gender];
  if (q.length >= MAX_QUEUE) return false;
  if (q.find((x) => x.socketId === socketId)) return true;
  q.push({ socketId, gender, joinedAt: Date.now() });
  return true;
};

const removeFromQueues = (socketId) => {
  ['male', 'female'].forEach((g) => {
    const idx = queues[g].findIndex((x) => x.socketId === socketId);
    if (idx !== -1) queues[g].splice(idx, 1);
  });
};

const createRoom = (idA, idB, io) => {
  const roomId = uuidv4();
  rooms.set(roomId, { sockets: [idA, idB], createdAt: Date.now() });
  socketToRoom.set(idA, roomId);
  socketToRoom.set(idB, roomId);

  const sockA = io.sockets.sockets.get(idA);
  const sockB = io.sockets.sockets.get(idB);
  sockA?.join(roomId);
  sockB?.join(roomId);

  io.to(idA).emit('matched', { roomId });
  io.to(idB).emit('matched', { roomId });
};

const findMatch = (socketId, gender, queuedAt, io) => {
  const opposite = gender === 'male' ? 'female' : 'male';
  const oppositeQ = queues[opposite];
  const sameQ = queues[gender];

  // Always prefer opposite gender
  if (oppositeQ.length > 0) {
    const partner = oppositeQ.shift();
    createRoom(socketId, partner.socketId, io);
    return true;
  }

  // Fallback: same gender after long wait (30% chance to avoid infinite wait)
  const waited = Date.now() - queuedAt > SAME_GENDER_WAIT_MS;
  if (waited && sameQ.length > 0 && Math.random() < 0.3) {
    const partner = sameQ.shift();
    createRoom(socketId, partner.socketId, io);
    return true;
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

const getStats = () => ({
  maleQueue: queues.male.length,
  femaleQueue: queues.female.length,
  activeRooms: rooms.size,
  online: rooms.size * 2 + queues.male.length + queues.female.length,
});

// Prune stale queue entries (user disconnected without event)
const pruneStaleEntries = (io) => {
  ['male', 'female'].forEach((g) => {
    queues[g] = queues[g].filter((entry) => {
      const sock = io.sockets.sockets.get(entry.socketId);
      return !!sock;
    });
  });
};

module.exports = { addToQueue, removeFromQueues, findMatch, leaveRoom, getRoomId, getStats, pruneStaleEntries };
