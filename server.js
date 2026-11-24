const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { WebSocketServer, WebSocket } = require("ws");

const PORT = process.env.PORT || 10000;
const ROOT = __dirname;
const rooms = new Map();
const sockets = new Map();

const mimeTypes = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".modelbin": "application/octet-stream",
  ".glb": "model/gltf-binary",
  ".gltf": "model/gltf+json",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".svg": "image/svg+xml",
  ".mp3": "audio/mpeg",
  ".wav": "audio/wav"
};

function send(ws, data) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function roomCount(room) {
  return room ? room.players.size : 0;
}

function broadcast(room, data, except) {
  for (const player of room.players.values()) {
    if (player.ws !== except) send(player.ws, data);
  }
}

function removePlayer(ws) {
  const info = sockets.get(ws);
  if (!info) return;
  sockets.delete(ws);

  const room = rooms.get(info.code);
  if (!room) return;

  room.players.delete(info.playerId);

  if (info.playerId === room.hostId) {
    for (const player of room.players.values()) {
      send(player.ws, { type: "room-closed" });
      sockets.delete(player.ws);
      try { player.ws.close(1000, "Room host left"); } catch (_) {}
    }
    rooms.delete(info.code);
    return;
  }

  if (room.players.size === 0) {
    rooms.delete(info.code);
  } else {
    broadcast(room, {
      type: "player-left",
      playerId: info.playerId,
      count: roomCount(room)
    });
  }
}

function makeRoomCode() {
  let code;
  do {
    code = String(Math.floor(100000 + Math.random() * 900000));
  } while (rooms.has(code));
  return code;
}

const server = http.createServer((req, res) => {
  let pathname;
  try {
    pathname = decodeURIComponent(new URL(req.url, "http://localhost").pathname);
  } catch (_) {
    res.writeHead(400); return res.end("Bad request");
  }

  if (pathname === "/health") {
    res.writeHead(200, { "content-type": "text/plain; charset=utf-8" });
    return res.end("ok");
  }

  if (pathname === "/") pathname = "/index.html";
  const filePath = path.resolve(ROOT, "." + pathname);
  if (!filePath.startsWith(ROOT + path.sep) && filePath !== path.join(ROOT, "index.html")) {
    res.writeHead(403); return res.end("Forbidden");
  }

  fs.stat(filePath, (err, stat) => {
    if (err || !stat.isFile()) {
      res.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
      return res.end("Not found");
    }
    res.writeHead(200, {
      "content-type": mimeTypes[path.extname(filePath).toLowerCase()] || "application/octet-stream",
      "content-length": stat.size,
      "x-content-type-options": "nosniff"
    });
    fs.createReadStream(filePath).pipe(res);
  });
});

const wss = new WebSocketServer({ noServer: true, maxPayload: 16 * 1024 });

server.on("upgrade", (req, socket, head) => {
  let pathname = "/";
  try { pathname = new URL(req.url, "http://localhost").pathname; } catch (_) {}
  if (pathname !== "/ws") {
    socket.write("HTTP/1.1 404 Not Found\r\n\r\n");
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, ws => wss.emit("connection", ws, req));
});

wss.on("connection", ws => {
  ws.on("message", raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch (_) {
      return send(ws, { type: "error", message: "Geçersiz mesaj." });
    }

    const info = sockets.get(ws);

    if (msg.type === "create") {
      if (info) removePlayer(ws);
      const code = makeRoomCode();
      const playerId = crypto.randomUUID();
      const room = { code, hostId: playerId, players: new Map() };
      room.players.set(playerId, { id: playerId, ws, x: 0, y: 0, z: 0, yaw: 0, color: 0x20a9d6 });
      rooms.set(code, room);
      sockets.set(ws, { code, playerId });
      return send(ws, { type: "created", code, playerId, count: 1 });
    }

    if (msg.type === "join") {
      if (info) removePlayer(ws);
      const code = String(msg.code || "").replace(/\D/g, "").slice(0, 6);
      const room = rooms.get(code);
      if (!room) return send(ws, { type: "error", message: "Bu oda bulunamadı veya kapatılmış." });
      if (room.players.size >= 4) return send(ws, { type: "full" });

      const playerId = crypto.randomUUID();
      const player = { id: playerId, ws, x: 0, y: 0, z: 0, yaw: 0, color: 0x20a9d6 };
      const existingPlayers = [...room.players.values()].map(p => ({
        id: p.id, x: p.x, y: p.y, z: p.z, yaw: p.yaw, color: p.color
      }));
      room.players.set(playerId, player);
      sockets.set(ws, { code, playerId });
      send(ws, { type: "joined", code, playerId, count: roomCount(room), players: existingPlayers });
      broadcast(room, {
        type: "player-joined",
        player: { id: playerId, x: 0, y: 0, z: 0, yaw: 0, color: player.color },
        count: roomCount(room)
      }, ws);
      return;
    }

    if (!info) {
      return send(ws, { type: "error", message: "Önce bir oda oluştur veya odaya katıl." });
    }

    const room = rooms.get(info.code);
    if (!room) {
      sockets.delete(ws);
      return send(ws, { type: "room-closed" });
    }

    if (msg.type === "state") {
      const player = room.players.get(info.playerId);
      if (!player) return;
      const x = Number(msg.x), y = Number(msg.y), z = Number(msg.z), yaw = Number(msg.yaw);
      if (![x, y, z, yaw].every(Number.isFinite)) return;
      player.x = Math.max(-100000, Math.min(100000, x));
      player.y = Math.max(-1000, Math.min(1000, y));
      player.z = Math.max(-100000, Math.min(100000, z));
      player.yaw = yaw;
      if (Number.isFinite(Number(msg.color))) player.color = Number(msg.color);
      broadcast(room, {
        type: "state", playerId: info.playerId,
        x: player.x, y: player.y, z: player.z, yaw: player.yaw, color: player.color
      }, ws);
    } else if (msg.type === "leave") {
      removePlayer(ws);
    }
  });

  ws.on("close", () => removePlayer(ws));
  ws.on("error", () => removePlayer(ws));
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Simulation server listening on ${PORT}`);
});
