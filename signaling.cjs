const http = require("node:http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT ? Number(process.env.PORT) : 8787;
const MAX_HISTORY = 200;
const MAX_TEXT_LEN = 2000;
const MAX_IMAGE_DATA_LEN = 600 * 1024;

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("GameHub signaling server up\n");
});

const wss = new WebSocketServer({ server });

/** @type {Map<string, { ws: any, room: string|null, user: string }>} */
const clients = new Map();

/** @type {Map<string, Array<any>>} */
const roomHistory = new Map();

let nextId = 1;

function safeSend(ws, obj) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}

function peersInRoom(room, exceptId) {
  const list = [];
  for (const [id, c] of clients) {
    if (c.room === room && id !== exceptId) list.push({ id, user: c.user });
  }
  return list;
}

function presenceSnapshot() {
  const presence = {};
  for (const [id, c] of clients) {
    if (!c.room) continue;
    if (!presence[c.room]) presence[c.room] = [];
    presence[c.room].push({ id, user: c.user });
  }
  return presence;
}

function roomsSummary() {
  const presence = presenceSnapshot();
  return Object.entries(presence).map(([name, users]) => ({
    name,
    count: users.length
  }));
}

function broadcastPresence() {
  const presence = presenceSnapshot();
  for (const c of clients.values()) {
    safeSend(c.ws, { type: "presence", presence });
  }
}

function notifyRoom(room, exceptId, payload) {
  if (!room) return;
  for (const [id, c] of clients) {
    if (id !== exceptId && c.room === room) safeSend(c.ws, payload);
  }
}

function getHistory(room) {
  return roomHistory.get(room) || [];
}

function pushHistory(room, msg) {
  if (!roomHistory.has(room)) roomHistory.set(room, []);
  const list = roomHistory.get(room);
  list.push(msg);
  if (list.length > MAX_HISTORY) list.shift();
}

function leaveRoom(id, broadcast = true) {
  const me = clients.get(id);
  if (!me || !me.room) return null;
  const prevRoom = me.room;

  notifyRoom(prevRoom, id, { type: "share-stopped", from: id });
  me.room = null;
  notifyRoom(prevRoom, id, { type: "peer-left", id });

  if (broadcast) broadcastPresence();
  return prevRoom;
}

wss.on("connection", (ws) => {
  const id = String(nextId++);

  clients.set(id, { ws, room: null, user: `User-${id}` });

  safeSend(ws, {
    type: "welcome",
    id,
    rooms: roomsSummary(),
    presence: presenceSnapshot()
  });

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); }
    catch { return safeSend(ws, { type: "error", message: "JSON inválido" }); }

    const me = clients.get(id);
    if (!me) return;

    if (msg.type === "watch") return;

    if (msg.type === "list-rooms") {
      return safeSend(ws, { type: "rooms", rooms: roomsSummary() });
    }

    if (msg.type === "join") {
      const room = String(msg.room || "").trim();
      const user = String(msg.user || "").trim() || `User-${id}`;
      if (!room) return safeSend(ws, { type: "error", message: "room obrigatório" });
      if (room.length > 40) return safeSend(ws, { type: "error", message: "Nome da sala muito longo." });

      if (me.room && me.room !== room) leaveRoom(id, false);

      me.room = room;
      me.user = user;

      safeSend(ws, {
        type: "welcome",
        id,
        room,
        peers: peersInRoom(room, id),
        rooms: roomsSummary(),
        presence: presenceSnapshot()
      });

      safeSend(ws, {
        type: "chat-history",
        room,
        messages: getHistory(room)
      });

      notifyRoom(room, id, { type: "peer-joined", id, user });
      console.log(`[+] ${user} (${id}) entrou em "${room}"`);
      broadcastPresence();
      return;
    }

    if (msg.type === "leave") {
      leaveRoom(id, true);
      return;
    }

    if (msg.type === "signal") {
      const target = clients.get(String(msg.to));
      if (!target) return;
      safeSend(target.ws, { type: "signal", from: id, payload: msg.payload });
      return;
    }

    if (msg.type === "chat") {
      const room = me.room;
      if (!room) return;
      const text = String(msg.text || "").slice(0, MAX_TEXT_LEN).trim();

      let image = undefined;
      if (msg.image && typeof msg.image === "object") {
        const data = String(msg.image.data || "");
        const width = Number(msg.image.width) || 0;
        const height = Number(msg.image.height) || 0;
        if (data.startsWith("data:image/") && data.length <= MAX_IMAGE_DATA_LEN) {
          image = { data, width, height };
        } else if (data.length > MAX_IMAGE_DATA_LEN) {
          return safeSend(ws, { type: "error", message: "Imagem grande demais." });
        }
      }

      if (!text && !image) return;

      const entry = {
        id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
        userId: id,
        user: me.user,
        text,
        at: Date.now(),
        ...(image ? { image } : {}),
      };
      pushHistory(room, entry);

      for (const [, c] of clients) {
        if (c.room === room) safeSend(c.ws, { type: "chat", message: entry });
      }
      return;
    }

    if (msg.type === "share-started") {
      notifyRoom(me.room, id, {
        type: "share-started",
        from: id,
        user: me.user,
        quality: msg.quality || "720p30",
        hasAudio: !!msg.hasAudio,
        sourceKind: msg.sourceKind === "window" ? "window" : "screen"
      });
      return;
    }

    if (msg.type === "share-stopped") {
      notifyRoom(me.room, id, { type: "share-stopped", from: id });
      return;
    }
  });

  ws.on("close", () => {
    const me = clients.get(id);
    if (me) leaveRoom(id, true);
    clients.delete(id);
    broadcastPresence();
  });

  ws.on("error", () => { try { ws.close(); } catch {} });
});

server.listen(PORT, () => {
  console.log(`GameHub signaling rodando na porta ${PORT}`);
});