const http = require("http");
const WebSocket = require("ws");
const { Pool } = require("pg");

const GRID_SIZE = 200;
const COOLDOWN_MS = 10 * 1000; // 10 saniye - client'taki süreyle eşleşmeli
const CHAT_COOLDOWN_MS = 2 * 1000; // spam'i onlemek icin kisa bir sohbet bekleme suresi
const CHAT_HISTORY_LIMIT = 50;
const HEX_COLOR_REGEX = /^#[0-9A-Fa-f]{6}$/;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// Basit bir HTTP sunucu: hem WebSocket'i taşımak hem de
// dış bir servisin (cron-job.org, UptimeRobot vb.) sunucuyu
// uyanık tutmak için "ping" atabileceği bir healthcheck sağlamak için.
const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("ok");
    return;
  }
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("PlaceTurk backend calisiyor");
});

const wss = new WebSocket.Server({ server });

// IP başına son piksel basma zamanı (server tarafı cooldown - client tarafı
// atlatılamaz hale getirmek için). Basit bir bellek içi harita; sunucu
// yeniden başlarsa sıfırlanır, bu da bu ölçekte sorun değil.
const lastPlacedAt = new Map();

// Sohbet: sadece bellekte tutuluyor, kalici degil (sunucu yeniden
// baslarsa - orn. uykudan uyanirken - sifirlanir). Bu olcekte bir
// veritabani tablosuna gerek yok, basit ve yeterli.
let chatHistory = [];
const lastChatAt = new Map();
let nextChatId = 1;
// IP basina "ayni mesaji ust uste kac kez yazdi" takibi (spam tespiti icin)
const repeatTracker = new Map(); // ip -> { text, count, ids:[] }

// Basit kufur/argo/+18 kelime filtresi. Kelime sinirlarina gore kontrol
// ediyoruz (orn. "sik" gecen "sikayet" gibi masum kelimeleri yanlislikla
// yakalamamak icin). Listeyi ihtiyaca gore genisletebilirsin.
const BANNED_WORDS = [
  "amk", "aq", "amq", "oç", "oc", "orospu", "piç", "pic",
  "yarrak", "yarak", "sikeyim", "siktir", "sikim", "götveren",
  "gotveren", "ibne", "kahpe", "şerefsiz", "serefsiz", "pezevenk",
  "amcık", "amcik", "göt", "got", "yavşak", "yavsak", "sürtük", "surtuk"
];
const BANNED_WORDS_REGEX = new RegExp(
  "(^|[^a-zçğıöşü0-9])(" + BANNED_WORDS.join("|") + ")([^a-zçğıöşü0-9]|$)",
  "i"
);
function containsBannedWord(text) {
  return BANNED_WORDS_REGEX.test(text.toLocaleLowerCase("tr"));
}

function broadcast(obj) {
  const payload = JSON.stringify(obj);
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) client.send(payload);
  });
}

// Sohbet gecmisini 24 saatte bir otomatik temizle (istek uzerine).
const CHAT_HISTORY_MAX_AGE_MS = 24 * 60 * 60 * 1000;
setInterval(() => {
  chatHistory = [];
  repeatTracker.clear();
  broadcast({ type: "chat_clear" });
  console.log("Sohbet gecmisi 24 saatlik periyotla temizlendi.");
}, CHAT_HISTORY_MAX_AGE_MS);

async function ensureSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS pixels (
      x INT NOT NULL,
      y INT NOT NULL,
      color TEXT NOT NULL,
      PRIMARY KEY (x, y)
    );
  `);
}

async function loadBoard() {
  const res = await pool.query("SELECT x,y,color FROM pixels");
  return res.rows;
}

function getClientIp(req) {
  // Render/Railway gibi platformlarda proxy arkasında olduğumuz için
  // öncelikle x-forwarded-for başlığına bakıyoruz.
  const forwarded = req.headers["x-forwarded-for"];
  if (forwarded) return forwarded.split(",")[0].trim();
  return req.socket.remoteAddress;
}

wss.on("connection", async (ws, req) => {
  ws.clientIp = getClientIp(req);
  broadcast({ type: "online", count: wss.clients.size });

  ws.on("close", () => {
    // biraz gecikmeyle yayinlayalim ki ws zaten clients setinden cikmis olsun
    setTimeout(() => broadcast({ type: "online", count: wss.clients.size }), 0);
  });

  try {
    const board = await loadBoard();
    ws.send(JSON.stringify({ type: "init", board, chat: chatHistory, online: wss.clients.size }));
  } catch (err) {
    console.error("Tahta yuklenirken hata:", err);
  }

  ws.on("message", async (msg) => {
    let data;
    try {
      data = JSON.parse(msg);
    } catch {
      return; // gecersiz JSON, sessizce yoksay
    }

    if (data.type === "chat") {
      let { name, text } = data;
      if (typeof text !== "string") return;
      text = text.trim().slice(0, 200);
      if (!text) return;
      name = (typeof name === "string" ? name.trim() : "").slice(0, 20) || "Misafir";

      // --- Kufur/argo/+18 filtresi: bu kelimeler geciyorsa mesaj hic
      // yayinlanmiyor, gonderene de bir seffaflik icin bilgi gonderiliyor ---
      if (containsBannedWord(text) || containsBannedWord(name)) {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({
            type: "chat_blocked",
            reason: "Mesajın uygunsuz içerik nedeniyle gönderilemedi."
          }));
        }
        return;
      }

      // --- Sunucu tarafi sohbet cooldown'i (spam onleme) ---
      const now = Date.now();
      const lastChat = lastChatAt.get(ws.clientIp) || 0;
      if (now - lastChat < CHAT_COOLDOWN_MS) {
        return;
      }
      lastChatAt.set(ws.clientIp, now);

      // --- Ayni mesaji ust uste yazan kullanicilarin spam'ini temizle ---
      let tracker = repeatTracker.get(ws.clientIp);
      if (!tracker || tracker.text !== text) {
        tracker = { text, count: 0, ids: [] };
        repeatTracker.set(ws.clientIp, tracker);
      }
      tracker.count++;

      if (tracker.count >= 3) {
        // 3. tekrarda: onceki ayni mesajlari herkesin ekranindan da sil,
        // bu mesaji da hic eklemeden yoksay.
        tracker.ids.forEach((id) => {
          chatHistory = chatHistory.filter((m) => m.id !== id);
          broadcast({ type: "chat_delete", id });
        });
        tracker.ids = [];
        return;
      }

      const id = nextChatId++;
      const entry = { id, name, text, ts: now };
      chatHistory.push(entry);
      if (chatHistory.length > CHAT_HISTORY_LIMIT) chatHistory.shift();
      tracker.ids.push(id);

      broadcast({ type: "chat", id, name, text, ts: now });
      return;
    }

    if (data.type !== "place") return;

    const { x, y, color } = data;

    // --- Sunucu tarafi dogrulama ---
    if (
      !Number.isInteger(x) || !Number.isInteger(y) ||
      x < 0 || x >= GRID_SIZE || y < 0 || y >= GRID_SIZE
    ) {
      return; // sinir disi koordinat
    }
    if (typeof color !== "string" || !HEX_COLOR_REGEX.test(color)) {
      return; // gecersiz renk
    }

    // --- Sunucu tarafi cooldown ---
    const now = Date.now();
    const last = lastPlacedAt.get(ws.clientIp) || 0;
    if (now - last < COOLDOWN_MS) {
      return; // cooldown dolmamis, istegi yoksay
    }
    lastPlacedAt.set(ws.clientIp, now);

    try {
      await pool.query(
        "INSERT INTO pixels (x,y,color) VALUES ($1,$2,$3) ON CONFLICT (x,y) DO UPDATE SET color=$3",
        [x, y, color]
      );
    } catch (err) {
      console.error("Piksel yazilirken hata:", err);
      return;
    }

    broadcast({ type: "update", x, y, color });
  });
});

const PORT = process.env.PORT || 3000;

ensureSchema()
  .then(() => {
    server.listen(PORT, () => {
      console.log(`PlaceTurk backend ${PORT} portunda calisiyor`);
    });
  })
  .catch((err) => {
    console.error("Veritabani semasi hazirlanirken hata:", err);
    process.exit(1);
  });
