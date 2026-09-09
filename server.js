const http = require("http");
const WebSocket = require("ws");
const { Pool } = require("pg");

const GRID_SIZE = 200;
const COOLDOWN_MS = 10 * 60 * 1000; // 10 dakika - client'taki süreyle eşleşmeli
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
  res.end("PixelTur backend calisiyor");
});

const wss = new WebSocket.Server({ server });

// IP başına son piksel basma zamanı (server tarafı cooldown - client tarafı
// atlatılamaz hale getirmek için). Basit bir bellek içi harita; sunucu
// yeniden başlarsa sıfırlanır, bu da bu ölçekte sorun değil.
const lastPlacedAt = new Map();

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

  try {
    const board = await loadBoard();
    ws.send(JSON.stringify({ type: "init", board }));
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

    const payload = JSON.stringify({ type: "update", x, y, color });
    wss.clients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(payload);
      }
    });
  });
});

const PORT = process.env.PORT || 3000;

ensureSchema()
  .then(() => {
    server.listen(PORT, () => {
      console.log(`PixelTur backend ${PORT} portunda calisiyor`);
    });
  })
  .catch((err) => {
    console.error("Veritabani semasi hazirlanirken hata:", err);
    process.exit(1);
  });
