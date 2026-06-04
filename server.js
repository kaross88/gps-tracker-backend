const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const Database = require('better-sqlite3');
const cors = require('cors');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json());

// --- Database setup ---
const db = new Database(path.join(__dirname, 'gps_data.db'));

db.exec(`
  CREATE TABLE IF NOT EXISTS trips (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    distance_km REAL DEFAULT 0,
    duration_sec INTEGER DEFAULT 0,
    avg_speed_kmh REAL DEFAULT 0,
    max_speed_kmh REAL DEFAULT 0
  );

  CREATE TABLE IF NOT EXISTS points (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trip_id INTEGER REFERENCES trips(id),
    lat REAL NOT NULL,
    lng REAL NOT NULL,
    speed_kmh REAL DEFAULT 0,
    heading REAL DEFAULT 0,
    altitude REAL DEFAULT 0,
    recorded_at TEXT NOT NULL
  );
`);

// --- State ---
let currentTripId = null;
let lastPoint = null;
let connectedClients = new Set();

// Haversine distance in km
function haversine(lat1, lng1, lat2, lng2) {
  const R = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a = Math.sin(dLat/2)**2 +
    Math.cos(lat1 * Math.PI/180) * Math.cos(lat2 * Math.PI/180) *
    Math.sin(dLng/2)**2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}

function broadcast(data) {
  const msg = JSON.stringify(data);
  connectedClients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) ws.send(msg);
  });
}

// --- WebSocket ---
wss.on('connection', (ws) => {
  connectedClients.add(ws);
  console.log('Client connected, total:', connectedClients.size);

  // Send last known point on connect
  if (lastPoint) {
    ws.send(JSON.stringify({ type: 'position', ...lastPoint }));
  }

  // Send current trip info
  if (currentTripId) {
    const trip = db.prepare('SELECT * FROM trips WHERE id = ?').get(currentTripId);
    ws.send(JSON.stringify({ type: 'trip_update', trip }));
  }

  ws.on('close', () => {
    connectedClients.delete(ws);
  });
});

// --- GPS data endpoint (trackeris sūta šeit) ---
// POST /api/gps
// Body: { lat, lng, speed_kmh, heading, altitude, device_id }
app.post('/api/gps', (req, res) => {
  const { lat, lng, speed_kmh = 0, heading = 0, altitude = 0 } = req.body;

  if (lat === undefined || lng === undefined) {
    return res.status(400).json({ error: 'lat and lng are required' });
  }

  const now = new Date().toISOString();

  // Auto-start trip if none active
  if (!currentTripId) {
    const result = db.prepare(
      'INSERT INTO trips (started_at) VALUES (?)'
    ).run(now);
    currentTripId = result.lastInsertRowid;
    console.log('New trip started:', currentTripId);
    broadcast({ type: 'trip_started', trip_id: currentTripId, started_at: now });
  }

  // Calculate distance from last point
  let distanceDelta = 0;
  if (lastPoint) {
    distanceDelta = haversine(lastPoint.lat, lastPoint.lng, lat, lng);
  }

  // Insert point
  db.prepare(
    'INSERT INTO points (trip_id, lat, lng, speed_kmh, heading, altitude, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?)'
  ).run(currentTripId, lat, lng, speed_kmh, heading, altitude, now);

  // Update trip stats
  const trip = db.prepare('SELECT * FROM trips WHERE id = ?').get(currentTripId);
  const newDistance = (trip.distance_km || 0) + distanceDelta;
  const startedAt = new Date(trip.started_at);
  const durationSec = Math.floor((new Date() - startedAt) / 1000);
  const avgSpeed = durationSec > 0 ? (newDistance / durationSec * 3600) : 0;
  const maxSpeed = Math.max(trip.max_speed_kmh || 0, speed_kmh);

  db.prepare(
    'UPDATE trips SET distance_km = ?, duration_sec = ?, avg_speed_kmh = ?, max_speed_kmh = ? WHERE id = ?'
  ).run(newDistance, durationSec, avgSpeed, maxSpeed, currentTripId);

  lastPoint = { lat, lng, speed_kmh, heading, altitude, recorded_at: now };

  // Broadcast to all WebSocket clients
  broadcast({
    type: 'position',
    lat, lng, speed_kmh, heading, altitude,
    recorded_at: now,
    trip_id: currentTripId
  });

  broadcast({
    type: 'trip_update',
    trip: { ...trip, distance_km: newDistance, duration_sec: durationSec, avg_speed_kmh: avgSpeed, max_speed_kmh: maxSpeed }
  });

  res.json({ ok: true, trip_id: currentTripId, distance_delta: distanceDelta });
});

// End current trip
app.post('/api/trip/end', (req, res) => {
  if (!currentTripId) return res.status(400).json({ error: 'No active trip' });

  const now = new Date().toISOString();
  db.prepare('UPDATE trips SET ended_at = ? WHERE id = ?').run(now, currentTripId);

  broadcast({ type: 'trip_ended', trip_id: currentTripId, ended_at: now });
  currentTripId = null;
  lastPoint = null;

  res.json({ ok: true });
});

// Get all trips with stats
app.get('/api/trips', (req, res) => {
  const trips = db.prepare('SELECT * FROM trips ORDER BY started_at DESC LIMIT 100').all();
  res.json(trips);
});

// Get points for a trip
app.get('/api/trips/:id/points', (req, res) => {
  const points = db.prepare('SELECT * FROM points WHERE trip_id = ? ORDER BY recorded_at ASC').all(req.params.id);
  res.json(points);
});

// Overall statistics
app.get('/api/stats', (req, res) => {
  const stats = db.prepare(`
    SELECT
      COUNT(*) as total_trips,
      COALESCE(SUM(distance_km), 0) as total_km,
      COALESCE(SUM(duration_sec), 0) as total_sec,
      COALESCE(AVG(avg_speed_kmh), 0) as overall_avg_speed,
      COALESCE(MAX(max_speed_kmh), 0) as all_time_max_speed
    FROM trips WHERE ended_at IS NOT NULL
  `).get();
  res.json(stats);
});

// Health check
app.get('/api/health', (req, res) => res.json({ ok: true, active_trip: currentTripId }));

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`GPS Tracker backend running on http://localhost:${PORT}`);
  console.log(`WebSocket available on ws://localhost:${PORT}`);
  console.log(`Send GPS data to POST http://localhost:${PORT}/api/gps`);
});
