const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const Database = require('better-sqlite3');
const cors = require('cors');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type']
}));
app.options('*', cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

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

let currentTripId = null;
let lastPoint = null;
let connectedClients = new Set();

function haversine(lat1, lng1, lat2, lng2) {
  const R = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a = Math.sin(dLat/2)**2 + Math.cos(lat1*Math.PI/180)*Math.cos(lat2*Math.PI/180)*Math.sin(dLng/2)**2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}

function broadcast(data) {
  const msg = JSON.stringify(data);
  connectedClients.forEach(ws => { if (ws.readyState === WebSocket.OPEN) ws.send(msg); });
}

function processGPS({ lat, lng, speed_kmh, heading, altitude }, res) {
  const now = new Date().toISOString();
  if (!currentTripId) {
    const result = db.prepare('INSERT INTO trips (started_at) VALUES (?)').run(now);
    currentTripId = result.lastInsertRowid;
    broadcast({ type: 'trip_started', trip_id: currentTripId, started_at: now });
  }
  let distanceDelta = 0;
  if (lastPoint) distanceDelta = haversine(lastPoint.lat, lastPoint.lng, lat, lng);
  db.prepare('INSERT INTO points (trip_id, lat, lng, speed_kmh, heading, altitude, recorded_at) VALUES (?,?,?,?,?,?,?)').run(currentTripId, lat, lng, speed_kmh, heading, altitude, now);
  const trip = db.prepare('SELECT * FROM trips WHERE id = ?').get(currentTripId);
  const newDistance = (trip.distance_km || 0) + distanceDelta;
  const durationSec = Math.floor((new Date() - new Date(trip.started_at)) / 1000);
  const avgSpeed = durationSec > 0 ? (newDistance / durationSec * 3600) : 0;
  const maxSpeed = Math.max(trip.max_speed_kmh || 0, speed_kmh);
  db.prepare('UPDATE trips SET distance_km=?, duration_sec=?, avg_speed_kmh=?, max_speed_kmh=? WHERE id=?').run(newDistance, durationSec, avgSpeed, maxSpeed, currentTripId);
  lastPoint = { lat, lng, speed_kmh, heading, altitude, recorded_at: now };
  broadcast({ type: 'position', lat, lng, speed_kmh, heading, altitude, recorded_at: now, trip_id: currentTripId });
  broadcast({ type: 'trip_update', trip: { ...trip, distance_km: newDistance, duration_sec: durationSec, avg_speed_kmh: avgSpeed, max_speed_kmh: maxSpeed } });
  console.log(`GPS OK: ${lat}, ${lng}, ${speed_kmh} km/h`);
  if (res) res.status(200).send('OK');
}

wss.on('connection', (ws) => {
  connectedClients.add(ws);
  if (lastPoint) ws.send(JSON.stringify({ type: 'position', ...lastPoint }));
  if (currentTripId) {
    const trip = db.prepare('SELECT * FROM trips WHERE id = ?').get(currentTripId);
    ws.send(JSON.stringify({ type: 'trip_update', trip }));
  }
  ws.on('close', () => connectedClients.delete(ws));
});

function handleTraccar(req, res) {
  let lat, lng, speed_kmh, heading, altitude;

  // Traccar Client sūta { location: { coords: { latitude, longitude, speed, heading, altitude } } }
  if (req.body && req.body.location && req.body.location.coords) {
    const coords = req.body.location.coords;
    lat = parseFloat(coords.latitude);
    lng = parseFloat(coords.longitude);
    speed_kmh = Math.max(0, parseFloat(coords.speed || 0)) * 3.6; // m/s uz km/h
    heading = Math.max(0, parseFloat(coords.heading || 0));
    altitude = parseFloat(coords.altitude || 0);
  } else {
    // Vienkāršs query string formāts
    const q = Object.assign({}, req.query, req.body);
    lat = parseFloat(q.lat);
    lng = parseFloat(q.lon || q.lng);
    speed_kmh = parseFloat(q.speed || 0);
    heading = parseFloat(q.bearing || q.heading || 0);
    altitude = parseFloat(q.altitude || 0);
  }

  if (isNaN(lat) || isNaN(lng)) {
    console.log('Missing lat/lon, body:', JSON.stringify(req.body));
    return res.status(400).send('Missing lat/lon');
  }

  processGPS({ lat, lng, speed_kmh, heading, altitude }, res);
}

app.get('/', handleTraccar);
app.post('/', handleTraccar);

app.post('/api/gps', (req, res) => {
  const { lat, lng, speed_kmh = 0, heading = 0, altitude = 0 } = req.body;
  if (!lat || !lng) return res.status(400).json({ error: 'lat and lng required' });
  processGPS({ lat, lng, speed_kmh, heading, altitude }, res);
});

app.post('/api/trip/end', (req, res) => {
  if (!currentTripId) return res.status(400).json({ error: 'No active trip' });
  const now = new Date().toISOString();
  db.prepare('UPDATE trips SET ended_at=? WHERE id=?').run(now, currentTripId);
  broadcast({ type: 'trip_ended', trip_id: currentTripId, ended_at: now });
  currentTripId = null;
  lastPoint = null;
  res.json({ ok: true });
});

app.get('/api/trips', (req, res) => {
  res.json(db.prepare('SELECT * FROM trips ORDER BY started_at DESC LIMIT 100').all());
});

// Delete a trip and all its points
app.delete('/api/trips/:id', (req, res) => {
  const id = parseInt(req.params.id);
  if (!id) return res.status(400).json({ error: 'Invalid id' });
  db.prepare('DELETE FROM points WHERE trip_id = ?').run(id);
  db.prepare('DELETE FROM trips WHERE id = ?').run(id);
  console.log('Deleted trip:', id);
  res.json({ ok: true });
});

app.get('/api/trips/:id/points', (req, res) => {
  res.json(db.prepare('SELECT * FROM points WHERE trip_id=? ORDER BY recorded_at ASC').all(req.params.id));
});

app.get('/api/stats', (req, res) => {
  res.json(db.prepare('SELECT COUNT(*) as total_trips, COALESCE(SUM(distance_km),0) as total_km, COALESCE(SUM(duration_sec),0) as total_sec, COALESCE(MAX(max_speed_kmh),0) as all_time_max_speed FROM trips WHERE ended_at IS NOT NULL').get());
});

app.get('/api/health', (req, res) => res.json({ ok: true, active_trip: currentTripId }));

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => console.log(`Running on port ${PORT}`));
