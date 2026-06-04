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
 
// Traccar sūta POST / ar datiem body vai query
function handleTraccar(req, res) {
  const q = Object.assign({}, req.query, req.body);
  const lat = parseFloat(q.lat);
  const lng = parseFloat(q.lon || q.lng);
  const speed_kmh = parseFloat(q.speed || 0);
  const heading = parseFloat(q.bearing || q.heading || 0);
  const altitude = parseFloat(q.altitude || 0);
  console.log('Traccar data:', q);
  if (isNaN(lat) || isNaN(lng)) return res.status(400).send('Missing lat/lon');
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
 
app.get('/api/trips/:id/points', (req, res) => {
  res.json(db.prepare('SELECT * FROM points WHERE trip_id=? ORDER BY recorded_at ASC').all(req.params.id));
});
 
app.get('/api/stats', (req, res) => {
  res.json(db.prepare('SELECT COUNT(*) as total_trips, COALESCE(SUM(distance_km),0) as total_km, COALESCE(SUM(duration_sec),0) as total_sec, COALESCE(MAX(max_speed_kmh),0) as all_time_max_speed FROM trips WHERE ended_at IS NOT NULL').get());
});
 
app.get('/api/health', (req, res) => res.json({ ok: true, active_trip: currentTripId }));
 
const PORT = process.env.PORT || 3001;
server.listen(PORT, () => console.log(`Running on port ${PORT}`));
 
