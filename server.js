const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { Pool } = require('pg');
const cors = require('cors');

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

// --- PostgreSQL ---
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// Create tables if not exists
async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS trips (
      id SERIAL PRIMARY KEY,
      started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      ended_at TIMESTAMPTZ,
      distance_km REAL DEFAULT 0,
      duration_sec INTEGER DEFAULT 0,
      avg_speed_kmh REAL DEFAULT 0,
      max_speed_kmh REAL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS points (
      id SERIAL PRIMARY KEY,
      trip_id INTEGER REFERENCES trips(id) ON DELETE CASCADE,
      lat REAL NOT NULL,
      lng REAL NOT NULL,
      speed_kmh REAL DEFAULT 0,
      heading REAL DEFAULT 0,
      altitude REAL DEFAULT 0,
      recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  console.log('DB tables ready');
}

// --- State ---
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

async function processGPS({ lat, lng, speed_kmh, heading, altitude }, res) {
  const now = new Date().toISOString();
  try {
    // Auto-start trip if none active
    if (!currentTripId) {
      const result = await pool.query(
        'INSERT INTO trips (started_at) VALUES ($1) RETURNING id', [now]
      );
      currentTripId = result.rows[0].id;
      broadcast({ type: 'trip_started', trip_id: currentTripId, started_at: now });
      console.log('New trip started:', currentTripId);
    }

    // Distance from last point
    let distanceDelta = 0;
    if (lastPoint) distanceDelta = haversine(lastPoint.lat, lastPoint.lng, lat, lng);

    // Insert point
    await pool.query(
      'INSERT INTO points (trip_id, lat, lng, speed_kmh, heading, altitude, recorded_at) VALUES ($1,$2,$3,$4,$5,$6,$7)',
      [currentTripId, lat, lng, speed_kmh, heading, altitude, now]
    );

    // Update trip stats
    const tripRes = await pool.query('SELECT * FROM trips WHERE id = $1', [currentTripId]);
    const trip = tripRes.rows[0];
    const newDistance = (parseFloat(trip.distance_km) || 0) + distanceDelta;
    const durationSec = Math.floor((new Date() - new Date(trip.started_at)) / 1000);
    const avgSpeed = durationSec > 0 ? (newDistance / durationSec * 3600) : 0;
    const maxSpeed = Math.max(parseFloat(trip.max_speed_kmh) || 0, speed_kmh);

    await pool.query(
      'UPDATE trips SET distance_km=$1, duration_sec=$2, avg_speed_kmh=$3, max_speed_kmh=$4 WHERE id=$5',
      [newDistance, durationSec, avgSpeed, maxSpeed, currentTripId]
    );

    lastPoint = { lat, lng, speed_kmh, heading, altitude, recorded_at: now };

    broadcast({ type: 'position', lat, lng, speed_kmh, heading, altitude, recorded_at: now, trip_id: currentTripId });
    broadcast({ type: 'trip_update', trip: { ...trip, distance_km: newDistance, duration_sec: durationSec, avg_speed_kmh: avgSpeed, max_speed_kmh: maxSpeed } });

    console.log(`GPS: ${lat}, ${lng}, ${speed_kmh} km/h`);
    if (res) res.status(200).send('OK');
  } catch(e) {
    console.error('processGPS error:', e.message);
    if (res) res.status(500).send('Error');
  }
}

// --- WebSocket ---
wss.on('connection', async (ws) => {
  connectedClients.add(ws);
  console.log('Client connected, total:', connectedClients.size);
  if (lastPoint) ws.send(JSON.stringify({ type: 'position', ...lastPoint }));
  if (currentTripId) {
    const tripRes = await pool.query('SELECT * FROM trips WHERE id = $1', [currentTripId]);
    if (tripRes.rows[0]) ws.send(JSON.stringify({ type: 'trip_update', trip: tripRes.rows[0] }));
  }
  ws.on('close', () => connectedClients.delete(ws));
});

// --- Traccar / GPS endpoints ---
function handleTraccar(req, res) {
  let lat, lng, speed_kmh, heading, altitude;
  if (req.body && req.body.location && req.body.location.coords) {
    const coords = req.body.location.coords;
    lat = parseFloat(coords.latitude);
    lng = parseFloat(coords.longitude);
    speed_kmh = Math.max(0, parseFloat(coords.speed || 0)) * 3.6;
    heading = Math.max(0, parseFloat(coords.heading || 0));
    altitude = parseFloat(coords.altitude || 0);
  } else {
    const q = Object.assign({}, req.query, req.body);
    lat = parseFloat(q.lat);
    lng = parseFloat(q.lon || q.lng);
    speed_kmh = parseFloat(q.speed || 0);
    heading = parseFloat(q.bearing || q.heading || 0);
    altitude = parseFloat(q.altitude || 0);
  }
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

// End current trip
app.post('/api/trip/end', async (req, res) => {
  if (!currentTripId) return res.status(400).json({ error: 'No active trip' });
  const now = new Date().toISOString();
  await pool.query('UPDATE trips SET ended_at=$1 WHERE id=$2', [now, currentTripId]);
  broadcast({ type: 'trip_ended', trip_id: currentTripId, ended_at: now });
  currentTripId = null;
  lastPoint = null;
  res.json({ ok: true });
});

// Get all trips
app.get('/api/trips', async (req, res) => {
  const result = await pool.query('SELECT * FROM trips ORDER BY started_at DESC LIMIT 100');
  res.json(result.rows);
});

// Get points for a trip
app.get('/api/trips/:id/points', async (req, res) => {
  const result = await pool.query(
    'SELECT * FROM points WHERE trip_id=$1 ORDER BY recorded_at ASC', [req.params.id]
  );
  res.json(result.rows);
});

// Delete a trip and all its points
app.delete('/api/trips/:id', async (req, res) => {
  const id = parseInt(req.params.id);
  if (!id) return res.status(400).json({ error: 'Invalid id' });
  await pool.query('DELETE FROM points WHERE trip_id=$1', [id]);
  await pool.query('DELETE FROM trips WHERE id=$1', [id]);
  console.log('Deleted trip:', id);
  res.json({ ok: true });
});

// Overall statistics
app.get('/api/stats', async (req, res) => {
  const result = await pool.query(`
    SELECT
      COUNT(*) as total_trips,
      COALESCE(SUM(distance_km),0) as total_km,
      COALESCE(SUM(duration_sec),0) as total_sec,
      COALESCE(MAX(max_speed_kmh),0) as all_time_max_speed
    FROM trips WHERE ended_at IS NOT NULL
  `);
  res.json(result.rows[0]);
});

// Health check
app.get('/api/health', (req, res) => res.json({ ok: true, active_trip: currentTripId }));

// --- Auto end trip at midnight (server-side) ---
async function autoEndTripAtMidnight() {
  const now = new Date();
  // Calculate ms until next midnight in Riga timezone (UTC+3)
  const rigaOffset = 3 * 60 * 60 * 1000;
  const rigaNow = new Date(now.getTime() + rigaOffset);
  const rigaMidnight = new Date(rigaNow);
  rigaMidnight.setUTCHours(24, 0, 0, 0);
  const msUntilMidnight = rigaMidnight.getTime() - rigaNow.getTime();

  console.log(`Auto-end trip scheduled in ${Math.round(msUntilMidnight/1000/60)} minutes (Riga midnight)`);

  setTimeout(async () => {
    if (currentTripId) {
      const now = new Date().toISOString();
      try {
        await pool.query('UPDATE trips SET ended_at=$1 WHERE id=$2', [now, currentTripId]);
        broadcast({ type: 'trip_ended', trip_id: currentTripId, ended_at: now });
        console.log('Auto-ended trip at midnight:', currentTripId);
        currentTripId = null;
        lastPoint = null;
      } catch(e) {
        console.error('Auto-end error:', e.message);
      }
    } else {
      console.log('Midnight: no active trip to end');
    }
    // Schedule next midnight
    autoEndTripAtMidnight();
  }, msUntilMidnight);
}

// --- Restore active trip on server restart ---
async function restoreActiveTrip() {
  try {
    const result = await pool.query(
      'SELECT * FROM trips WHERE ended_at IS NULL ORDER BY started_at DESC LIMIT 1'
    );
    if (result.rows.length > 0) {
      currentTripId = result.rows[0].id;
      console.log('Restored active trip:', currentTripId);

      // Restore last point
      const ptResult = await pool.query(
        'SELECT * FROM points WHERE trip_id=$1 ORDER BY recorded_at DESC LIMIT 1',
        [currentTripId]
      );
      if (ptResult.rows.length > 0) {
        const p = ptResult.rows[0];
        lastPoint = { lat: p.lat, lng: p.lng, speed_kmh: p.speed_kmh, heading: p.heading, altitude: p.altitude, recorded_at: p.recorded_at };
        console.log('Restored last point:', lastPoint.lat, lastPoint.lng);
      }
    } else {
      console.log('No active trip to restore');
    }
  } catch(e) {
    console.error('Restore error:', e.message);
  }
}

// Start server
const PORT = process.env.PORT || 3001;
initDB().then(async () => {
  await restoreActiveTrip();
  autoEndTripAtMidnight();
  server.listen(PORT, () => console.log(`GPS Tracker running on port ${PORT}`));
}).catch(err => {
  console.error('DB init failed:', err.message);
  process.exit(1);
});
