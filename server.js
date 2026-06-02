// ============================================================
//  CRYPTOORACLE BACKEND — always-on 24/7 prediction engine
//
//  - Fetches multi-source prices every 60 seconds
//  - Generates 15-min-ahead predictions
//  - Scores them when their target time arrives (even with nobody watching)
//  - Auto-adjusts model weights from realized accuracy
//  - Persists everything to a Postgres database (Neon) so data survives
//    redeploys and accumulates for model training
//  - Exposes a small JSON API the static website reads from
//
//  Requires env var: DATABASE_URL  (Neon Postgres connection string)
//  Falls back to a local file if DATABASE_URL is not set (for local dev).
// ============================================================

import http from 'http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import pg from 'pg';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data.json');
const DATABASE_URL = process.env.DATABASE_URL || '';
const PORT = process.env.PORT || 3000;
const USE_DB = !!DATABASE_URL;

// Postgres connection pool (only used when DATABASE_URL is set)
let pool = null;
if (USE_DB) {
  pool = new pg.Pool({
    connectionString: DATABASE_URL,
    ssl: { rejectUnauthorized: false } // Neon requires SSL
  });
}

const COINS = ['bitcoin', 'ethereum', 'ripple'];
const SOURCE_SYMBOLS = {
  bitcoin:  { binance: 'BTCUSDT', coinbase: 'BTC-USD', kraken: 'XBTUSD' },
  ethereum: { binance: 'ETHUSDT', coinbase: 'ETH-USD', kraken: 'ETHUSD' },
  ripple:   { binance: 'XRPUSDT', coinbase: 'XRP-USD', kraken: 'XRPUSD' }
};

// ---------- Persistent State ----------
let state = { coins: {}, lastPrices: {}, lastStatus: {}, updatedAt: null };

function defaultCoinState() {
  return {
    priceHistory: [],
    weights: { sma: 0.25, rsi: 0.20, macd: 0.20, lin: 0.25, bb: 0.10 },
    scores: { sma:{e:0,n:0}, rsi:{e:0,n:0}, macd:{e:0,n:0}, lin:{e:0,n:0}, bb:{e:0,n:0} },
    pending: [],
    log: []
  };
}
// ---------- Persistence (Postgres via Neon, with file fallback) ----------
// Strategy: keep the whole in-memory `state` as the working copy, and persist
// it as a single JSONB row. This keeps all prediction/scoring/learning logic
// unchanged while making the data durable across redeploys.

async function initDB() {
  if (!USE_DB) return;
  // One table, one row (id=1) holding the entire state as JSONB.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS app_state (
      id INTEGER PRIMARY KEY,
      data JSONB NOT NULL,
      updated_at TIMESTAMPTZ DEFAULT now()
    )
  `);
}

async function loadState() {
  if (USE_DB) {
    try {
      const res = await pool.query('SELECT data FROM app_state WHERE id = 1');
      if (res.rows.length) {
        state = res.rows[0].data;
        console.log('[state] loaded from Postgres');
      } else {
        console.log('[state] no saved row yet — starting fresh');
      }
    } catch (e) {
      console.warn('[state] DB load failed:', e.message);
    }
  } else {
    try {
      if (fs.existsSync(DATA_FILE)) { state = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); console.log('[state] loaded from file'); }
    } catch (e) { console.warn('[state] file load failed:', e.message); }
  }
  if (!state.coins) state.coins = {};
  if (!state.lastPrices) state.lastPrices = {};
  if (!state.lastStatus) state.lastStatus = {};
  COINS.forEach(c => { if (!state.coins[c]) state.coins[c] = defaultCoinState(); });
}

let saveTimer = null;
let saveInFlight = false;
function saveState() {
  if (saveTimer) return;
  saveTimer = setTimeout(async () => {
    saveTimer = null;
    state.updatedAt = Date.now();
    if (USE_DB) {
      if (saveInFlight) return; // avoid overlapping writes
      saveInFlight = true;
      try {
        await pool.query(
          `INSERT INTO app_state (id, data, updated_at) VALUES (1, $1, now())
           ON CONFLICT (id) DO UPDATE SET data = $1, updated_at = now()`,
          [state]
        );
      } catch (e) {
        console.warn('[state] DB save failed:', e.message);
      } finally {
        saveInFlight = false;
      }
    } else {
      try { fs.writeFileSync(DATA_FILE, JSON.stringify(state)); }
      catch (e) { console.warn('[state] file save failed:', e.message); }
    }
  }, 500);
}

// ---------- Helpers ----------
const median = arr => {
  if (!arr.length) return null;
  const s = [...arr].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};
async function timedFetch(url, ms = 8000) {
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), ms);
  try { return await fetch(url, { signal: ctrl.signal }); }
  finally { clearTimeout(id); }
}

// ---------- Source fetchers ----------
async function srcBinance() {
  const r = await timedFetch('https://api.binance.com/api/v3/ticker/price');
  if (!r.ok) throw new Error('binance ' + r.status);
  const data = await r.json();
  const bySym = {}; data.forEach(d => { bySym[d.symbol] = parseFloat(d.price); });
  const out = {}; for (const c of COINS) { const v = bySym[SOURCE_SYMBOLS[c].binance]; if (v) out[c] = v; }
  return out;
}
async function srcCoinbase() {
  const out = {};
  await Promise.all(COINS.map(async c => {
    try {
      const r = await timedFetch(`https://api.coinbase.com/v2/prices/${SOURCE_SYMBOLS[c].coinbase}/spot`);
      if (!r.ok) return;
      const d = await r.json();
      if (d?.data?.amount) out[c] = parseFloat(d.data.amount);
    } catch {}
  }));
  if (!Object.keys(out).length) throw new Error('coinbase no data');
  return out;
}
async function srcKraken() {
  const pairs = COINS.map(c => SOURCE_SYMBOLS[c].kraken).join(',');
  const r = await timedFetch(`https://api.kraken.com/0/public/Ticker?pair=${pairs}`);
  if (!r.ok) throw new Error('kraken ' + r.status);
  const d = await r.json();
  if (!d.result) throw new Error('kraken no result');
  const out = {};
  for (const c of COINS) {
    const want = SOURCE_SYMBOLS[c].kraken;
    const key = Object.keys(d.result).find(k => k === want || (k.includes(want.replace('USD','')) && k.endsWith('USD')));
    if (key && d.result[key]?.c) out[c] = parseFloat(d.result[key].c[0]);
  }
  return out;
}
async function srcCoinGecko() {
  const r = await timedFetch('https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,ripple&vs_currencies=usd');
  if (!r.ok) throw new Error('coingecko ' + r.status);
  const d = await r.json();
  const out = {}; for (const c of COINS) { if (d[c]?.usd) out[c] = d[c].usd; }
  return out;
}
async function fetchAllSources() {
  const defs = [['coingecko', srcCoinGecko], ['binance', srcBinance], ['coinbase', srcCoinbase], ['kraken', srcKraken]];
  const results = await Promise.allSettled(defs.map(([, fn]) => fn()));
  const perCoin = { bitcoin: {}, ethereum: {}, ripple: {} };
  const status = {};
  results.forEach((res, i) => {
    const name = defs[i][0];
    if (res.status === 'fulfilled') {
      status[name] = 'ok';
      COINS.forEach(c => { const v = res.value[c]; if (typeof v === 'number' && isFinite(v) && v > 0) perCoin[c][name] = v; });
    } else status[name] = 'fail';
  });
  return { perCoin, status };
}

// ---------- Indicators ----------
const sma = (a, p) => a.length < p ? null : a.slice(-p).reduce((x, y) => x + y, 0) / p;
function ema(a, p) { if (!a.length) return null; const k = 2 / (p + 1); let e = a[0]; for (let i = 1; i < a.length; i++) e = a[i] * k + e * (1 - k); return e; }
function rsi(a, p = 14) {
  if (a.length < p + 1) return 50;
  let g = 0, l = 0;
  for (let i = a.length - p; i < a.length; i++) { const d = a[i] - a[i - 1]; if (d >= 0) g += d; else l -= d; }
  const ag = g / p, al = l / p;
  return al === 0 ? 100 : 100 - 100 / (1 + ag / al);
}
function stddev(a) { const m = a.reduce((x, y) => x + y, 0) / a.length; return Math.sqrt(a.reduce((s, v) => s + (v - m) ** 2, 0) / a.length); }
function linreg(a, p = 30) {
  const s = a.slice(-p); const n = s.length; if (n < 5) return null;
  const xb = (n - 1) / 2, yb = s.reduce((x, y) => x + y, 0) / n;
  let num = 0, den = 0;
  s.forEach((y, i) => { num += (i - xb) * (y - yb); den += (i - xb) ** 2; });
  const slope = den ? num / den : 0, intercept = yb - slope * xb;
  return { slope, predict: steps => intercept + slope * (n - 1 + steps) };
}

// ---------- Prediction ----------
function predictNext(coin) {
  const cs = state.coins[coin];
  const closes = cs.priceHistory.map(p => p.c);
  if (closes.length < 30) return null;
  const cur = closes[closes.length - 1];
  const e12 = ema(closes, 12), e26 = ema(closes, 26);
  const r = rsi(closes, 14);
  const vol = stddev(closes.slice(-20));
  const lr = linreg(closes, 30);
  const bbMid = sma(closes, 20) || cur;
  const bbSd = stddev(closes.slice(-20));
  const emaSig = e12 > e26 ? 1 : -1;
  const macdSig = (e12 - e26) > 0 ? 1 : -1;
  const rsiSig = r < 30 ? 1 : r > 70 ? -1 : (50 - r) / 50 * 0.5;
  const drift = lr ? lr.slope * 0.15 : 0;
  const models = {
    sma: cur + (e12 - cur) * 0.05 + emaSig * vol * 0.08,
    rsi: cur + rsiSig * vol * 0.15 + drift,
    macd: cur + macdSig * vol * 0.12 + drift,
    lin: lr ? lr.predict(1) : cur + drift,
    bb: cur + ((cur < bbMid - 2 * bbSd) ? 0.5 : (cur > bbMid + 2 * bbSd) ? -0.5 : 0) * vol
  };
  const w = cs.weights;
  const wSum = w.sma + w.rsi + w.macd + w.lin + w.bb;
  const predicted = (models.sma*w.sma + models.rsi*w.rsi + models.macd*w.macd + models.lin*w.lin + models.bb*w.bb) / wSum;
  return { predicted, models, priceAtPrediction: cur };
}
function nextBoundary() { const ms = 15 * 60 * 1000; return Math.ceil(Date.now() / ms) * ms; }

// ---------- Learning ----------
function recalcWeights(coin) {
  const cs = state.coins[coin];
  const keys = ['sma', 'rsi', 'macd', 'lin', 'bb'];
  const inv = {}; let any = false;
  keys.forEach(k => { const s = cs.scores[k]; if (s.n > 0) { inv[k] = 1 / (s.e / s.n + 0.05); any = true; } else inv[k] = null; });
  if (!any) return;
  const known = keys.filter(k => inv[k] !== null);
  const meanInv = known.reduce((a, k) => a + inv[k], 0) / known.length;
  keys.forEach(k => { if (inv[k] === null) inv[k] = meanInv; });
  const tot = keys.reduce((a, k) => a + inv[k], 0);
  const lr = 0.3;
  keys.forEach(k => { cs.weights[k] = cs.weights[k] * (1 - lr) + (inv[k] / tot) * lr; });
  const wt = keys.reduce((a, k) => a + cs.weights[k], 0);
  keys.forEach(k => cs.weights[k] /= wt);
}

// ---------- 24/7 cycle ----------
async function cycle() {
  try {
    const { perCoin, status } = await fetchAllSources();
    const nowMs = Date.now();
    COINS.forEach(coin => {
      const cs = state.coins[coin];
      const prices = Object.values(perCoin[coin]);

      // Determine the price to use this cycle.
      // Prefer fresh sources; if none came in this minute, fall back to the
      // last known verified price so scoring/history still proceed for EVERY coin.
      let verified = null;
      if (prices.length) {
        verified = median(prices);
        state.lastPrices[coin] = { verified, sources: perCoin[coin], ts: nowMs };
        cs.priceHistory.push({ t: nowMs, c: verified });
        if (cs.priceHistory.length > 200) cs.priceHistory.shift();
      } else if (state.lastPrices[coin]?.verified) {
        verified = state.lastPrices[coin].verified; // fallback for scoring only
      }

      // ----- Score any pending predictions whose target time has passed -----
      // This now runs every cycle for every coin, as long as we have ANY price
      // to compare against (fresh or last-known), so no coin gets left behind.
      if (verified !== null) {
        const stillPending = [];
        cs.pending.forEach(p => {
          if (nowMs < p.targetMs) { stillPending.push(p); return; }
          const actual = verified;
          const errPct = Math.abs(actual - p.predicted) / actual * 100;
          const higherLower = actual > p.predicted ? 'HIGHER' : actual < p.predicted ? 'LOWER' : 'EXACT';
          cs.log.unshift({ targetMs: p.targetMs, predicted: p.predicted, actual, higherLower, errPct, ts: nowMs });
          if (cs.log.length > 1000) cs.log.pop();
          Object.keys(cs.scores).forEach(k => {
            if (p.models[k] === undefined) return;
            cs.scores[k].e += Math.abs(actual - p.models[k]) / actual * 100;
            cs.scores[k].n += 1;
          });
          recalcWeights(coin);
        });
        cs.pending = stillPending;
      }

      // ----- Make a fresh prediction for the next interval -----
      // Only when we have enough real history (needs fresh price data to grow).
      const pred = predictNext(coin);
      if (pred) {
        const targetMs = nextBoundary();
        if (!cs.pending.some(p => p.targetMs === targetMs)) {
          cs.pending.push({ targetMs, predicted: pred.predicted, models: pred.models, priceAtPrediction: pred.priceAtPrediction });
          if (cs.pending.length > 300) cs.pending = cs.pending.slice(-300);
        }
      }
    });
    state.lastStatus = status;
    state.updatedAt = nowMs;
    saveState();
    const ok = Object.values(status).filter(s => s === 'ok').length;
    console.log(`[cycle] ${new Date(nowMs).toISOString()} — ${ok}/4 sources ok`);
  } catch (e) { console.error('[cycle] error:', e.message); }
}

// ---------- API (built-in http, with CORS) ----------
function sendJSON(res, code, obj) {
  res.writeHead(code, {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Cache-Control': 'no-store'
  });
  res.end(JSON.stringify(obj));
}
function coinSnapshot(id) {
  const cs = state.coins[id];
  if (!cs) return null;
  const log = cs.log;
  const total = log.length;
  const higher = log.filter(r => r.higherLower === 'HIGHER').length;
  const lower = log.filter(r => r.higherLower === 'LOWER').length;
  const within1 = log.filter(r => r.errPct <= 1).length;
  const avgErr = total ? log.reduce((a, r) => a + r.errPct, 0) / total : 0;
  return {
    coin: id,
    lastPrice: state.lastPrices[id] || null,
    weights: cs.weights,
    scores: cs.scores,
    pending: cs.pending.length,
    summary: { total, higher, lower, within1, within1Pct: total ? Math.round(within1 / total * 100) : 0, avgErr },
    log: log.slice(0, 100)
  };
}

const server = http.createServer((req, res) => {
  if (req.method === 'OPTIONS') { sendJSON(res, 204, {}); return; }
  const url = new URL(req.url, `http://${req.headers.host}`);
  const p = url.pathname;
  if (p === '/') return sendJSON(res, 200, { ok: true, service: 'cryptooracle-backend', updatedAt: state.updatedAt });
  if (p === '/api/health') return sendJSON(res, 200, { ok: true, updatedAt: state.updatedAt, sources: state.lastStatus });
  if (p === '/api/prices') return sendJSON(res, 200, { prices: state.lastPrices, sources: state.lastStatus, updatedAt: state.updatedAt });
  if (p.startsWith('/api/coin/')) {
    const id = decodeURIComponent(p.split('/').pop());
    const snap = coinSnapshot(id);
    return snap ? sendJSON(res, 200, snap) : sendJSON(res, 404, { error: 'unknown coin' });
  }
  sendJSON(res, 404, { error: 'not found' });
});

// ---------- Boot ----------
async function boot() {
  try {
    await initDB();
    await loadState();
  } catch (e) {
    console.error('[boot] init error:', e.message);
  }
  server.listen(PORT, () => {
    console.log(`[server] listening on :${PORT} (persistence: ${USE_DB ? 'Postgres' : 'file'})`);
    cycle();
    setInterval(cycle, 60 * 1000);
  });
}
boot();
