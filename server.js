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

// Kalshi 15-minute up/down market series tickers (public market data, no auth).
const KALSHI_BASE = 'https://api.elections.kalshi.com/trade-api/v2';
// Series tickers, all confirmed live via the /api/kalshi-raw diagnostic:
// BTC, ETH and XRP all return active 15-min markets with valid prices.
const KALSHI_SERIES = {
  bitcoin:  'KXBTC15M',
  ethereum: 'KXETH15M',
  ripple:   'KXXRP15M'
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

// ---------- Kalshi prediction-market data (public, no auth) ----------
// Reads the 15-minute up/down market for each coin and extracts the market's
// implied probability of "up". Purely for comparison — we never trade.
// NOTE: Kalshi may rate-limit or geo-block cloud IPs; if so this returns null
// and the dashboard simply shows "market data unavailable".
async function fetchKalshi() {
  const out = {};
  for (const coin of COINS) {
    out[coin] = null;
    try {
      const series = KALSHI_SERIES[coin];
      // Get markets in this series that are open, soonest close first.
      const url = `${KALSHI_BASE}/markets?series_ticker=${series}&status=open&limit=100`;
      const r = await timedFetch(url, 9000);
      if (!r.ok) { out[coin] = { error: 'kalshi ' + r.status }; continue; }
      const d = await r.json();
      const markets = d.markets || [];
      if (!markets.length) { out[coin] = { error: 'no open markets' }; continue; }
      // Pick the market closing soonest in the future (the next 15-min interval).
      const now = Date.now();
      const upcoming = markets
        .map(m => ({ m, closeMs: Date.parse(m.close_time || m.expiration_time || 0) }))
        .filter(x => x.closeMs > now)
        .sort((a, b) => a.closeMs - b.closeMs);
      if (!upcoming.length) { out[coin] = { error: 'no upcoming' }; continue; }
      const pick = upcoming[0].m;
      // Kalshi's fixed-point migration returns price fields as STRINGS in dollars
      // (e.g. "0.5700"), not numbers. parseFloat handles strings AND numbers.
      const num = v => { if (v == null) return null; const n = parseFloat(v); return isFinite(n) ? n : null; };
      const asProb = (dollars, cents) => {
        const d = num(dollars); if (d != null) return d > 1 ? d / 100 : d;
        const c = num(cents);   if (c != null) return c / 100;
        return null;
      };
      const lastP = asProb(pick.last_price_dollars, pick.last_price);
      let yesBid = asProb(pick.yes_bid_dollars, pick.yes_bid);
      let yesAsk = asProb(pick.yes_ask_dollars, pick.yes_ask);

      // The /markets list often returns 0/empty bid-ask for fast 15-min markets.
      // Pull the market's orderbook directly — that's where the live resting
      // bid/ask actually live for actively traded markets.
      if ((yesBid == null || yesBid === 0) && (yesAsk == null || yesAsk === 0) && (lastP == null || lastP === 0)) {
        try {
          const obr = await timedFetch(`${KALSHI_BASE}/markets/${pick.ticker}/orderbook`, 8000);
          if (obr.ok) {
            const obd = await obr.json();
            const ob = obd.orderbook || obd.orderbook_fp || obd || {};
            // yes[]/no[] are arrays of [price, size]; best level is the last entry.
            const yesLevels = ob.yes || ob.yes_dollars || [];
            const noLevels  = ob.no  || ob.no_dollars  || [];
            const toProb = x => x == null ? null : (x > 1 ? x / 100 : x);
            const bestYes = yesLevels.length ? toProb(yesLevels[yesLevels.length - 1][0]) : null;
            const bestNo  = noLevels.length  ? toProb(noLevels[noLevels.length - 1][0])   : null;
            if (bestYes != null) yesBid = bestYes;
            if (bestNo != null)  yesAsk = 1 - bestNo; // No bid at p == Yes ask at (1-p)
          }
        } catch {}
      }

      let probUp = null;
      if (lastP != null && lastP > 0 && lastP < 1) probUp = lastP;
      else if (yesBid != null && yesAsk != null && (yesBid > 0 || yesAsk > 0)) probUp = (yesBid + yesAsk) / 2;
      else if (yesBid != null && yesBid > 0) probUp = yesBid;
      else if (yesAsk != null && yesAsk > 0) probUp = yesAsk;

      // The strike is the absolute price level the market grades against. Most
      // 15-min markets are "greater_or_equal" (YES = price AT/ABOVE strike), but
      // capture the type so a "below"-type market isn't interpreted backwards.
      const strike = num(pick.floor_strike) ?? num(pick.cap_strike) ?? null;
      // strikeAbove = true means YES resolves when price >= strike (the usual case).
      const st = (pick.strike_type || '').toLowerCase();
      const strikeAbove = st.includes('less') ? false : true; // 'less'/'less_or_equal' => YES is below
      out[coin] = {
        ticker: pick.ticker,
        title: pick.title || pick.yes_sub_title || pick.subtitle || '',
        closeMs: upcoming[0].closeMs,
        probUp,                                   // 0..1, market's chance of "up"
        marketDir: probUp == null ? null : (probUp >= 0.5 ? 'UP' : 'DOWN'),
        strike,                                   // absolute price level for YES
        strikeAbove,                              // true: YES = price >= strike
        volume: num(pick.volume_fp) ?? num(pick.volume) ?? null
      };
    } catch (e) {
      out[coin] = { error: e.message };
    }
  }
  return out;
}

// ---------- One-time historical seeding (Kraken 15-min candles) ----------
// Kraken's public OHLC endpoint returns up to ~720 candles per request with
// no API key. At interval=15 (minutes) that's ~7.5 days of history per coin —
// a solid head start for the trainable model. Fills priceHistory for any coin
// that doesn't already have a substantial history, so it's safe to call once.
async function seedHistory() {
  const results = {};
  // Make sure the state structure exists before we touch it (in case the DB
  // loaded an older/empty shape).
  if (!state.coins) state.coins = {};
  for (const coin of COINS) {
    try {
      if (!state.coins[coin]) state.coins[coin] = defaultCoinState();
      const cs = state.coins[coin];
      if (!Array.isArray(cs.priceHistory)) cs.priceHistory = [];
      if (cs.priceHistory.length >= 600) { results[coin] = 'skipped (already seeded)'; continue; }
      const pair = SOURCE_SYMBOLS[coin].kraken;
      const r = await timedFetch(`https://api.kraken.com/0/public/OHLC?pair=${pair}&interval=15`, 12000);
      if (!r.ok) { results[coin] = 'fetch failed ' + r.status; continue; }
      const d = await r.json();
      if (!d.result) { results[coin] = 'no result'; continue; }
      // The result object has the candle array under a pair-named key (not "last")
      const key = Object.keys(d.result).find(k => k !== 'last');
      const candles = key ? d.result[key] : null;
      if (!Array.isArray(candles) || !candles.length) { results[coin] = 'no candles'; continue; }
      // Kraken candle: [time(sec), open, high, low, close, vwap, volume, count]
      const hist = candles.map(c => ({ t: Number(c[0]) * 1000, c: parseFloat(c[4]) }))
                          .filter(p => isFinite(p.c) && p.c > 0)
                          .sort((a, b) => a.t - b.t);
      // Merge in front of any live data we've already collected, keep chronological
      const merged = [...hist, ...cs.priceHistory].sort((a, b) => a.t - b.t);
      // De-dupe by timestamp and cap
      const seen = new Set();
      cs.priceHistory = merged.filter(p => { if (seen.has(p.t)) return false; seen.add(p.t); return true; }).slice(-1500);
      results[coin] = `seeded ${hist.length} candles (history now ${cs.priceHistory.length})`;
    } catch (e) {
      results[coin] = 'error: ' + e.message;
    }
  }
  saveState();
  return results;
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

// ============================================================
//  TRAINABLE MODEL (pure JS) — logistic reg for direction,
//  linear reg for price. Trained on accumulated price history
//  with an HONEST out-of-sample (train/test) split.
//
//  IMPORTANT — guarding against look-ahead leakage:
//  For each historical point i, we build features using ONLY
//  data up to and including i, and the label is the move from
//  i to i+1. We never let future info into the features. The
//  test set is the most recent chunk the model never trained on.
// ============================================================

// Build a feature vector at index i using only closes[0..i].
// NOTE on honesty: every feature uses ONLY data up to and including index i.
// The label (built later) is the move from i to i+1. No future info leaks in.
function featuresAt(closes, i) {
  if (i < 35) return null; // need enough history behind this point
  const window = closes.slice(0, i + 1);
  const cur = window[window.length - 1];
  const at = k => window[window.length - 1 - k]; // k bars ago

  // --- Momentum over several horizons (returns %) ---
  const ret1 = (cur - at(1)) / at(1);
  const ret2 = (cur - at(2)) / at(2);
  const ret3 = (cur - at(3)) / at(3);
  const ret5 = (cur - at(5)) / at(5);
  const ret10 = (cur - at(10)) / at(10);

  // --- Acceleration: is momentum speeding up or slowing? ---
  const accel = ret1 - ((at(1) - at(2)) / at(2));

  // --- Oscillators ---
  const r14 = rsi(window, 14);
  const r7 = rsi(window, 7);

  // --- MACD (normalized) + its components ---
  const e12 = ema(window, 12), e26 = ema(window, 26);
  const macd = (e12 - e26) / cur;
  const e9base = ema(window, 9);
  const emaSpread = (e9base - e26) / cur; // short vs long trend

  // --- Price relative to moving averages ---
  const s20 = sma(window, 20) || cur;
  const s10 = sma(window, 10) || cur;
  const smaRel20 = (cur - s20) / s20;
  const smaRel10 = (cur - s10) / s10;

  // --- Bollinger position: where in the band is price (−1..+1-ish) ---
  const sd20 = stddev(window.slice(-20)) || (cur * 1e-6);
  const bollPos = (cur - s20) / (2 * sd20);

  // --- Volatility level + volatility ratio (regime change signal) ---
  const vol20 = stddev(window.slice(-20)) / cur;
  const vol5 = stddev(window.slice(-5)) / cur;
  const volRatio = vol20 > 0 ? (vol5 / vol20) : 1; // >1 = vol rising

  return [
    ret1 * 100,
    ret2 * 100,
    ret3 * 100,
    ret5 * 100,
    ret10 * 100,
    accel * 100,
    (r14 - 50) / 50,
    (r7 - 50) / 50,
    macd * 100,
    emaSpread * 100,
    smaRel20 * 100,
    smaRel10 * 100,
    bollPos,
    vol20 * 100,
    volRatio
  ];
}

const N_FEATURES = 15;
const sigmoid = z => 1 / (1 + Math.exp(-z));

// Standardize features (z-score) using training-set mean/std only.
function computeScaler(rows) {
  const means = new Array(N_FEATURES).fill(0);
  const stds = new Array(N_FEATURES).fill(0);
  rows.forEach(r => r.forEach((v, j) => { means[j] += v; }));
  means.forEach((_, j) => means[j] /= rows.length);
  rows.forEach(r => r.forEach((v, j) => { stds[j] += (v - means[j]) ** 2; }));
  stds.forEach((_, j) => { stds[j] = Math.sqrt(stds[j] / rows.length) || 1; });
  return { means, stds };
}
function applyScaler(row, scaler) {
  return row.map((v, j) => (v - scaler.means[j]) / scaler.stds[j]);
}

// Train logistic regression (direction: 1=up, 0=down) via gradient descent.
// L2 regularization (lambda) penalizes large weights → less overfitting, which
// matters now that we feed it more features. warmStart lets it resume from prior
// weights instead of starting cold, so retraining converges faster and smoother.
function trainLogistic(X, y, epochs = 400, lr = 0.1, lambda = 0.01, warmStart = null) {
  const n = X.length, d = N_FEATURES;
  let w = (warmStart && warmStart.w && warmStart.w.length === d) ? warmStart.w.slice() : new Array(d).fill(0);
  let b = (warmStart && typeof warmStart.b === 'number') ? warmStart.b : 0;
  for (let e = 0; e < epochs; e++) {
    const gw = new Array(d).fill(0); let gb = 0;
    for (let i = 0; i < n; i++) {
      const z = X[i].reduce((s, v, j) => s + v * w[j], b);
      const p = sigmoid(z);
      const err = p - y[i];
      for (let j = 0; j < d; j++) gw[j] += err * X[i][j];
      gb += err;
    }
    for (let j = 0; j < d; j++) w[j] -= lr * (gw[j] / n + lambda * w[j]);
    b -= lr * gb / n;
  }
  return { w, b };
}
function predictLogistic(model, x) {
  return sigmoid(x.reduce((s, v, j) => s + v * model.w[j], model.b));
}

// Train linear regression (predict next-interval return %) via gradient descent.
function trainLinear(X, y, epochs = 400, lr = 0.05, lambda = 0.01, warmStart = null) {
  const n = X.length, d = N_FEATURES;
  let w = (warmStart && warmStart.w && warmStart.w.length === d) ? warmStart.w.slice() : new Array(d).fill(0);
  let b = (warmStart && typeof warmStart.b === 'number') ? warmStart.b : 0;
  for (let e = 0; e < epochs; e++) {
    const gw = new Array(d).fill(0); let gb = 0;
    for (let i = 0; i < n; i++) {
      const pred = X[i].reduce((s, v, j) => s + v * w[j], b);
      const err = pred - y[i];
      for (let j = 0; j < d; j++) gw[j] += err * X[i][j];
      gb += err;
    }
    for (let j = 0; j < d; j++) w[j] -= lr * (gw[j] / n + lambda * w[j]);
    b -= lr * gb / n;
  }
  return { w, b };
}
function predictLinear(model, x) {
  return x.reduce((s, v, j) => s + v * model.w[j], model.b);
}

// Train both models for a coin on its history, with an honest out-of-sample split.
function trainModelForCoin(coin) {
  const cs = state.coins[coin];
  if (!cs) return null;
  const closes = (Array.isArray(cs.priceHistory) ? cs.priceHistory : []).map(p => p.c);
  if (closes.length < 90) return null;

  const rawX = [], dirY = [], retY = [];
  for (let i = 35; i < closes.length - 1; i++) {
    const f = featuresAt(closes, i);
    if (!f) continue;
    const nextRet = (closes[i + 1] - closes[i]) / closes[i];
    rawX.push(f);
    dirY.push(nextRet > 0 ? 1 : 0);
    retY.push(nextRet * 100);
  }
  if (rawX.length < 60) return null;

  // Chronological split (no shuffle) so the test set is truly "future".
  const split = Math.floor(rawX.length * 0.8);
  const trainRaw = rawX.slice(0, split), testRaw = rawX.slice(split);
  const dirTrain = dirY.slice(0, split), dirTest = dirY.slice(split);
  const retTrain = retY.slice(0, split), retTest = retY.slice(split);

  const scaler = computeScaler(trainRaw);
  const Xtrain = trainRaw.map(r => applyScaler(r, scaler));
  const Xtest = testRaw.map(r => applyScaler(r, scaler));

  // Warm-start from the previous model if its shape matches (faster convergence).
  const prev = cs.mlModel;
  const warmLog = (prev && prev.logModel && prev.logModel.w && prev.logModel.w.length === N_FEATURES) ? prev.logModel : null;
  const warmLin = (prev && prev.linModel && prev.linModel.w && prev.linModel.w.length === N_FEATURES) ? prev.linModel : null;

  const logModel = trainLogistic(Xtrain, dirTrain, 400, 0.1, 0.01, warmLog);
  const linModel = trainLinear(Xtrain, retTrain, 400, 0.05, 0.01, warmLin);

  // Out-of-sample direction accuracy (the number that actually matters).
  let correct = 0;
  for (let i = 0; i < Xtest.length; i++) {
    const p = predictLogistic(logModel, Xtest[i]);
    if ((p >= 0.5 ? 1 : 0) === dirTest[i]) correct++;
  }
  const dirAcc = Xtest.length ? correct / Xtest.length : 0;

  // In-sample (training) accuracy — used ONLY to detect overfitting. If this is
  // much higher than the test accuracy, the model is memorizing noise.
  let trCorrect = 0;
  for (let i = 0; i < Xtrain.length; i++) {
    const p = predictLogistic(logModel, Xtrain[i]);
    if ((p >= 0.5 ? 1 : 0) === dirTrain[i]) trCorrect++;
  }
  const trainDirAcc = Xtrain.length ? trCorrect / Xtrain.length : 0;

  // Honest baseline to beat: always guessing the majority class on the test set.
  const upRate = dirTest.reduce((a, v) => a + v, 0) / (dirTest.length || 1);
  const baseline = Math.max(upRate, 1 - upRate);

  let mae = 0;
  for (let i = 0; i < Xtest.length; i++) mae += Math.abs(predictLinear(linModel, Xtest[i]) - retTest[i]);
  mae = Xtest.length ? mae / Xtest.length : 0;

  return {
    trainedAt: Date.now(), nSamples: rawX.length, nTest: Xtest.length, nFeatures: N_FEATURES,
    scaler, logModel, linModel,
    testDirAcc: dirAcc, trainDirAcc, baselineAcc: baseline, testRetMae: mae,
    overfitGap: trainDirAcc - dirAcc
  };
}

// Live prediction from the trained model for the next interval.
function mlPredict(coin) {
  const cs = state.coins[coin];
  if (!cs) return null;
  const m = cs.mlModel;
  if (!m) return null;
  const closes = (Array.isArray(cs.priceHistory) ? cs.priceHistory : []).map(p => p.c);
  if (closes.length < 31) return null;
  const f = featuresAt(closes, closes.length - 1);
  if (!f) return null;
  const x = applyScaler(f, m.scaler);
  const pUp = predictLogistic(m.logModel, x);
  const predRetPct = predictLinear(m.linModel, x);
  const cur = closes[closes.length - 1];
  return {
    direction: pUp >= 0.5 ? 'UP' : 'DOWN',
    pUp,
    predictedPrice: cur * (1 + predRetPct / 100),
    testDirAcc: m.testDirAcc, baselineAcc: m.baselineAcc,
    trainDirAcc: m.trainDirAcc, overfitGap: m.overfitGap, nFeatures: m.nFeatures,
    testRetMae: m.testRetMae, nSamples: m.nSamples, trainedAt: m.trainedAt
  };
}

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
    const kalshi = await fetchKalshi();   // public market data for comparison
    state.kalshi = kalshi;
    const nowMs = Date.now();
    COINS.forEach(coin => {
      // Guarantee this coin's state object exists and is well-formed before use.
      // (Protects against any partial/legacy state loaded from the DB.)
      if (!state.coins[coin]) state.coins[coin] = defaultCoinState();
      const cs = state.coins[coin];
      if (!Array.isArray(cs.priceHistory)) cs.priceHistory = [];
      if (!Array.isArray(cs.pending)) cs.pending = [];
      if (!Array.isArray(cs.log)) cs.log = [];
      if (!cs.weights) cs.weights = { sma: 0.25, rsi: 0.20, macd: 0.20, lin: 0.25, bb: 0.10 };
      if (!cs.scores) cs.scores = { sma:{e:0,n:0}, rsi:{e:0,n:0}, macd:{e:0,n:0}, lin:{e:0,n:0}, bb:{e:0,n:0} };
      const prices = Object.values(perCoin[coin]);

      // Determine the price to use this cycle.
      // Prefer fresh sources; if none came in this minute, fall back to the
      // last known verified price so scoring/history still proceed for EVERY coin.
      let verified = null;
      if (prices.length) {
        verified = median(prices);
        state.lastPrices[coin] = { verified, sources: perCoin[coin], ts: nowMs };
        cs.priceHistory.push({ t: nowMs, c: verified });
        if (cs.priceHistory.length > 1500) cs.priceHistory.shift();
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

      // ----- THREE-WAY COMPARISON: market vs ensemble vs AI -----
      // Lock in all three direction calls for the next boundary (only once each),
      // then score them together when that boundary's actual price is known.
      if (!Array.isArray(cs.compare)) cs.compare = [];
      if (!cs.compareScore) cs.compareScore = { market:{c:0,n:0}, ensemble:{c:0,n:0}, ai:{c:0,n:0}, overall:{c:0,n:0} };
      // Backfill 'overall' for states created before this feature existed.
      if (!cs.compareScore.overall) cs.compareScore.overall = { c:0, n:0 };
      // Per-source reliability weights (per coin). These rise when a source is
      // right and fall when it's wrong, so the Overall Pick learns whom to trust.
      // Start neutral at 1.0 each.
      if (!cs.srcWeights) cs.srcWeights = { market:1, ensemble:1, ai:1 };

      // ----- ELITE METRICS -----
      // Calibration: for the Overall Pick, bucket predictions by stated confidence
      // and track how often each bucket actually hits. A well-calibrated forecaster's
      // 70% bucket wins ~70% of the time — THE mark of an elite predictor.
      // Bins: 50-60, 60-70, 70-80, 80-90, 90-100 (%).
      if (!cs.calibration) cs.calibration = [0,1,2,3,4].map(() => ({ sum:0, hit:0, n:0 }));
      // Brier score accumulator for the Overall Pick (lower = sharper probabilities).
      if (!cs.brier) cs.brier = { sum:0, n:0 };
      // High-conviction sub-scoreboard: only count Overall picks above a confidence
      // threshold. Pros don't bet every hand — this measures the ones it's sure of.
      if (!cs.convict) cs.convict = { c:0, n:0, threshold:0.65 };

      // (a) Score any locked comparisons whose target has passed.
      if (verified !== null) {
        const keep = [];
        cs.compare.forEach(c => {
          if (nowMs < c.targetMs) { keep.push(c); return; }
          if (c.scored) { keep.push(c); return; }

          // Kalshi grades on the AVERAGE of the final ~60 seconds of price, not a
          // single tick. Approximate that by averaging our recorded prices within
          // the last 90s before the target; fall back to the latest price.
          const hist = Array.isArray(cs.priceHistory) ? cs.priceHistory : [];
          const windowPts = hist.filter(p => p.t >= c.targetMs - 90000 && p.t <= c.targetMs + 5000);
          const settle = windowPts.length
            ? windowPts.reduce((a, p) => a + p.c, 0) / windowPts.length
            : verified;

          // Grade against the strike if we locked one (same question as the market);
          // otherwise grade vs the price at lock (old behavior). UP = the YES side.
          const ref = (typeof c.strike === 'number') ? c.strike : c.priceAtLock;
          const above = settle > ref;
          let actualDir;
          if (settle === ref) actualDir = 'FLAT';
          else if (typeof c.strike === 'number') {
            const sa = c.strikeAbove !== false; // default YES = above
            actualDir = (above === sa) ? 'UP' : 'DOWN';
          } else {
            actualDir = above ? 'UP' : 'DOWN';
          }
          c.actualDir = actualDir;
          c.actualPrice = settle;        // the averaged settlement value we graded on
          c.gradedVs = (typeof c.strike === 'number') ? 'strike' : 'priceAtLock';
          c.scored = true;
          // Tally correctness for each predictor that made a call (now incl. overall).
          [['market', c.marketDir], ['ensemble', c.ensembleDir], ['ai', c.aiDir], ['overall', c.overallDir]].forEach(([k, dir]) => {
            if (!dir || actualDir === 'FLAT') return;
            cs.compareScore[k].n += 1;
            if (dir === actualDir) cs.compareScore[k].c += 1;
          });

          // ----- Update per-source reliability weights (the learning step) -----
          // Each of the 3 base sources nudges its weight up when right, down when
          // wrong, via a slow exponential update. This is how the Overall Pick
          // "keeps improving itself" — it gradually trusts the better sources more.
          if (actualDir !== 'FLAT') {
            const LR = 0.04, MINW = 0.2, MAXW = 3.0;
            [['market', c.marketDir], ['ensemble', c.ensembleDir], ['ai', c.aiDir]].forEach(([k, dir]) => {
              if (!dir) return;
              const right = dir === actualDir;
              const w = cs.srcWeights[k] ?? 1;
              // Multiplicative-weights style: reward correct, penalize wrong.
              const next = right ? w * (1 + LR) : w * (1 - LR);
              cs.srcWeights[k] = Math.max(MINW, Math.min(MAXW, next));
            });
          }

          // ----- Record ELITE METRICS for the Overall Pick -----
          if (actualDir !== 'FLAT' && c.overallDir && typeof c.overallConf === 'number') {
            const hit = c.overallDir === actualDir ? 1 : 0;
            const conf = Math.max(0.5, Math.min(1, c.overallConf)); // stated P(correct)

            // Calibration bin (by confidence): 50-60→0, 60-70→1, 70-80→2, 80-90→3, 90-100→4
            let bin = Math.floor((conf - 0.5) / 0.1);
            bin = Math.max(0, Math.min(4, bin));
            cs.calibration[bin].sum += conf;
            cs.calibration[bin].hit += hit;
            cs.calibration[bin].n += 1;

            // Brier score: (forecast_prob_of_actual − outcome)². For a directional
            // call with confidence `conf`, the forecast prob that it's correct is conf;
            // outcome is 1 if right, 0 if wrong. Lower is sharper. 0.25 = no skill.
            cs.brier.sum += Math.pow(conf - hit, 2);
            cs.brier.n += 1;

            // High-conviction sub-scoreboard: only the picks it was most sure about.
            if (conf >= (cs.convict.threshold || 0.65)) {
              cs.convict.n += 1;
              if (hit) cs.convict.c += 1;
            }
          }
          keep.unshift(c); // keep scored ones at the front (recent history)
        });
        // Keep a bounded history of scored comparisons + all still-pending ones.
        cs.compare = keep.slice(0, 500);
      }

      // (b) Lock in a new comparison for the next boundary if not already locked.
      if (verified !== null && pred) {
        const targetMs = nextBoundary();
        if (!cs.compare.some(c => c.targetMs === targetMs)) {
          const km = kalshi[coin];
          const ml = mlPredict(coin);
          // If Kalshi gives us the strike (absolute level it grades against), all
          // three predictors answer the SAME question Kalshi poses, e.g.
          // "will BTC be >= $68,800 at close?". We define UP = the YES side of that
          // market. For the usual "greater_or_equal" market YES = price >= strike;
          // for a "less" market YES = price <= strike. If no strike, fall back to
          // generic direction vs current price.
          const strike = (km && typeof km.strike === 'number') ? km.strike : null;
          const strikeAbove = !km || km.strikeAbove !== false; // default: YES = above
          const ref = strike != null ? strike : verified;
          // Map a predicted price to a YES/NO (UP/DOWN) call given the strike sense.
          const dirVsStrike = (price) => {
            if (price === ref) return 'FLAT';
            const above = price > ref;
            // YES (UP) when above & strikeAbove, or below & !strikeAbove.
            return (above === strikeAbove) ? 'UP' : 'DOWN';
          };

          // Ensemble: its predicted price vs the reference (strike or current).
          const ensembleDir = strike != null ? dirVsStrike(pred.predicted)
            : (pred.predicted > ref ? 'UP' : pred.predicted < ref ? 'DOWN' : 'FLAT');
          // AI: prefer its predicted price vs strike; without a strike use its own direction call.
          let aiDir = null;
          if (ml) {
            aiDir = (strike != null && typeof ml.predictedPrice === 'number')
              ? dirVsStrike(ml.predictedPrice)
              : ml.direction;
          }
          // Market: Kalshi's own implied call (YES if probUp >= 0.5).
          const marketDir = (km && km.marketDir) ? km.marketDir : null;

          // ----- OVERALL PICK: combine the 3 via (reliability weight × confidence) -----
          // Confidence = how far each source leans from a 50/50 coin flip (0..1).
          //   market:   |probUp - 0.5| * 2
          //   ai:       |pUp - 0.5| * 2
          //   ensemble: scaled distance of predicted price from the reference level
          // Each source casts a signed vote (+ for UP, − for DOWN) sized by
          // weight × confidence. We sum the votes; the sign is the Overall direction
          // and the magnitude (squashed to 0..1) is its confidence.
          const sw = cs.srcWeights || { market:1, ensemble:1, ai:1 };
          const signOf = d => d === 'UP' ? 1 : d === 'DOWN' ? -1 : 0;
          const marketConf = (km && typeof km.probUp === 'number') ? Math.min(1, Math.abs(km.probUp - 0.5) * 2) : 0;
          const aiConf = (ml && typeof ml.pUp === 'number') ? Math.min(1, Math.abs(ml.pUp - 0.5) * 2) : 0;
          // Ensemble confidence: how far its predicted % move is from the reference,
          // capped so a wild prediction can't dominate. ~0.5% move => full confidence.
          const ensMovePct = ref > 0 ? Math.abs(pred.predicted - ref) / ref : 0;
          const ensConf = Math.min(1, ensMovePct / 0.005);

          let vote = 0, totalW = 0;
          if (marketDir) { vote += sw.market * marketConf * signOf(marketDir); totalW += sw.market * marketConf; }
          if (ensembleDir && ensembleDir !== 'FLAT') { vote += sw.ensemble * ensConf * signOf(ensembleDir); totalW += sw.ensemble * ensConf; }
          if (aiDir && aiDir !== 'FLAT') { vote += sw.ai * aiConf * signOf(aiDir); totalW += sw.ai * aiConf; }

          let overallDir = null, overallConf = null, overallConfRaw = null;
          if (totalW > 0 && Math.abs(vote) > 1e-9) {
            overallDir = vote > 0 ? 'UP' : 'DOWN';
            // Raw conviction: net vote as a fraction of total weight, mapped to ~0.5..1.
            overallConfRaw = 0.5 + 0.5 * Math.min(1, Math.abs(vote) / totalW);
          } else if (marketDir || ensembleDir || aiDir) {
            let m = 0;
            if (marketDir) m += sw.market * signOf(marketDir);
            if (ensembleDir) m += sw.ensemble * signOf(ensembleDir);
            if (aiDir) m += sw.ai * signOf(aiDir);
            if (m !== 0) { overallDir = m > 0 ? 'UP' : 'DOWN'; overallConfRaw = 0.5; }
          }

          // ----- CALIBRATION: make stated confidence TRUE -----
          // Look up the historical hit-rate of the confidence bin this prediction
          // falls into. If the model has said "70%" enough times, we trust the
          // empirical hit-rate over the raw number, blending more toward reality as
          // evidence accumulates. This is what makes "70%" actually mean 70%.
          if (overallConfRaw != null) {
            let bin = Math.floor((overallConfRaw - 0.5) / 0.1);
            bin = Math.max(0, Math.min(4, bin));
            const cal = cs.calibration && cs.calibration[bin];
            if (cal && cal.n >= 10) {
              const empirical = cal.hit / cal.n;            // what actually happened in this bin
              const trust = Math.min(1, cal.n / 50);        // more samples → trust empirical more
              overallConf = overallConfRaw * (1 - trust) + empirical * trust;
              overallConf = Math.max(0.5, Math.min(0.99, overallConf));
            } else {
              overallConf = overallConfRaw;                 // not enough history yet
            }
          }

          cs.compare.unshift({
            targetMs,
            priceAtLock: verified,
            strike,                               // the level we'll grade against (null = vs priceAtLock)
            strikeAbove,                          // true: YES = price >= strike
            marketDir, marketProbUp: (km && typeof km.probUp === 'number') ? km.probUp : null,
            ensembleDir, ensemblePredicted: pred.predicted,
            aiDir, aiProbUp: ml ? ml.pUp : null, aiPredicted: ml ? ml.predictedPrice : null,
            overallDir, overallConf, overallConfRaw,   // calibrated + raw confidence
            srcWeightsAtLock: { ...sw },          // snapshot of trust weights when locked
            scored: false, lockedAt: nowMs
          });
          cs.compare = cs.compare.slice(0, 500);
        }
      }

      // ----- Retrain the ML model periodically (at most every ~15 min/coin) -----
      // Training is gradient descent over the full history, so we throttle it
      // rather than running it every single cycle.
      const RETRAIN_MS = 5 * 60 * 1000;
      if (!cs.mlTrainedAt || (nowMs - cs.mlTrainedAt) > RETRAIN_MS) {
        const trained = trainModelForCoin(coin);
        if (trained) {
          cs.mlModel = trained;
          cs.mlTrainedAt = nowMs;
          console.log(`[ml] ${coin} retrained on ${trained.nSamples} samples — out-of-sample dir acc ${(trained.testDirAcc*100).toFixed(1)}% (baseline ${(trained.baselineAcc*100).toFixed(1)}%)`);
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
  const log = Array.isArray(cs.log) ? cs.log : [];
  const total = log.length;
  const higher = log.filter(r => r.higherLower === 'HIGHER').length;
  const lower = log.filter(r => r.higherLower === 'LOWER').length;
  const within1 = log.filter(r => r.errPct <= 1).length;
  const avgErr = total ? log.reduce((a, r) => a + r.errPct, 0) / total : 0;

  // ----- Live in-block readout -----
  // The OFFICIAL prediction for the current 15-min block is locked (it's the
  // pending entry for the upcoming boundary). Within the block we also report
  // how the live price is tracking versus that locked prediction — this updates
  // every cycle without changing the locked call.
  const ms = 15 * 60 * 1000;
  const curBoundary = Math.ceil(Date.now() / ms) * ms;
  const lockedPending = Array.isArray(cs.pending) ? cs.pending.find(p => p.targetMs === curBoundary) : null;
  const livePrice = state.lastPrices[id]?.verified ?? null;
  let live = null;
  if (lockedPending) {
    const minsLeft = Math.max(0, Math.round((curBoundary - Date.now()) / 60000));
    live = {
      targetMs: curBoundary,
      minutesLeft: minsLeft,
      lockedPredicted: lockedPending.predicted,
      lockedAtPrice: lockedPending.priceAtPrediction,
      lockedDir: lockedPending.predicted > lockedPending.priceAtPrediction ? 'UP' : lockedPending.predicted < lockedPending.priceAtPrediction ? 'DOWN' : 'FLAT',
      livePrice,
      // How the live price is currently tracking vs the locked prediction:
      trackingDir: (livePrice != null) ? (livePrice > lockedPending.priceAtPrediction ? 'UP' : livePrice < lockedPending.priceAtPrediction ? 'DOWN' : 'FLAT') : null,
      gapToPredicted: (livePrice != null) ? (lockedPending.predicted - livePrice) : null
    };
  }

  // Trained-model live prediction + honest out-of-sample metrics (null until trained)
  const ml = mlPredict(id);

  // Honest comparison: ensemble's own direction hit-rate on its scored log.
  // (HIGHER means actual came in above prediction = ensemble said "lower than reality".)
  // We compare each logged prediction's implied direction vs the actual move.
  const ensembleScored = log.length;
  // We can't recompute ensemble direction perfectly here without prior price,
  // so we report the ensemble's within-1% rate as its quality proxy alongside
  // the ML model's honest test accuracy. The dashboard explains both.

  return {
    coin: id,
    lastPrice: state.lastPrices[id] || null,
    weights: cs.weights,
    scores: cs.scores,
    pending: cs.pending.length,
    summary: { total, higher, lower, within1, within1Pct: total ? Math.round(within1 / total * 100) : 0, avgErr },
    ml: ml ? {
      direction: ml.direction,
      probUp: Math.round(ml.pUp * 1000) / 10,           // % chance up, 1 decimal
      predictedPrice: ml.predictedPrice,
      testDirAccPct: Math.round(ml.testDirAcc * 1000) / 10,   // honest out-of-sample
      baselineAccPct: Math.round(ml.baselineAcc * 1000) / 10, // bar to beat
      beatsBaseline: ml.testDirAcc > ml.baselineAcc,
      trainDirAccPct: ml.trainDirAcc != null ? Math.round(ml.trainDirAcc * 1000) / 10 : null, // in-sample (for overfit check)
      overfitGapPct: ml.overfitGap != null ? Math.round(ml.overfitGap * 1000) / 10 : null,    // train minus test; big = overfitting
      nFeatures: ml.nFeatures || null,
      testRetMae: Math.round(ml.testRetMae * 1000) / 1000,
      nSamples: ml.nSamples,
      trainedAt: ml.trainedAt
    } : null,
    log: log.slice(0, 100),
    live,
    // Three-way comparison: market (Kalshi) vs ensemble vs AI
    compare: {
      latest: (Array.isArray(cs.compare) && cs.compare.length) ? cs.compare[0] : null,
      recent: Array.isArray(cs.compare) ? cs.compare.filter(c => c.scored).slice(0, 30) : [],
      score: cs.compareScore || { market:{c:0,n:0}, ensemble:{c:0,n:0}, ai:{c:0,n:0}, overall:{c:0,n:0} },
      srcWeights: cs.srcWeights || { market:1, ensemble:1, ai:1 },
      // Elite forecasting metrics for the Overall Pick:
      elite: {
        // Brier score: 0 = perfect, 0.25 = no skill (coin flip), lower is better.
        brier: (cs.brier && cs.brier.n > 0) ? cs.brier.sum / cs.brier.n : null,
        brierN: cs.brier ? cs.brier.n : 0,
        // High-conviction record: accuracy on only the picks it was most sure of.
        conviction: cs.convict ? { c: cs.convict.c, n: cs.convict.n, threshold: cs.convict.threshold } : null,
        // Calibration curve: per confidence bin, stated vs actual hit-rate.
        calibration: (cs.calibration || []).map((b, i) => ({
          band: `${50 + i*10}-${60 + i*10}%`,
          stated: b.n > 0 ? b.sum / b.n : null,
          actual: b.n > 0 ? b.hit / b.n : null,
          n: b.n
        }))
      },
      kalshi: (() => {
        const k = (state.kalshi && state.kalshi[id]) ? { ...state.kalshi[id] } : {};
        const lp = state.lastPrices && state.lastPrices[id] ? state.lastPrices[id].verified : null;
        if (lp != null) k.livePrice = lp;   // coin's current spot price, for the market card
        return k;
      })()
    }
  };
}

const server = http.createServer((req, res) => {
  if (req.method === 'OPTIONS') { sendJSON(res, 204, {}); return; }
  const url = new URL(req.url, `http://${req.headers.host}`);
  const p = url.pathname;
  if (p === '/') return sendJSON(res, 200, { ok: true, service: 'cryptooracle-backend', updatedAt: state.updatedAt });
  if (p === '/api/health') return sendJSON(res, 200, { ok: true, updatedAt: state.updatedAt, sources: state.lastStatus });
  if (p === '/api/prices') return sendJSON(res, 200, { prices: state.lastPrices, sources: state.lastStatus, updatedAt: state.updatedAt });
  if (p === '/api/kalshi-raw') {
    // Diagnostic: for EACH coin, show the soonest market's key price fields so we
    // can see exactly what ETH and XRP return vs BTC. Safe (public, read-only).
    (async () => {
      const result = {};
      for (const coin of COINS) {
        try {
          const series = KALSHI_SERIES[coin];
          const r = await timedFetch(`${KALSHI_BASE}/markets?series_ticker=${series}&status=open&limit=20`, 9000);
          if (!r.ok) { result[coin] = { series, error: 'kalshi ' + r.status }; continue; }
          const d = await r.json();
          const markets = d.markets || [];
          const now = Date.now();
          const upcoming = markets
            .map(m => ({ m, closeMs: Date.parse(m.close_time || m.expiration_time || 0) }))
            .filter(x => x.closeMs > now)
            .sort((a, b) => a.closeMs - b.closeMs);
          const pick = upcoming.length ? upcoming[0].m : (markets[0] || null);
          result[coin] = {
            series,
            count: markets.length,
            ticker: pick ? pick.ticker : null,
            status: pick ? pick.status : null,
            last_price_dollars: pick ? pick.last_price_dollars : null,
            yes_bid_dollars: pick ? pick.yes_bid_dollars : null,
            yes_ask_dollars: pick ? pick.yes_ask_dollars : null,
            volume_fp: pick ? pick.volume_fp : null
          };
        } catch (e) { result[coin] = { error: e.message }; }
      }
      sendJSON(res, 200, result);
    })();
    return;
  }
  if (p === '/api/seed') {
    // One-time historical seeding. Protected by a token so it can't be run by
    // random visitors. Pass ?key=YOUR_TOKEN matching the SEED_KEY env var.
    const provided = url.searchParams.get('key') || '';
    const expected = process.env.SEED_KEY || 'seed-cryptooracle-once';
    if (provided !== expected) { sendJSON(res, 403, { error: 'forbidden — missing or wrong key' }); return; }
    seedHistory().then(results => sendJSON(res, 200, { ok: true, results }))
                 .catch(e => sendJSON(res, 500, { error: e.message }));
    return;
  }
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
