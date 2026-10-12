/**
 * HomeMood — приймання замовлень.
 * Netlify Function. Виконується на сервері, тому токени беруться зі змінних
 * середовища і НІКОЛИ не потрапляють у браузер покупця.
 *
 * Змінні середовища (Netlify → Site configuration → Environment variables):
 *   TELEGRAM_BOT_TOKEN   — токен бота з @BotFather
 *   TELEGRAM_CHAT_ID     — куди слати повідомлення
 *   SHEETS_WEBHOOK_URL   — адреса /exec вашого Google Apps Script
 *   SHEETS_SHARED_SECRET — спільний рядок, щоб у таблицю не писав хтось чужий
 *   ALLOWED_ORIGIN       — https://ваш-домен, обмежує CORS
 * Жодна з них не має потрапляти в HTML. Докладно — в INTEGRATIONS.md.
 *
 * Відповідь: { ok: true, id, channels } або { ok: false, error }.
 * ok:true означає, що замовлення прийняв щонайменше один канал.
 */

const MAX_BODY = 32 * 1024;
const MAX_ITEMS = 60;
const CLAIM_TTL_MS = 24 * 60 * 60 * 1000;   // скільки пам'ятаємо замовлення
const INFLIGHT_MS = 90 * 1000;              // скільки чекаємо на паралельну спробу

/* ════════════════════════════════════════════════════════════════
   Сховище заявок

   Дедуплікація між різними екземплярами функції потребує спільного
   сховища з атомарною перевіркою. Беремо Netlify Blobs: його set()
   з onlyIfNew:true або створює запис, або повертає modified:false,
   якщо ключ уже існує — це і є атомарне «захоплення» замовлення.

   Якщо Blobs недоступний (сайт залито перетягуванням, без збірки —
   тоді залежність не встановлена), переходимо на пам'ять процесу
   і ЧЕСНО повідомляємо про це в логах і у відповіді полем
   dedupe:"degraded". Мовчки вдавати, що захист працює, не можна.
   ════════════════════════════════════════════════════════════════ */

let storePromise = null;

async function getClaims() {
  if (storePromise) return storePromise;
  storePromise = (async () => {
    try {
      const { getStore } = await import('@netlify/blobs');
      const store = getStore({ name: 'homemood-orders', consistency: 'strong' });
      // пробна операція: якщо оточення не налаштоване, тут і впаде
      await store.getMetadata('__probe__');
      return { kind: 'blobs', store };
    } catch (err) {
      console.warn('blobs unavailable, dedupe degraded to process memory:', err && err.message);
      return { kind: 'memory', map: new Map() };
    }
  })();
  return storePromise;
}

/** Атомарно займає ключ. true — зайняли ми, false — уже існує. */
async function claim(claims, key, value) {
  if (claims.kind === 'blobs') {
    const res = await claims.store.setJSON(key, value, { onlyIfNew: true });
    return res.modified === true;
  }
  const now = Date.now();
  for (const [k, v] of claims.map) if (now - v.at > CLAIM_TTL_MS) claims.map.delete(k);
  if (claims.map.has(key)) return false;
  claims.map.set(key, value);
  return true;
}

async function readClaim(claims, key) {
  if (claims.kind === 'blobs') {
    const r = await claims.store.getWithMetadata(key, { type: 'json', consistency: 'strong' });
    return r ? { value: r.data, etag: r.etag } : null;
  }
  const v = claims.map.get(key);
  return v ? { value: v, etag: 'mem' } : null;
}

async function updateClaim(claims, key, value, etag) {
  if (claims.kind === 'blobs') {
    // onlyIfMatch ставимо лише коли знаємо етег — щоб не затерти чужий
    // паралельний запис. Без етега (ми самі щойно створили запис)
    // пишемо беззастережно.
    const opts = etag ? { onlyIfMatch: etag } : {};
    const r = await claims.store.setJSON(key, value, opts);
    if (!r.modified) console.warn('claim update raced, leaving as is');
    return r.modified === true;
  }
  claims.map.set(key, value);
  return true;
}

/* ════════════════════════════════════════════════════════════════
   Службове
   ════════════════════════════════════════════════════════════════ */

function cors(origin) {
  const allow = process.env.ALLOWED_ORIGIN || '*';
  return {
    'Access-Control-Allow-Origin': allow === '*' ? '*' : allow,
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
  };
}

function str(v, max) {
  if (v === undefined || v === null) return '';
  return String(v).replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/g, '').slice(0, max || 200);
}

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

/* У логах не має бути ні телефонів, ні імен, ні токенів.
   Для зв'язування записів досить короткого відбитка. */
function fingerprint(s) {
  let h = 2166136261;
  for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619); }
  return (h >>> 0).toString(36);
}

/* Мінімальний захист від найпростішого флуду. Лічильник живе в пам'яті
   екземпляра, тож це бар'єр, а не захист від цілеспрямованої атаки. */
const hits = new Map();
function throttled(ip) {
  const now = Date.now(), windowMs = 60 * 1000, limit = 5;
  const rec = hits.get(ip) || { n: 0, t: now };
  if (now - rec.t > windowMs) { rec.n = 0; rec.t = now; }
  rec.n += 1;
  hits.set(ip, rec);
  if (hits.size > 5000) hits.clear();
  return rec.n > limit;
}

/* ════════════════════════════════════════════════════════════════
   Перевірка замовлення
   ════════════════════════════════════════════════════════════════ */

function validate(order) {
  if (!order || typeof order !== 'object') return 'порожнє замовлення';
  const name = str(order.name, 120).trim();
  const phone = str(order.phone, 40).trim();
  if (name.length < 2) return 'не вказано ім’я';
  const digits = phone.replace(/\D/g, '');
  if (digits.length < 9 || digits.length > 15) return 'некоректний телефон';
  if (!Array.isArray(order.items) || order.items.length === 0) return 'порожній кошик';
  if (order.items.length > MAX_ITEMS) return 'забагато позицій';
  for (const it of order.items) {
    if (!it || typeof it !== 'object') return 'пошкоджена позиція';
    const q = num(it.qty);
    if (q < 1 || q > 999) return 'некоректна кількість';
    if (num(it.price) < 0) return 'некоректна ціна';
  }
  return null;
}

const fmtMoney = n => num(n).toLocaleString('uk-UA').replace(/ /g, ' ') + ' ₴';

/** Нормалізований вигляд замовлення. Текст і суми рахує сервер:
 *  те, що прийшло з браузера, — лише вихідні дані, не готовий текст. */
function normalize(o) {
  const items = o.items.map(i => ({
    sku: str(i.sku, 40),
    title: str(i.title, 120),
    opt: str(i.opt, 80),
    qty: num(i.qty),
    price: num(i.price),
    line: num(i.price) * num(i.qty),
  }));
  const sum = items.reduce((a, i) => a + i.line, 0);
  const claimed = num(o.total);
  return {
    no: str(o.no, 20),
    date: new Date().toLocaleString('uk-UA', { timeZone: 'Europe/Kyiv' }),
    iso: new Date().toISOString(),
    name: str(o.name, 120),
    phone: str(o.phone, 40),
    ship: str(o.ship, 200),
    city: str(o.city, 120),
    branch: str(o.branch, 120),
    pay: str(o.pay, 60),
    note: str(o.note, 500),
    items,
    sum,
    qty: items.reduce((a, i) => a + i.qty, 0),
    mismatch: claimed && Math.abs(claimed - sum) > 0.5 ? claimed : 0,
  };
}

/* ════════════════════════════════════════════════════════════════
   Telegram
   ════════════════════════════════════════════════════════════════ */

const tgEsc = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

function telegramText(o) {
  const L = [];
  L.push(`🛍 <b>Нове замовлення №${tgEsc(o.no)}</b>`);
  L.push(`<i>${tgEsc(o.date)}</i>`);
  L.push('');
  L.push('<b>Покупець</b>');
  L.push(`👤 ${tgEsc(o.name)}`);
  L.push(`📞 <code>${tgEsc(o.phone)}</code>`);
  L.push('');
  L.push(`<b>Товари</b> — ${o.items.length} ${o.items.length === 1 ? 'позиція' : 'позицій'}, ${o.qty} шт`);
  for (const i of o.items) {
    L.push(`• ${tgEsc(i.title)}${i.opt ? ' — ' + tgEsc(i.opt) : ''}`);
    L.push(`   ${i.qty} × ${fmtMoney(i.price)} = <b>${fmtMoney(i.line)}</b>${i.sku ? '   <code>' + tgEsc(i.sku) + '</code>' : ''}`);
  }
  L.push('');
  L.push(`💰 <b>Разом за товари: ${fmtMoney(o.sum)}</b>`);
  L.push('');
  L.push('<b>Доставка</b>');
  L.push(`🚚 ${tgEsc(o.ship)}`);
  if (o.city) L.push(`🏙 ${tgEsc(o.city)}${o.branch ? ' · ' + tgEsc(o.branch) : ''}`);
  L.push('');
  L.push(`💳 <b>Оплата:</b> ${tgEsc(o.pay)}`);
  if (o.note) { L.push(''); L.push(`💬 <b>Коментар:</b> ${tgEsc(o.note)}`); }
  if (o.mismatch) {
    L.push('');
    L.push(`⚠️ Сума з сайту (${fmtMoney(o.mismatch)}) не збіглася з перерахованою — перевірте замовлення.`);
  }
  return L.join('\n');
}

async function toTelegram(o) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chat = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chat) return { used: false };

  const body = {
    chat_id: chat,
    text: telegramText(o),
    parse_mode: 'HTML',
    disable_web_page_preview: true,
  };

  let r, js;
  try {
    r = await fetch('https://api.telegram.org/bot' + token + '/sendMessage', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    js = await r.json().catch(() => null);
  } catch (e) {
    console.error('telegram network error:', e && e.message);
    return { used: true, ok: false, why: 'network' };
  }

  if (!r.ok || !js || js.ok !== true) {
    const code = js && js.error_code;
    const desc = js && js.description;
    // типові причини — щоб у логах одразу було видно, що робити
    let hint = '';
    if (code === 401) hint = 'невірний TELEGRAM_BOT_TOKEN';
    else if (code === 400 && /chat not found/i.test(desc || '')) hint = 'невірний TELEGRAM_CHAT_ID';
    else if (code === 403) hint = 'напишіть боту /start, інакше він не може вам писати';
    else if (code === 429) hint = 'забагато повідомлень, Telegram просить зачекати';
    else if (code === 400 && /parse/i.test(desc || '')) hint = 'розмітка повідомлення не прийнята';
    console.error('telegram failed:', r.status, code, desc, hint ? '→ ' + hint : '');
    return { used: true, ok: false, why: hint || ('api ' + (code || r.status)) };
  }
  return { used: true, ok: true };
}

/* ════════════════════════════════════════════════════════════════
   Google Таблиця (через Apps Script)
   ════════════════════════════════════════════════════════════════ */

async function toSheets(o) {
  const url = process.env.SHEETS_WEBHOOK_URL;
  if (!url) return { used: false };

  const payload = {
    secret: process.env.SHEETS_SHARED_SECRET || '',
    order: {
      no: o.no, iso: o.iso, date: o.date,
      name: o.name, phone: o.phone,
      items: o.items, qty: o.qty, sum: o.sum,
      ship: o.ship, city: o.city, branch: o.branch,
      pay: o.pay, note: o.note,
    },
  };

  let r, txt;
  try {
    r = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'text/plain;charset=utf-8' },
      body: JSON.stringify(payload),
      redirect: 'follow',
    });
    txt = await r.text();
  } catch (e) {
    console.error('sheets network error:', e && e.message);
    return { used: true, ok: false, why: 'network' };
  }

  if (!r.ok) {
    console.error('sheets http', r.status);
    return { used: true, ok: false, why: 'http ' + r.status };
  }
  let js = null;
  try { js = JSON.parse(txt); } catch (e) { /* Apps Script іноді віддає HTML */ }
  if (js && js.ok === false) {
    const why = js.error === 'forbidden'
      ? 'SHEETS_SHARED_SECRET не збігається зі значенням у властивостях скрипта'
      : String(js.error || 'відмова');
    console.error('sheets rejected:', why);
    return { used: true, ok: false, why };
  }
  if (!js) {
    // найчастіше це сторінка входу Google: скрипт опубліковано не для «Anyone»
    console.error('sheets returned non-JSON — скрипт опубліковано не для «Anyone»?');
    return { used: true, ok: false, why: 'доступ до скрипта закритий' };
  }
  return { used: true, ok: true, duplicate: js.duplicate === true };
}

/* ════════════════════════════════════════════════════════════════
   Обробник
   ════════════════════════════════════════════════════════════════ */

export default async (req) => {
  const origin = req.headers.get('origin') || '';
  const send = (status, obj) =>
    new Response(JSON.stringify(obj), { status, headers: cors(origin) });

  if (req.method === 'OPTIONS') return new Response(null, { status: 204, headers: cors(origin) });
  if (req.method !== 'POST') return send(405, { ok: false, error: 'method not allowed' });

  const ip = req.headers.get('x-nf-client-connection-ip')
    || (req.headers.get('x-forwarded-for') || '').split(',')[0].trim()
    || 'unknown';
  if (throttled(ip)) return send(429, { ok: false, error: 'Забагато спроб. Зачекайте хвилину.' });

  let raw;
  try { raw = await req.text(); } catch (e) { return send(400, { ok: false, error: 'не вдалося прочитати запит' }); }
  if (!raw || raw.length > MAX_BODY) return send(413, { ok: false, error: 'завеликий запит' });

  let body;
  try { body = JSON.parse(raw); } catch (e) { return send(400, { ok: false, error: 'некоректний JSON' }); }

  const incoming = body && body.order ? body.order : body;
  const bad = validate(incoming);
  if (bad) return send(400, { ok: false, error: bad });

  const o = normalize(incoming);
  const tag = o.no ? o.no + '-' + fingerprint(o.phone.replace(/\D/g, '')) : '';

  /* Сховище не має права зронити замовлення. Якщо Blobs відмовить уже
     після перевірки (мережевий збій, вичерпана квота), ми не падаємо:
     працюємо без дедуплікації і позначаємо це в логах і у відповіді.
     Можливий дубль у менеджера — менше зло, ніж втрачене замовлення. */
  let claims, degraded = false;
  try {
    claims = await getClaims();
    degraded = claims.kind !== 'blobs';
  } catch (e) {
    console.warn('claim store unavailable:', e && e.message);
    claims = null; degraded = true;
  }
  const guard = async (fn, dflt) => {
    if (!claims) return dflt;
    try { return await fn(); }
    catch (e) { console.warn('claim op failed, dedupe skipped:', e && e.message); degraded = true; return dflt; }
  };

  /* ── стан за каналами ──────────────────────────────────────────
     Зберігаємо окремо, що вже дійшло в Telegram, а що — в таблицю.
     Повтор після часткового успіху дошле тільки те, чого бракує. */
  let state = { tg: 'pending', sheets: 'pending', at: Date.now() };
  let etag = null;

  if (tag && claims) {
    // true за замовчуванням: не змогли перевірити — поводимось як перша спроба
    const mine = await guard(() => claim(claims, tag, state), true);
    if (!mine) {
      const prev = await guard(() => readClaim(claims, tag), null);
      if (prev) {
        state = prev.value; etag = prev.etag;
        const fresh = Date.now() - (state.at || 0) < INFLIGHT_MS;
        const bothSettled = state.tg !== 'pending' && state.sheets !== 'pending';
        if (!bothSettled && fresh) {
          // паралельна спроба ще в роботі — не дублюємо і не брешемо про успіх
          console.log('order in flight, asked to wait:', tag);
          return send(409, { ok: false, error: 'Це замовлення вже обробляється. Зачекайте кілька секунд.' });
        }
        if (state.tg === 'sent' && state.sheets === 'sent') {
          console.log('duplicate suppressed:', tag);
          return send(200, { ok: true, id: o.no, duplicate: true, channels: state });
        }
        // частковий успіх: нижче дошлемо лише те, що не пройшло
      }
    }
  }

  const needTg = state.tg !== 'sent';
  const needSheets = state.sheets !== 'sent';

  let tg = { used: false }, sh = { used: false };
  try {
    [tg, sh] = await Promise.all([
      needTg ? toTelegram(o) : Promise.resolve({ used: true, ok: true, skipped: true }),
      needSheets ? toSheets(o) : Promise.resolve({ used: true, ok: true, skipped: true }),
    ]);
  } catch (e) {
    console.error('delivery crashed:', e && e.message);
    return send(502, { ok: false, error: 'канал доставки недоступний' });
  }

  const mark = (r, prev) => {
    if (r.skipped) return 'sent';
    if (!r.used) return 'off';
    return r.ok ? 'sent' : 'failed';
  };
  state = { tg: mark(tg, state.tg), sheets: mark(sh, state.sheets), at: Date.now() };

  if (tag && claims) {
    await guard(() => updateClaim(claims, tag, state, etag || undefined), false);
  }

  if (state.tg === 'off' && state.sheets === 'off') {
    console.error('no channel configured: set TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID or SHEETS_WEBHOOK_URL');
    return send(503, { ok: false, error: 'приймання замовлень ще не налаштоване' });
  }

  const anyDelivered = state.tg === 'sent' || state.sheets === 'sent';
  if (!anyDelivered) {
    const why = [tg.why, sh.why].filter(Boolean).join('; ');
    console.error('all channels failed for', tag || '(no id)', why);
    return send(502, { ok: false, error: 'не вдалося передати замовлення менеджеру' });
  }

  // Щонайменше один канал прийняв — замовлення не втрачене.
  const partial = state.tg === 'failed' || state.sheets === 'failed';
  if (partial) {
    console.warn('partial delivery for', tag || '(no id)', JSON.stringify(state),
                 '— повторний запит дошле те, що не пройшло');
  }
  return send(200, {
    ok: true,
    id: o.no,
    channels: state,
    ...(partial ? { partial: true } : {}),
    ...(degraded ? { dedupe: 'degraded' } : {}),
  });
};

export const config = { path: '/.netlify/functions/order' };
