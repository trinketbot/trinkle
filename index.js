// ============================================================
// TrinketBot - Unified Bot
// Raw Discord API + WebSocket gateway. Only dependency: 'ws'.
//
// Features:
//   Marketplace (Open Shop / List Item / Edit Item / Remove Item)
//   ISO board (Add / Edit / Remove)
//   Moderation (mute/ban/kick/warn/detain/release/purge)
//   Support tickets & reports
//   Brownie points
//   Sticky messages
//   Starboard
//   ISO reactions (🛎️)
//   Message/edit/delete logging
//   Member join logging
//   YouTube auto-forward
//   Embed builder (/createembed)
//   Role menus
//   Scheduled messages
// ============================================================

const WebSocket = require('ws');
const https     = require('https');
const http      = require('http');
const fs        = require('fs');

// ── HTTP keep-alive server (required by Replit Autoscale) ─────
const HTTP_PORT = parseInt(process.env.PORT || '8080', 10);
const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('TrinketBot is running!');
});
httpServer.on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    console.warn(`Port ${HTTP_PORT} in use, retrying in 3s...`);
    setTimeout(() => httpServer.listen(HTTP_PORT, '0.0.0.0'), 3000);
  } else {
    console.error('HTTP server error:', err.message);
  }
});
httpServer.listen(HTTP_PORT, '0.0.0.0', () => {
  console.log('HTTP server listening on 0.0.0.0:' + HTTP_PORT);
});

const TOKEN   = process.env.MARKETPLACE_TOKEN;
const API     = 'https://discord.com/api/v10';
const GATEWAY = 'wss://gateway.discord.gg/?v=10&encoding=json';

const httpsAgent = new https.Agent({ keepAlive: true });

// ── Intents ───────────────────────────────────────────────────
const INTENTS = 1 | 2 | 512 | 32768 | 1024 | 4096;

// ── Storage ───────────────────────────────────────────────────
function loadJSON(file, def = {}) {
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); }
  catch { return typeof def === 'function' ? def() : def; }
}
function saveJSON(file, data) { fs.writeFileSync(file, JSON.stringify(data, null, 2)); }

let cooldowns     = loadJSON('cooldowns.json');
let threads       = loadJSON('threads.json');
let brownie       = loadJSON('brownie.json');
let isoListings   = loadJSON('iso_listings.json');
let stickyData    = loadJSON('sticky.json');
let roleMenus     = loadJSON('role_menus.json');
let embedButtons  = loadJSON('embed_buttons.json');
let scheduled     = loadJSON('scheduled.json', []);
let warnings      = loadJSON('warnings.json');
let itemMessages  = loadJSON('item_messages.json'); // { [msgId]: { userId, threadId, name, price, condition, notes, photoUrls, avatarUrl, username } }

// ── Instance lock (prevents Autoscale duplicate handling) ─────
const INSTANCE_ID  = Math.random().toString(36).slice(2);
const LOCK_FILE    = 'instance.lock';
const LOCK_TTL_MS  = 8000;

function acquireInstanceLock() {
  try {
    if (fs.existsSync(LOCK_FILE)) {
      const data = JSON.parse(fs.readFileSync(LOCK_FILE, 'utf8'));
      const age  = Date.now() - data.ts;
      if (age < LOCK_TTL_MS && data.id !== INSTANCE_ID) {
        console.log(`Instance ${INSTANCE_ID} yielding to ${data.id} (lock age ${age}ms)`);
        return false;
      }
    }
    fs.writeFileSync(LOCK_FILE, JSON.stringify({ id: INSTANCE_ID, ts: Date.now() }));
    return true;
  } catch {
    return true;
  }
}

function refreshLock() {
  try {
    const existing = fs.existsSync(LOCK_FILE)
      ? JSON.parse(fs.readFileSync(LOCK_FILE, 'utf8'))
      : null;
    if (!existing || existing.id === INSTANCE_ID) {
      fs.writeFileSync(LOCK_FILE, JSON.stringify({ id: INSTANCE_ID, ts: Date.now() }));
    }
  } catch {}
}

// ── Config ────────────────────────────────────────────────────
const MARKETPLACE_PANEL_ID    = '1475232032765509874';
const MARKETPLACE_FORUM_ID    = '1466105963621777572';
const ISO_FORUM_ID            = '1466146126330597591';
const INVENTORY_FORUM_ID      = '1466105963621777572';
const ADMIN_ROLE_ID           = '1465161088814289089';
const BOT_ROLE_ID             = '1465163793934848194';
const MOD_ROLE_ID             = '1465161088814289089';
const DETAINED_ROLE_ID        = '1466156273966780682';
const MUTED_ROLE_ID           = '1465165320749584540';
const WELCOME_ROLE_ID         = '1473871114299113522';
const EMERGENCY_ROLE_ID       = '1466153665193836706';
const JOIN_LOG_CHANNEL_ID     = '1465166830657343708';
const MESSAGE_LOG_CHANNEL_ID  = '1465101983760253262';
const MOD_LOG_CHANNEL_ID      = '1465102072356802807';
const STARBOARD_CHANNEL_ID    = '1466287239603687456';
const STARBOARD_EMOJI_ID      = '1473884427137323108';
const THREAD_CHANNEL_ID       = '1466156903561429279';
const RECEIPT_CHANNEL_ID      = '1466860508526284851';
const SUPPORT_PANEL_CHANNEL_ID= '1465162656590860482';
const GENERAL_TICKET_CAT_ID   = '1468473438703325214';
const REPORT_TICKET_CAT_ID    = '1468473302493429976';
const REP_CHANNEL_ID          = '1469112081750949930';
const WORKBENCH_CHANNEL_ID    = '1468649429694152827';
const DEFAULT_CHANNEL_ID      = '1465191877987930298';
const AUTO_FORWARD_CHANNEL_ID = '1470967536584626187';
const COLOR                   = 0xe0ad76;
const COOLDOWN_DAYS           = 14;
const BUMP_COOLDOWN_MS        = 72 * 60 * 60 * 1000;
const ISO_EMOJI               = '🛎️';

const TAG_IDS = [
  '1466283426075115583','1466283469452873730','1466283480735420488',
  '1466283506467602472','1466283217496707072','1466283356701331642',
  '1466283393732837602','1466283407695806808','1466283544480448552',
  '1466283529175437364','1466283590080794867','1466283603565482118',
  '1466283716371288136','1466283732221820938','1466283816078278731',
  '1466704594510811270','1474194075220443166',
];

const IPTHREAD_IDS = [
  '1466683002028560498','1466982282106769451','1466706222693482597',
  '1466700846166310945','1466699623082102872','1466698761844687062',
  '1466696231425413319','1466693473804746897','1466692748508794921',
  '1466690531840102440','1466688832471826569','1466686206116102429',
  '1466687111305625765',
];

const YOUTUBE_WEBHOOKS = {
  'jellycat':    { webhook: 'https://discord.com/api/webhooks/1468102067640864911/6wdPMRu9BEXQyVORiAuZeqkAwkzuiHqeHepLPtCBqMMWUhp6mhHkYZjy19h6GESoBfnC', role_id: '1466117927584534611', names: ['Jellycat'] },
  'cureplaneta': { webhook: 'https://discord.com/api/webhooks/1468102761135603722/GljN_CARdl6vKrRhqd4JcPJs2zTGyOvYT5f1DCOUZCggFgjxTvBSeVidLOO87Hpv_Jjg', role_id: '1466118119922864169', names: ['Cureplaneta Official','Cureplaneta'] },
  'pop mart':    { webhook: 'https://discord.com/api/webhooks/1465602242005696681/AxYnvew2QgcYEJHl4PQUGlbEX3-EC_2YxIYwSh_ZorqP6nmyNk9Q0fBR56puAbvCpW43', role_id: '1466117757212033197', names: ['POP MART'] },
  'smiski':      { webhook: 'https://discord.com/api/webhooks/1468102419983503370/Gy8r761L4mQOPRjnWrj4Gx1jlitxgCRVIa4tjyzX_boM1P8mGc7OeuAOTPOlSpZwfnrw', role_id: '1467384983445442706', names: ['【Official】スミスキー Smiski','Smiski','スミスキー'] },
  'dreams':      { webhook: 'https://discord.com/api/webhooks/1468102419983503370/Gy8r761L4mQOPRjnWrj4Gx1jlitxgCRVIa4tjyzX_boM1P8mGc7OeuAOTPOlSpZwfnrw', role_id: '1467384983445442706', names: ['ドリームズ','Dreams Inc.','Dreams Inc'] },
};


// ── REST ──────────────────────────────────────────────────────
function rest(method, path, body) {
  return new Promise((resolve, reject) => {
    const data = body ? JSON.stringify(body) : null;
    const req  = https.request(`${API}${path}`, {
      agent: httpsAgent, method,
      headers: {
        'Authorization': `Bot ${TOKEN}`,
        'Content-Type':  'application/json',
        ...(data ? { 'Content-Length': Buffer.byteLength(data) } : {}),
      },
    }, res => {
      let raw = '';
      res.on('data', c => raw += c);
      res.on('end', () => {
        try {
          const parsed = raw ? JSON.parse(raw) : {};
          if (res.statusCode >= 400) console.error(`REST ${method} ${path} -> ${res.statusCode}:`, JSON.stringify(parsed));
          resolve(parsed);
        } catch { resolve({}); }
      });
    });
    req.on('error', reject);
    if (data) req.write(data);
    req.end();
  });
}

// Multipart REST for file uploads (used by webhook forwarding)
function restMultipart(method, path, payload, fileBuffer, filename, mimetype) {
  return new Promise((resolve, reject) => {
    const boundary = '----TrinketBotBoundary' + Date.now();
    const jsonPart = Buffer.from(
      `--${boundary}\r\nContent-Disposition: form-data; name="payload_json"\r\nContent-Type: application/json\r\n\r\n` +
      JSON.stringify(payload) + `\r\n`
    );
    const filePart = fileBuffer ? Buffer.concat([
      Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="${filename}"\r\nContent-Type: ${mimetype}\r\n\r\n`),
      fileBuffer,
      Buffer.from('\r\n'),
    ]) : Buffer.alloc(0);
    const closing = Buffer.from(`--${boundary}--\r\n`);
    const body = Buffer.concat([jsonPart, filePart, closing]);
    const req = https.request(`${API}${path}`, {
      agent: httpsAgent, method,
      headers: {
        'Authorization': `Bot ${TOKEN}`,
        'Content-Type':  `multipart/form-data; boundary=${boundary}`,
        'Content-Length': body.length,
      },
    }, res => {
      let raw = '';
      res.on('data', c => raw += c);
      res.on('end', () => { try { resolve(JSON.parse(raw)); } catch { resolve({}); } });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

function respond(id, token, type, data) {
  return rest('POST', `/interactions/${id}/${token}/callback`, { type, data });
}
function showModal(id, token, modal)        { return respond(id, token, 9, modal); }
function replyEphemeral(id, token, content) { return respond(id, token, 4, { content, flags: 64 }); }
function updateMessage(id, token, data)     { return respond(id, token, 7, data); }
// ── FIX: defer + follow-up helpers to prevent interaction timeout double-posts ──
function deferEphemeral(id, token) {
  return rest('POST', `/interactions/${id}/${token}/callback`, { type: 5, data: { flags: 64 } });
}
function followUp(token, content) {
  return rest('PATCH', `/webhooks/${appIdCache}/${token}/messages/@original`, { content });
}
// ── Permission helper ─────────────────────────────────────────
function isAdmin(member) {
  const roles = member?.roles || [];
  const perms = BigInt(member?.permissions || '0');
  return roles.includes(ADMIN_ROLE_ID) || roles.includes(BOT_ROLE_ID) || (perms & 8n) === 8n;
}
// ── Modal component builders ──────────────────────────────────
let _lid = 1;
const nextId   = () => _lid++;
function resetIds() { _lid = 1; }

function label(id, labelText, description, innerComponent) {
  return { type: 18, id, label: labelText.slice(0, 45), description: description?.slice(0, 100), component: innerComponent };
}
function textInput(id, placeholder, paragraph = false, required = true, maxLength = 200) {
  return { type: 4, custom_id: String(id), style: paragraph ? 2 : 1, placeholder, required, max_length: maxLength };
}
function stringSelect(id, placeholder, options, minValues = 1, maxValues = 1) {
  return { type: 3, custom_id: String(id), placeholder, options, min_values: minValues, max_values: maxValues };
}
function fileUpload(id, minValues = 1, maxValues = 5, required = true) {
  return { type: 19, custom_id: String(id), min_values: minValues, max_values: maxValues, required };
}
function checkboxGroup(id, options, minValues, maxValues, required = true) {
  return { type: 22, id, custom_id: `cb_${id}`, options, min_values: minValues, max_values: maxValues, required };
}
function radioGroup(id, options, required = true) {
  return { type: 21, id, custom_id: `rb_${id}`, options, required };
}

// Extract all inner components from a modal submission (walks label wrappers)
function getFields(components, resolved) {
  const fields = {};
  function walk(comps) {
    for (const c of comps || []) {
      if (c.type === 18 && c.component) {
        const comp = c.component;
        if (comp.type === 19 && comp.values && resolved?.attachments)
          comp.files = comp.values.map(id => resolved.attachments[id]).filter(Boolean);
        if (comp.custom_id) fields[comp.custom_id] = comp;
      }
      if (c.components) walk(c.components);
    }
  }
  walk(components);
  return fields;
}

// ── Item embed button row (shared between post and edit) ──────
function itemButtonRow(msgId) {
  return {
    type: 1,
    components: [
      { type: 2, style: 2, label: '✏️ Edit Item',   custom_id: `edit_item_${msgId}`   },
      { type: 2, style: 4, label: '🗑️ Remove Item', custom_id: `remove_item_${msgId}` },
    ],
  };
}

// ── Build item embed from stored/provided state ───────────────
function buildItemEmbed(username, avatarUrl, name, price, condition, notes, photoUrls) {
  const photoLinks = (photoUrls || []).map((u, i) => `[Photo ${i + 1}](${u})`).join('  ');
  return {
    color: COLOR,
    author: { name: `${username} - New Item`, icon_url: avatarUrl },
    fields: [
      { name: 'Item',      value: `**${name}** - $${price}`, inline: false },
      { name: 'Condition', value: condition,                  inline: true  },
      ...(notes      ? [{ name: 'Notes',  value: notes,      inline: false }] : []),
      ...(photoLinks ? [{ name: 'Photos', value: photoLinks, inline: false }] : []),
    ],
    image: photoUrls?.[0] ? { url: photoUrls[0] } : undefined,
    timestamp: new Date().toISOString(),
  };
}


// ============================================================
// MARKETPLACE
// ============================================================

const TRANSACTION_OPTS = [
  { label: 'Sale',   value: 'Sale'   },
  { label: 'Trade',  value: 'Trade'  },
  { label: 'Barter', value: 'Barter' },
];
const PAYMENT_OPTS = [
  { label: 'PayPal G&S', value: 'PayPal G&S' },
  { label: 'Venmo G&S',  value: 'Venmo G&S'  },
  { label: 'Other',      value: 'Other', description: 'add info in the notes' },
];
const SHIPPING_OPTS = [
  { label: 'Shipping cost included',   value: 'included'   },
  { label: 'Shipping cost additional', value: 'additional' },
];
const CONDITION_OPTS = [
  { label: 'Boxed - sealed',      value: 'Boxed - sealed'      },
  { label: 'Boxed - top open',    value: 'Boxed - top open'    },
  { label: 'Boxed - bottom open', value: 'Boxed - bottom open' },
  { label: 'Boxed - fully open',  value: 'Boxed - fully open'  },
  { label: 'Boxed - no box',      value: 'Boxed - no box'      },
  { label: 'Tagged - NWT',        value: 'Tagged - NWT'        },
  { label: 'Tagged - NWRT',       value: 'Tagged - NWRT'       },
  { label: 'Tagged - NWOT',       value: 'Tagged - NWOT'       },
  { label: 'Pre-loved',           value: 'Pre-loved'           },
  { label: 'Other',               value: 'Other'               },
];

async function buildOpenShopModal() {
  resetIds();
  const forum  = await rest('GET', `/channels/${MARKETPLACE_FORUM_ID}`);
  const tagMap = {};
  for (const t of forum.available_tags || []) tagMap[t.id] = t.name;
  const tagOpts = TAG_IDS.filter(id => tagMap[id]).map(id => ({ label: tagMap[id].slice(0, 100), value: id }));

  const l1 = nextId(), inner1 = nextId();
  const l2 = nextId(), inner2 = nextId();
  const l3 = nextId(), inner3 = nextId();
  const l4 = nextId(), inner4 = nextId();
  const l5 = nextId(), inner5 = nextId();

  return {
    title: 'Open Shop', custom_id: 'mp_open_shop',
    components: [
      label(l1, 'Transaction Types', 'Select all that apply', checkboxGroup(inner1, TRANSACTION_OPTS, 1, 3)),
      label(l2, 'Accepted Payments',  'Select all that apply', checkboxGroup(inner2, PAYMENT_OPTS, 1, 3)),
      label(l3, 'Shipping',           'Select one',            radioGroup(inner3, SHIPPING_OPTS)),
      label(l4, 'Tags',               'Select all that apply', stringSelect(inner4, 'Choose tags…', tagOpts, 1, Math.min(tagOpts.length, 25))),
      label(l5, 'General Notes (optional)', null, textInput(inner5, 'e.g. Bundle deals, location, other info, etc.', true, false, 500)),
    ],
  };
}

function buildListItemModal() {
  resetIds();
  const l1 = nextId(), inner1 = nextId();
  const l2 = nextId(), inner2 = nextId();
  const l3 = nextId(), inner3 = nextId();
  const l4 = nextId(), inner4 = nextId();
  const l5 = nextId(), inner5 = nextId();
  return {
    title: 'List Item', custom_id: 'mp_list_item',
    components: [
      label(l1, 'Item Name',                'Full name of the item',              textInput(inner1, 'e.g. Jellycat Bashful Bunny Medium', false, true, 200)),
      label(l2, 'Price (USD)',              'Numbers only - no $ symbol',          textInput(inner2, 'e.g. 35.00', false, true, 10)),
      label(l3, 'Condition',               'Select the condition that best applies', stringSelect(inner3, 'Select condition…', CONDITION_OPTS, 1, 1)),
      label(l4, 'Item Photos (1–5)',        'Upload 1 to 5 photos of this item',   fileUpload(inner4, 1, 5)),
      label(l5, 'Additional Notes (optional)', 'Flaws, details, extras',           textInput(inner5, 'e.g. Minor thread pull on ear, barely noticeable', true, false, 500)),
    ],
  };
}

function buildEditItemModal(msgId) {
  resetIds();
  const l1 = nextId(), inner1 = nextId();
  const l2 = nextId(), inner2 = nextId();
  const l3 = nextId(), inner3 = nextId();
  const l4 = nextId(), inner4 = nextId();
  const l5 = nextId(), inner5 = nextId();
  return {
    title: 'Edit Item', custom_id: `mp_edit_item_${msgId}`,
    components: [
      label(l1, 'Item Name',                'Full name of the item',              textInput(inner1, 'e.g. Jellycat Bashful Bunny Medium', false, true, 200)),
      label(l2, 'Price (USD)',              'Numbers only - no $ symbol',          textInput(inner2, 'e.g. 35.00', false, true, 10)),
      label(l3, 'Condition',               'Select the condition that best applies', stringSelect(inner3, 'Select condition…', CONDITION_OPTS, 1, 1)),
      label(l4, 'New Photos (1–5)',         'Upload replacement photos',           fileUpload(inner4, 1, 5)),
      label(l5, 'Additional Notes (optional)', 'Flaws, details, extras',           textInput(inner5, 'e.g. Minor thread pull on ear, barely noticeable', true, false, 500)),
    ],
  };
}

async function postShop(iid, token, userId, username, avatarUrl, state) {
  if (threads[userId]) {
    await rest('PATCH', `/channels/${threads[userId]}`, { archived: true, locked: true }).catch(() => {});
  }
  const forum   = await rest('GET', `/channels/${MARKETPLACE_FORUM_ID}`);
  const tagObjs = (forum.available_tags || []).reduce((m, t) => { m[t.id] = t; return m; }, {});
  const appliedTagIds = (state.tags || []).map(id => tagObjs[id]?.id).filter(Boolean).slice(0, 5);
  if (!appliedTagIds.length) return replyEphemeral(iid, token, '❌ None of the selected tags were found. Please contact an admin.');

  const shippingLabel = state.shipping === 'included' ? 'Shipping cost included' : 'Shipping cost additional';
  const embed = {
    title: `${username}'s Shop`, color: COLOR,
    author: { name: username, icon_url: avatarUrl },
    fields: [
      { name: 'Transaction Types', value: state.transactions.join(', '), inline: true },
      { name: 'Payment',           value: state.payment.join(', '),      inline: true },
      { name: 'Shipping',          value: shippingLabel,                  inline: true },
      ...(state.notes ? [{ name: 'General Notes', value: state.notes, inline: false }] : []),
    ],
    footer: { text: `Seller ID: ${userId} • Use "List Item" button to add items` },
    timestamp: new Date().toISOString(),
  };
  const result = await rest('POST', `/channels/${MARKETPLACE_FORUM_ID}/threads`, {
    name: `${username}'s Shop`,
    message: {
      content: `**<@${userId}>'s Shop**\n-# Click **List Item** below to add items to this post.`,
      embeds: [embed],
      components: [{ type: 1, components: [{ type: 2, style: 2, label: 'List Item', custom_id: 'add_listing_item' }] }],
    },
    applied_tags: appliedTagIds,
  });
  if (!result.id) return replyEphemeral(iid, token, '❌ Failed to create shop thread.');
  threads[userId]   = result.id;
  cooldowns[userId] = new Date().toISOString();
  saveJSON('threads.json',   threads);
  saveJSON('cooldowns.json', cooldowns);
  return replyEphemeral(iid, token, `✅ Your shop has been created: <#${result.id}>\nClick **List Item** in the thread to add your first item.`);
}

async function postItem(iid, token, userId, username, avatarUrl, state) {
  const threadId = threads[userId];
  if (!threadId) return replyEphemeral(iid, token, '❌ No active shop found. Open a shop first using the panel button.');

  const embed = buildItemEmbed(username, avatarUrl, state.name, state.price, state.condition, state.notes, state.photoUrls);

  // Post the message first with no buttons so there's only one API call to create it
  const msg = await rest('POST', `/channels/${threadId}/messages`, { embeds: [embed] });
  if (!msg.id) return replyEphemeral(iid, token, '❌ Failed to post item.');

  // Now patch in the buttons using the real message ID — single PATCH, no duplicate post
  await rest('PATCH', `/channels/${threadId}/messages/${msg.id}`, {
    components: [itemButtonRow(msg.id)],
  });

  // Persist item data keyed by message ID
  itemMessages[msg.id] = {
    userId, threadId, username, avatarUrl,
    name:      state.name,
    price:     state.price,
    condition: state.condition,
    notes:     state.notes || '',
    photoUrls: state.photoUrls || [],
  };
  saveJSON('item_messages.json', itemMessages);

  return replyEphemeral(iid, token, '✅ Item added to your shop!');
}

// ============================================================
// ISO MODULE
// ============================================================

let _ipCachePromise = null;
function getIpOptions() {
  if (!_ipCachePromise) {
    _ipCachePromise = Promise.all(
      IPTHREAD_IDS.map(id =>
        rest('GET', `/channels/${id}`)
          .then(ch => ({ label: (ch.name || id).slice(0, 100), value: id }))
          .catch(() => ({ label: id, value: id }))
      )
    ).then(results => {
      console.log('ISO thread names cached.');
      return results;
    }).catch(err => {
      _ipCachePromise = null;
      throw err;
    });
  }
  return _ipCachePromise;
}

function buildIsoEmbed(username, avatarUrl, content, photoUrls) {
  return {
    color: COLOR,
    author: { name: `${username}'s ISOs`, icon_url: avatarUrl },
    description: content,
    image: photoUrls?.[0] ? { url: photoUrls[0] } : undefined,
    footer: { text: 'Use "Edit Listing" to update or "Remove Listing" to delete' },
    timestamp: new Date().toISOString(),
  };
}

async function upsertIsoListing(iid, token, userId, username, avatarUrl, threadId, content, photoUrls) {
  if (!isoListings[userId]) isoListings[userId] = {};
  const existing = isoListings[userId][threadId];
  if (existing?.ts) {
    const elapsed = Date.now() - new Date(existing.ts).getTime();
    if (elapsed < BUMP_COOLDOWN_MS) {
      const hoursLeft = Math.ceil((BUMP_COOLDOWN_MS - elapsed) / 3600000);
      return replyEphemeral(iid, token,
        `❌ You can only bump your listing once every 72 hours.\nTry again in **${hoursLeft} hour${hoursLeft !== 1 ? 's' : ''}**.\nUse **Edit Listing** to update your post without bumping.`
      );
    }
    await rest('DELETE', `/channels/${threadId}/messages/${existing.messageId}`).catch(() => {});
  }
  const embed = buildIsoEmbed(username, avatarUrl, content, photoUrls);
  const msg   = await rest('POST', `/channels/${threadId}/messages`, { embeds: [embed] });
  if (!msg.id) return replyEphemeral(iid, token, '❌ Failed to post ISO. Check bot permissions in that thread.');
  isoListings[userId][threadId] = { messageId: msg.id, content, photoUrls: photoUrls || [], ts: new Date().toISOString() };
  saveJSON('iso_listings.json', isoListings);
  return replyEphemeral(iid, token, `✅ Your ISO has been posted in <#${threadId}>!`);
}

async function editIsoListing(iid, token, userId, username, avatarUrl, threadId, content) {
  const listing = isoListings[userId]?.[threadId];
  if (!listing) return replyEphemeral(iid, token, '❌ No listing found. Use "Add ISO Item" instead.');
  const embed = buildIsoEmbed(username, avatarUrl, content, listing.photoUrls);
  await rest('PATCH', `/channels/${threadId}/messages/${listing.messageId}`, { embeds: [embed] });
  listing.content = content;
  saveJSON('iso_listings.json', isoListings);
  return replyEphemeral(iid, token, '✅ Your ISO listing has been updated.');
}

async function removeIsoListing(iid, token, userId, threadId) {
  const listing = isoListings[userId]?.[threadId];
  if (!listing) return replyEphemeral(iid, token, '❌ No listing found in that thread.');
  await rest('DELETE', `/channels/${threadId}/messages/${listing.messageId}`).catch(() => {});
  delete isoListings[userId][threadId];
  saveJSON('iso_listings.json', isoListings);
  return replyEphemeral(iid, token, '✅ Your ISO listing has been removed.');
}

async function buildIsoAddModal() {
  resetIds();
  const ipOpts = await getIpOptions();
  const l1 = nextId(), inner1 = nextId();
  const l2 = nextId(), inner2 = nextId();
  const l3 = nextId(), inner3 = nextId();
  return {
    title: 'Add ISO', custom_id: 'mp_iso_submit',
    components: [
      label(l1, 'IP / Brand Category', 'Select the thread for your ISO', stringSelect(inner1, 'Select category…', ipOpts)),
      label(l2, 'What are you looking for?', 'List items, budgets, conditions, etc.',
        textInput(inner2, 'e.g.\nJellycat Bashful Bunny Medium - budget $40\nPucky Bubble Up - sealed box only', true, true, 1000)),
      label(l3, 'Photos (optional)', 'Upload up to 5 reference photos', fileUpload(inner3, 0, 5, false)),
    ],
  };
}

async function buildIsoEditModal(userId) {
  resetIds();
  const userListings  = isoListings[userId] || {};
  const activeThreads = Object.keys(userListings);
  if (!activeThreads.length) return null;
  const ipOpts = await getIpOptions();
  const opts   = ipOpts.filter(o => activeThreads.includes(o.value));
  const l1 = nextId(), inner1 = nextId();
  const l2 = nextId(), inner2 = nextId();
  return {
    title: 'Edit ISO Listing', custom_id: 'mp_iso_edit',
    components: [
      label(l1, 'Select Listing to Edit', 'Choose which IP thread to edit', stringSelect(inner1, 'Select listing…', opts)),
      label(l2, 'Updated Content', 'Replace your listing with this text',
        textInput(inner2, 'e.g.\nJellycat Bashful Bunny Medium - budget $40', true, true, 1000)),
    ],
  };
}

async function buildIsoRemoveModal(userId) {
  resetIds();
  const userListings  = isoListings[userId] || {};
  const activeThreads = Object.keys(userListings);
  if (!activeThreads.length) return null;
  const ipOpts = await getIpOptions();
  const opts   = ipOpts.filter(o => activeThreads.includes(o.value));
  const l1 = nextId(), inner1 = nextId();
  return {
    title: 'Remove ISO Listing', custom_id: 'mp_iso_remove',
    components: [
      label(l1, 'Select Listing to Remove', 'This will delete your post in that thread', stringSelect(inner1, 'Select listing…', opts)),
    ],
  };
}


// ============================================================
// MODERATION
// ============================================================

async function logModAction(guildId, action, targetId, targetName, moderatorId, reason, duration) {
  const colorMap = { mute: 0xffa500, unmute: 0x00ff00, kick: 0xff0000, ban: 0x8b0000, unban: 0x00ff00, warn: 0xffff00 };
  const embed = {
    title: `${action.charAt(0).toUpperCase() + action.slice(1)} Action`,
    color: colorMap[action] || COLOR,
    fields: [
      { name: 'Target', value: `<@${targetId}> (${targetName})`, inline: true },
      ...(duration ? [{ name: 'Duration', value: duration, inline: true }] : []),
      { name: 'Reason', value: reason || 'No reason provided', inline: false },
    ],
    footer: { text: `User ID: ${targetId} | Moderator: <@${moderatorId}>` },
    timestamp: new Date().toISOString(),
  };
  await rest('POST', `/channels/${MOD_LOG_CHANNEL_ID}/messages`, { embeds: [embed] }).catch(() => {});
}

async function dmUser(userId, embed) {
  const dm = await rest('POST', '/users/@me/channels', { recipient_id: userId }).catch(() => null);
  if (dm?.id) await rest('POST', `/channels/${dm.id}/messages`, { embeds: [embed] }).catch(() => {});
}

function parseDuration(str) {
  const map = { s: 1, m: 60, h: 3600, d: 86400 };
  const match = str?.match(/^(\d+)([smhd])$/);
  if (!match) return null;
  return parseInt(match[1]) * (map[match[2]] || 60);
}

// ============================================================
// SUPPORT TICKETS
// ============================================================

async function createTicketChannel(guildId, categoryId, channelName, userId, modRoleId) {
  const channel = await rest('POST', `/guilds/${guildId}/channels`, {
    name: channelName.slice(0, 100),
    type: 0,
    parent_id: categoryId,
    permission_overwrites: [
      { id: guildId,   type: 0, deny: '1024'  },
      { id: userId,    type: 1, allow: '52224' },
      { id: modRoleId, type: 0, allow: '52224' },
    ],
  });
  return channel;
}

// ============================================================
// EMBED BUILDER (createembed)
// ============================================================

function parseEmbedSection(text) {
  const lines = text.trim().split('\n');
  const data  = { title: null, description: null, color: COLOR, image: null, thumbnail: null, footer: null, url: null, fields: [] };
  const fieldStore = {};
  for (const raw of lines) {
    const line = raw.trim();
    if (!line || !line.includes(':')) continue;
    const [rawKey, ...rest] = line.split(':');
    const key   = rawKey.trim().toUpperCase();
    const value = rest.join(':').trim();
    if (['TITLE','T'].includes(key))           data.title       = value;
    else if (['DESC','DESCRIPTION','D'].includes(key)) data.description = value;
    else if (['COLOR','COLOUR','C'].includes(key)) { try { data.color = parseInt(value.replace('#',''), 16); } catch {} }
    else if (['IMAGE','IMG','I'].includes(key))    data.image       = value;
    else if (['THUMBNAIL','THUMB','TH'].includes(key)) data.thumbnail = value;
    else if (['FOOTER','F'].includes(key))         data.footer      = value;
    else if (['URL','U'].includes(key))            data.url         = value;
    else if (key.match(/^FN(AME)?(\d+)$/)) {
      const n = parseInt(key.replace(/^FN(AME)?/,''));
      if (n >= 1 && n <= 25) { fieldStore[n] = fieldStore[n] || {}; fieldStore[n].name = value; }
    } else if (key.match(/^FV(ALUE)?(\d+)$/)) {
      const n = parseInt(key.replace(/^FV(ALUE)?/,''));
      if (n >= 1 && n <= 25) { fieldStore[n] = fieldStore[n] || {}; fieldStore[n].value = value; }
    } else if (key.match(/^FI(NLINE)?(\d+)$/)) {
      const n = parseInt(key.replace(/^FI(NLINE)?/,''));
      if (n >= 1 && n <= 25) { fieldStore[n] = fieldStore[n] || {}; fieldStore[n].inline = ['true','yes','1','t','y'].includes(value.toLowerCase()); }
    }
  }
  for (const n of Object.keys(fieldStore).sort((a,b) => a-b)) {
    const f = fieldStore[n];
    if (f.name && f.value) data.fields.push({ name: f.name.slice(0,256), value: f.value.slice(0,1024), inline: f.inline !== false });
  }
  return data;
}

function buildDiscordEmbed(data) {
  const embed = { color: data.color };
  if (data.title)       embed.title       = data.title;
  if (data.description) embed.description = data.description;
  if (data.url)         embed.url         = data.url;
  if (data.footer)      embed.footer      = { text: data.footer };
  if (data.image)       embed.image       = { url: data.image };
  if (data.thumbnail)   embed.thumbnail   = { url: data.thumbnail };
  if (data.fields?.length) embed.fields   = data.fields;
  return embed;
}

function parseComponentLine(line, row, guild) {
  const parts = line.split('|').map(p => p.trim());
  const type  = parts[0].toLowerCase();

  if (type === 'button' && parts.length >= 3) {
    const btnLabel = parts[1].slice(0, 80);
    const btnType  = parts[2];
    const btnValue = parts[3] || null;
    const emoji    = parts[4] || null;
    return { kind: 'button', label: btnLabel, btnType, value: btnValue, emoji, row };
  }
  if (type === 'dropdown' && parts.length >= 3) {
    const placeholder = parts[1].slice(0, 100);
    const opts = parts[2].split(',').map(o => o.trim()).filter(Boolean).slice(0, 25)
      .map(o => ({ label: o.slice(0, 100), value: o.toLowerCase().replace(/\s+/g,'_').slice(0, 100) }));
    return { kind: 'dropdown', placeholder, opts, row, min: parseInt(parts[3]||'1'), max: parseInt(parts[4]||'1') };
  }
  if (type === 'roledropdown' && parts.length >= 3) {
    const placeholder = parts[1].slice(0, 100);
    const roleIds = parts[2].split(',').map(r => r.trim().replace(/^<@&|>$/g, '')).filter(Boolean);
    return { kind: 'roledropdown', placeholder, roleIds, row, min: parseInt(parts[3]||'1'), max: parseInt(parts[4]||'1') };
  }
  return null;
}

function buildComponentRow(comp) {
  if (comp.kind === 'button') {
    const styleMap = { primary: 1, secondary: 2, success: 3, danger: 4, link: 5, role: 2, template: 1 };
    const style = styleMap[comp.btnType] || 2;
    const btn = { type: 2, style, label: comp.label };
    if (comp.emoji) btn.emoji = { name: comp.emoji };
    if (comp.btnType === 'link') { btn.url = comp.value; }
    else { btn.custom_id = `btn_${comp.btnType}_${comp.value || comp.label}`.slice(0, 100); }
    return btn;
  }
  if (comp.kind === 'dropdown') {
    return { type: 3, custom_id: `dd_${comp.placeholder.slice(0,20)}_${Date.now()}`, placeholder: comp.placeholder, options: comp.opts, min_values: comp.min, max_values: Math.min(comp.max, comp.opts.length) };
  }
  if (comp.kind === 'roledropdown') {
    const opts = comp.roleIds.map(id => ({ label: id, value: id }));
    return { type: 3, custom_id: `rd_${comp.placeholder.slice(0,20)}_${Date.now()}`, placeholder: comp.placeholder, options: opts, min_values: comp.min, max_values: Math.min(comp.max, opts.length) };
  }
  return null;
}


// ============================================================
// STICKY MESSAGES
// ============================================================

async function repostSticky(channelId) {
  const sticky = stickyData[channelId];
  if (!sticky) return;
  if (sticky.messageId) {
    await rest('DELETE', `/channels/${channelId}/messages/${sticky.messageId}`).catch(() => {});
  }
  const embed = buildDiscordEmbed(sticky.embed);
  const components = sticky.components?.length ? [{ type: 1, components: sticky.components }] : undefined;
  const msg = await rest('POST', `/channels/${channelId}/messages`, {
    content: sticky.content || undefined,
    embeds: [embed],
    ...(components ? { components } : {}),
  });
  if (msg.id) {
    stickyData[channelId].messageId = msg.id;
    saveJSON('sticky.json', stickyData);
  }
}

// ============================================================
// SCHEDULED MESSAGES
// ============================================================

async function checkScheduled() {
  const now  = Date.now();
  const keep = [];
  for (const item of scheduled) {
    if (now >= new Date(item.sendAt).getTime()) {
      const embed = buildDiscordEmbed(item.embed);
      const msg   = await rest('POST', `/channels/${item.channelId}/messages`, { embeds: [embed] }).catch(() => null);
      if (msg?.id && item.components?.length) {
        embedButtons[msg.id] = item.components;
        saveJSON('embed_buttons.json', embedButtons);
      }
    } else {
      keep.push(item);
    }
  }
  if (keep.length !== scheduled.length) {
    scheduled.length = 0;
    scheduled.push(...keep);
    saveJSON('scheduled.json', scheduled);
  }
}

// ============================================================
// YOUTUBE AUTO-FORWARD
// ============================================================

async function handleYouTubeForward(messageContent) {
  const urlMatch = messageContent.match(/https?:\/\/(?:www\.)?youtube\.com\/watch\?v=[\w-]+/);
  if (!urlMatch) return;
  const url = urlMatch[0];

  let channelName = null;
  const uploadedMatch = messageContent.match(/(\S+(?:\s+\S+)*?)\s+uploaded a new youtube video/i);
  if (uploadedMatch) channelName = uploadedMatch[1].trim();
  if (!channelName) {
    const lines = messageContent.split('\n');
    for (let i = 0; i < lines.length; i++) {
      if (lines[i].trim().toLowerCase() === 'youtube' && lines[i+1]) {
        channelName = lines[i+1].trim(); break;
      }
    }
  }
  if (!channelName) return;

  let cfg = null, matchedKey = null;
  const lower = messageContent.toLowerCase();
  for (const [kw, config] of Object.entries(YOUTUBE_WEBHOOKS)) {
    if (lower.includes(kw) || channelName.toLowerCase().includes(kw)) { cfg = config; matchedKey = kw; break; }
    if (config.names.some(n => lower.includes(n.toLowerCase()) || channelName.toLowerCase().includes(n.toLowerCase()))) { cfg = config; matchedKey = kw; break; }
  }
  if (!cfg) return;

  let title = null;
  const lines = messageContent.split('\n');
  for (let i = 0; i < lines.length; i++) {
    if (lines[i].trim() === channelName && lines[i+1]) { title = lines[i+1].trim(); break; }
  }
  if (!title || title.toLowerCase() === 'youtube') title = `New video from ${channelName}`;

  const formatted = `## ${title}\n[via ${channelName}](${url})  |  <@&${cfg.role_id}>`;

  await new Promise((resolve) => {
    const body = JSON.stringify({ content: formatted, allowed_mentions: { roles: [cfg.role_id] } });
    const urlObj = new URL(cfg.webhook);
    const req = https.request({ hostname: urlObj.hostname, path: urlObj.pathname + urlObj.search, method: 'POST', headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) } }, res => { res.resume(); res.on('end', resolve); });
    req.on('error', resolve);
    req.write(body); req.end();
  });
  console.log(`Forwarded YouTube: ${channelName} (${matchedKey})`);
}
// ============================================================
// SLASH COMMAND DEFINITIONS
// ============================================================

const COMMANDS = [
  { name: 'setup_market',   description: 'Post the marketplace panel',   default_member_permissions: '32' },
  { name: 'setup_bazaar',  description: 'Post the Bazaar New & Noteworthy forum thread', default_member_permissions: '32' },
  { name: 'setup_iso',      description: 'Post the ISO panel',            default_member_permissions: '32' },
  { name: 'setup_brownie',  description: 'Setup brownie points button',   default_member_permissions: '32' },
  { name: 'setup_support',  description: 'Setup support ticket panel',    default_member_permissions: '32' },
  { name: 'syncguild',      description: 'Clear guild commands and re-sync globally', default_member_permissions: '8' },
  { name: 'help',           description: 'View all available commands' },
  {
    name: 'createembed', description: 'Create an embed with optional components',
    default_member_permissions: '32',
    options: [
      { name: 'channel', description: 'Channel to send embed to', type: 7, required: true },
      { name: 'content', description: 'Message content outside embed (optional)', type: 3, required: false },
    ],
  },
  {
    name: 'createsticky', description: 'Create a sticky embed in a channel',
    default_member_permissions: '32',
    options: [{ name: 'channel', description: 'Channel to place sticky in', type: 7, required: true }],
  },
  {
    name: 'removesticky', description: 'Remove sticky embed from a channel',
    default_member_permissions: '32',
    options: [{ name: 'channel', description: 'Channel to remove sticky from', type: 7, required: true }],
  },
  {
    name: 'saychannel', description: 'Send a message to a specific channel',
    default_member_permissions: '32',
    options: [{ name: 'channel', description: 'Channel to send to', type: 7, required: true }],
  },
  {
    name: 'saythread', description: 'Send a message to a thread or forum post',
    default_member_permissions: '32',
    options: [{ name: 'thread_id', description: 'Thread ID to send to', type: 3, required: true }],
  },
  {
    name: 'sayreply', description: 'Reply to a specific message',
    default_member_permissions: '32',
    options: [
      { name: 'message_id',  description: 'ID of message to reply to', type: 3, required: true },
      { name: 'channel_id',  description: 'Channel ID (defaults to current channel)', type: 3, required: false },
    ],
  },
  {
    name: 'mute', description: 'Timeout/mute a user', default_member_permissions: '32',
    options: [
      { name: 'user',     description: 'User to mute', type: 6, required: true },
      { name: 'duration', description: 'Duration (e.g. 10m, 2h, 1d)', type: 3, required: false },
      { name: 'reason',   description: 'Reason', type: 3, required: false },
    ],
  },
  {
    name: 'unmute', description: 'Remove timeout from a user', default_member_permissions: '32',
    options: [
      { name: 'user',   description: 'User to unmute', type: 6, required: true },
      { name: 'reason', description: 'Reason', type: 3, required: false },
    ],
  },
  {
    name: 'warn', description: 'Warn a user', default_member_permissions: '32',
    options: [
      { name: 'user',   description: 'User to warn', type: 6, required: true },
      { name: 'reason', description: 'Reason', type: 3, required: true },
    ],
  },
  {
    name: 'kick', description: 'Kick a user', default_member_permissions: '32',
    options: [
      { name: 'user',   description: 'User to kick', type: 6, required: true },
      { name: 'reason', description: 'Reason', type: 3, required: false },
    ],
  },
  {
    name: 'ban', description: 'Ban a user', default_member_permissions: '32',
    options: [
      { name: 'user',   description: 'User to ban', type: 6, required: true },
      { name: 'reason', description: 'Reason', type: 3, required: false },
    ],
  },
  {
    name: 'unban', description: 'Unban a user by ID', default_member_permissions: '32',
    options: [
      { name: 'user_id', description: 'User ID to unban', type: 3, required: true },
      { name: 'reason',  description: 'Reason', type: 3, required: false },
    ],
  },
  {
    name: 'purge', description: 'Delete multiple messages (1–100)', default_member_permissions: '32',
    options: [{ name: 'amount', description: 'Number of messages to delete', type: 4, required: true }],
  },
  {
    name: 'detain', description: 'Detain a user for moderator review', default_member_permissions: '32',
    options: [
      { name: 'user',   description: 'User to detain', type: 6, required: true },
      { name: 'reason', description: 'Reason', type: 3, required: false },
    ],
  },
  {
    name: 'release', description: 'Release a detained user', default_member_permissions: '32',
    options: [
      { name: 'user',   description: 'User to release', type: 6, required: true },
      { name: 'reason', description: 'Reason', type: 3, required: false },
    ],
  },
  {
    name: 'scheduled', description: 'View scheduled messages', default_member_permissions: '32',
  },
  {
    name: 'createforum', description: 'Create a new forum post', default_member_permissions: '32',
    options: [
      { name: 'forum_id', description: 'Forum channel ID', type: 3, required: true },
      { name: 'title',    description: 'Post title', type: 3, required: true },
    ],
  },
  {
    name: 'componenthelp', description: 'Show detailed component formatting guide', default_member_permissions: '32',
  },
];

async function registerCommands(appId) {
  try {
    await rest('PUT', `/applications/${appId}/commands`, COMMANDS);
    console.log(`Registered ${COMMANDS.length} slash commands.`);
  } catch (e) {
    console.error('Command registration failed:', e.message);
  }
}


// ============================================================
// MODAL DEFINITIONS
// ============================================================

const MODAL_EMBED = {
  title: 'Create Embed(s)', custom_id: 'modal_createembed',
  components: [
    { type: 1, components: [{ type: 4, custom_id: 'embed_content', label: 'Embed Content (use === for multiple)', style: 2, placeholder: 'TITLE: My Title\nDESC: Description\nCOLOR: #e0ad76\n===\nTITLE: Second Embed', required: true, max_length: 4000 }] },
    { type: 1, components: [{ type: 4, custom_id: 'components_config', label: 'Components (optional)', style: 2, placeholder: 'button|Label|role|@Role\nroledropdown|Placeholder|ID1,ID2\ndropdown|Text|A,B,C', required: false, max_length: 2000 }] },
  ],
};

const MODAL_STICKY = {
  title: 'Create Sticky Message', custom_id: 'modal_createsticky',
  components: [
    { type: 1, components: [{ type: 4, custom_id: 'embed_content', label: 'Embed Content', style: 2, placeholder: 'TITLE: My Title\nDESC: Description\nCOLOR: #e0ad76', required: true, max_length: 3000 }] },
    { type: 1, components: [{ type: 4, custom_id: 'plain_content',  label: 'Plain text (optional)', style: 2, placeholder: 'Optional message above the embed', required: false, max_length: 2000 }] },
    { type: 1, components: [{ type: 4, custom_id: 'components_config', label: 'Components (optional)', style: 2, placeholder: 'button|Label|role|@Role\nroledropdown|Placeholder|ID1,ID2', required: false, max_length: 2000 }] },
  ],
};

const MODAL_SAY = {
  title: 'Send Message', custom_id: 'modal_say',
  components: [
    { type: 1, components: [{ type: 4, custom_id: 'message_content', label: 'Message', style: 2, required: true, max_length: 2000 }] },
    { type: 1, components: [{ type: 4, custom_id: 'image_url', label: 'Image URL (optional)', style: 2, required: false, max_length: 500 }] },
  ],
};

const MODAL_FORUM = {
  title: 'Create Forum Post', custom_id: 'modal_createforum',
  components: [
    { type: 1, components: [{ type: 4, custom_id: 'post_content', label: 'Post Content', style: 2, required: true, max_length: 2000 }] },
  ],
};

const MODAL_REPORT = {
  title: 'Report User/Incident', custom_id: 'modal_report',
  components: [
    { type: 1, components: [{ type: 4, custom_id: 'incident_summary', label: 'Incident Summary', style: 2, placeholder: 'Describe what happened...', required: true, max_length: 2000 }] },
    { type: 1, components: [{ type: 4, custom_id: 'media_links', label: 'Media Links (optional)', style: 2, placeholder: 'Paste image/video URLs (one per line)', required: false, max_length: 1000 }] },
  ],
};

const pendingState = {};


// ============================================================
// INTERACTION HANDLER
// ============================================================

async function handleInteraction(d) {
  const { id, token, type, data, member, guild_id, channel_id } = d;

  // ── FIX: instance lock prevents Autoscale duplicate handling ──
  if (!acquireInstanceLock()) return;

  const userId     = member?.user?.id       || d.user?.id;
  const username   = member?.user?.username || d.user?.username;
  const avatarHash = member?.user?.avatar   || d.user?.avatar;
  const avatarUrl  = avatarHash
    ? `https://cdn.discordapp.com/avatars/${userId}/${avatarHash}.png`
    : 'https://cdn.discordapp.com/embed/avatars/0.png';

  const opt = (name) => data.options?.find(o => o.name === name)?.value;

  try {

    // ── SLASH COMMANDS (type 2) ────────────────────────────────

    if (type === 2) {
      if (!isAdmin(member)) return replyEphemeral(id, token, "❌ You don't have permission.");
      const name = data.name;

      if (name === 'setup_bazaar') {
      // ── Bazaar New & Noteworthy ───────────────────────────────────
      const BAZAAR_CHANNEL_ID = MARKETPLACE_FORUM_ID;
      const embed = {
        title: 'Opening Shop',
        color: COLOR,
        image: { url: 'https://i.postimg.cc/nLWwfYWs/trinketbot-house-of-trinkets-22926.png' },
        fields: [
          {
            name:   'Click "Open Shop" below and complete the initial form.',
            value:  '\nGeneral shop info like if you\'re open to trades, or if shipping is included in your listed pricing.',
            inline: false,
          },
          {
            name:   'Navigate to your new shop and click "Add Items," then complete the form for each item you\'re selling.',
            value:  '\nLike items may be grouped under one listing. If an item differs in price, condition, etc., it should be in a separate listing. For example: 2 BIE for $28 each can be grouped. 1 BIE for $28 and 1 BIE for $30 can *not* be grouped.',
            inline: false,
          },
          {
            name:   'Add, remove, or edit your items at any time via the panel.',
            value:  '\nMake sure sold or otherwise unavailable items are removed in a timely manner.',
            inline: false,
          },
          {
            name:   'Recently Added Feed',
            value:  '\nThe recently added feed here is a quick way for buyers to browse new bazaar items. Recently added items will be periodically featured, so don\'t forget to check in!',
            inline: false,
          },
          {
            name:   '\nGeneral Info',
            value:  '\nMake sure prices are within the specified markup caps. Moderators may ask you to edit or remove certain listings. All item photos must include a handwritten note with your username, server name, and today\'s date.',
            inline: false,
          },
          { name: ' ',                       value: ' ',                                                                           inline: false },
          { name: '\nPrice Markup Limits',    value: ' ',                                                                           inline: false },
          { name: 'Current\nStandard Items', value: '+$10   if $1–$49 Retail\n+$15   if $50–$99 Retail\n+$20   if $100+ Retail', inline: true  },
          { name: 'Secrets, Exclusives,\n& Retired Items', value: '*Market value*\nCheck sale history on eBay and StockX', inline: true  },
          { name: ' ',                       value: ' ',                                                                           inline: false },
        ],
      };

        const result = await rest('POST', `/channels/${BAZAAR_CHANNEL_ID}/threads`, {
            name: 'New & Noteworthy',
            message: {
              embeds: [embed],
              components: [{ type: 1, components: [
                { type: 2, style: 2, label: 'Open Shop',          custom_id: 'create_marketplace_listing' },
                { type: 2, style: 2, label: 'Jump to My Shop',    custom_id: 'jump_to_my_shop'             },
                { type: 2, style: 2, label: 'Give Brownie Points', custom_id: 'rep_button' },
              ]}],
            },
          });
          return result.id
            ? replyEphemeral(id, token, `✅ Bazaar pin created: <#${result.id}>`)
            : replyEphemeral(id, token, '❌ Failed to create bazaar pin thread.');
    }

    // /setup_market
    if (name === 'setup_market') {
      const panelEmbed1 = {
        color: COLOR,
        title: 'Welcome to the HOT Bazaar!',
        description: " ",
        fields: [
          { name: 'Looking to Buy?', value: 'Each member has their own shop thread in ⁠**<#1466105963621777572>**. Use the search bar to look for specific items, the tags to search by IP, or just browse at your leisure!\n', inline: false },
          { name: 'On a quest for a particular item?', value: 'Visit the **⁠⁠<#1466146126330597591> channel**! It\'s separated by IP to keep buyers from getting lost, and to help sellers find buyers.\n', inline: false },
          { name: 'Have wares to sell?', value: 'Click the **Open Shop** button below to create your own shop thread. When you click the button, a form will pop up to collect basic shop info; you can add as many items as you want, and they can be updated at any time using the buttons in your shop. *Please be mindful of price caps.*', inline: false },
          { name: ' ',                       value: ' ',                                                                           inline: false },
          { name: "\nISO Guide for:", value: " ", inline: false },
          { name: 'Searchers', value: 'Visit the pinned post in the ISO channel to add your items. Complete one form for each IP you have ISOs from.', inline: true },
          { name: 'Sellers', value: 'Visit ISO the channel and find the IP you have available. Search or scroll through the comments to find a match!', inline: true },
          { name: ' ',                       value: ' ',                                                                           inline: false },
          { name: "\nISO Notification System", value: "If a seller has an item on someone's ISO list, they can react to the message with the 🛎️ emoji. This will notify the buyer of the potential match!\n", inline: false },
          { name: ' ',                       value: ' ',                                                                           inline: false },],
        footer: { text: 'Successfully completed a transaction? Give that user a brownie point!' },
      };
      const result = await rest('POST', `/channels/${MARKETPLACE_PANEL_ID}/messages`, {
        embeds: [panelEmbed1],
        components: [{ type: 1, components: [
          { type: 2, style: 2, label: 'Open Shop',          custom_id: 'create_marketplace_listing' },
          { type: 2, style: 2, label: 'Jump to My Shop',    custom_id: 'jump_to_my_shop'             },
          { type: 2, style: 2, label: 'Give Brownie Points', custom_id: 'rep_button' },
        ]}],
      });
      return result.id
        ? replyEphemeral(id, token, `✅ Marketplace panel posted in <#${MARKETPLACE_PANEL_ID}>!`)
        : replyEphemeral(id, token, '❌ Failed to post panel.');
    }

      // /setup_iso
      if (name === 'setup_iso') {
        const panelEmbed = {
          color: COLOR, title: 'List your ISOs',
          description: [
            "HOT ISOs are organized by brand and/or IP. Click the button below to post your listing and hopefully someone will have that item to sell to you soon!\n",
            "**Remember, it's against server rules to DM someone without mutual consent first.**  Utilize the auto-notification features or ask in the thread before DMing.  Need more of a rule refresher? Visit the <#1475232032765509874>.", '',
            '**Buyers:**',
            "• Complete the item form for each IP you have an ISO from. Be as specific as possible and include any budget or condition preferences!",
            "• Lists can be edited at any time - try to keep your ISOs fairly current",
            '**Sellers:**',
            "• Search or scroll through the comments to see if anyone is looking for one of your available items. If you come across a match, react to their listing with 🛎️ to automatically notify them in your shop!",
            "• Please do not spam this system - you should assume they're not interested if they don't respond.",
          ].join('\n'),
        };
        const result = await rest('POST', `/channels/${ISO_FORUM_ID}/threads`, {
          name: 'HOW TO ISO',
          message: {
            embeds: [panelEmbed],
            components: [{ type: 1, components: [
              { type: 2, style: 2, label: 'Add ISO Item',   custom_id: 'add_iso_item'    },
              { type: 2, style: 2, label: 'Edit Listing',   custom_id: 'edit_iso_item'   },
              { type: 2, style: 2, label: 'Remove Listing', custom_id: 'remove_iso_item' },
            ]}],
          },
        });
        return result.id
          ? replyEphemeral(id, token, `✅ ISO panel created: <#${result.id}>`)
          : replyEphemeral(id, token, '❌ Failed to create ISO panel.');
      }

      // /setup_brownie
      if (name === 'setup_brownie') {
        const embed = { title: 'Brownie Points Bazaar', description: 'Click the button below to give brownie points to a user!', color: 0x8b4513 };
        await rest('POST', `/channels/${REP_CHANNEL_ID}/messages`, {
          embeds: [embed],
          components: [{ type: 1, components: [{ type: 2, style: 2, label: 'Give Brownie Points', custom_id: 'rep_button' }] }],
        });
        return replyEphemeral(id, token, `✅ Brownie points button created in <#${REP_CHANNEL_ID}>!`);
      }

      // /setup_support
      if (name === 'setup_support') {
        const embed = {
          title: 'Support Center', color: COLOR,
          description: 'Click a button below to get help or report an issue.',
          fields: [
            { name: 'General Support',      value: 'Need help or found a bug? Click General Support to open a ticket.', inline: false },
            { name: 'Report User/Incident', value: 'Have a concern about a member or an incident? Click Report.',       inline: false },
          ],
        };
        await rest('POST', `/channels/${SUPPORT_PANEL_CHANNEL_ID}/messages`, {
          embeds: [embed],
          components: [{ type: 1, components: [
            { type: 2, style: 2, label: 'General Support',      custom_id: 'general_ticket'  },
            { type: 2, style: 4, label: 'Report User/Incident', custom_id: 'report_incident' },
          ]}],
        });
        return replyEphemeral(id, token, '✅ Support panel created!');
      }

      // /syncguild
      if (name === 'syncguild') {
        await rest('PUT', `/applications/${data.application_id || appIdCache}/guilds/${guild_id}/commands`, []);
        await registerCommands(appIdCache);
        return replyEphemeral(id, token, `✅ Cleared guild commands and re-synced ${COMMANDS.length} global commands.`);
      }

      // /help
      if (name === 'help') {
        const embed = {
          title: 'TrinketBot Commands', color: COLOR,
          fields: [
            { name: 'Setup',       value: '`/setup_market` `/setup_bazaar` `/setup_iso` `/setup_brownie` `/setup_support`', inline: false },
            { name: 'Embeds',      value: '`/createembed` `/componenthelp` `/createsticky` `/removesticky`', inline: false },
            { name: 'Messages',    value: '`/saychannel` `/saythread` `/sayreply` `/createforum`', inline: false },
            { name: 'Moderation',  value: '`/mute` `/unmute` `/warn` `/kick` `/ban` `/unban` `/purge` `/detain` `/release`', inline: false },
            { name: 'Admin',       value: '`/scheduled` `/syncguild`', inline: false },
            { name: 'Features',    value: '🛎️ ISO Reactions · ⭐ Starboard · Sticky Messages · Brownie Points · Support Tickets · Marketplace', inline: false },
          ],
          footer: { text: 'TrinketBot | Haus of Trinkets' },
        };
        return respond(id, token, 4, { embeds: [embed], flags: 64 });
      }

      // /componenthelp
      if (name === 'componenthelp') {
        const embed = {
          title: 'Embed & Component Guide', color: COLOR,
          fields: [
            { name: 'Embed Format', value: '```\nTITLE: My Embed Title\nDESC: Description\nCOLOR: #e0ad76\nIMAGE: https://...\nFOOTER: Footer text\nFNAME1: Field\nFVALUE1: Value\nFINLINE1: true```', inline: false },
            { name: 'Multiple Embeds', value: 'Separate with `===`', inline: false },
            { name: 'Buttons',  value: '```\nbutton|Label|primary\nbutton|Get Role|role|ROLE_ID\nbutton|Visit|link|https://url```', inline: false },
            { name: 'Dropdowns', value: '```\ndropdown|Placeholder|Opt1,Opt2,Opt3\nroledropdown|Pick a role|ID1,ID2```', inline: false },
            { name: 'Row prefix (optional)', value: 'Prefix line with row number: `0|button|...`', inline: false },
          ],
          footer: { text: 'Labels ≤80 chars | Max 10 embeds per message' },
        };
        return respond(id, token, 4, { embeds: [embed], flags: 64 });
      }

      // /createembed
      if (name === 'createembed') {
        const targetChannelId = opt('channel');
        const content         = opt('content') || null;
        pendingState[userId]  = { cmd: 'createembed', channelId: targetChannelId, content };
        return showModal(id, token, MODAL_EMBED);
      }

      // /createsticky
      if (name === 'createsticky') {
        const targetChannelId = opt('channel');
        if (stickyData[targetChannelId]) return replyEphemeral(id, token, `⚠️ A sticky already exists in <#${targetChannelId}>. Use \`/removesticky\` first.`);
        pendingState[userId] = { cmd: 'createsticky', channelId: targetChannelId };
        return showModal(id, token, MODAL_STICKY);
      }

      // /removesticky
      if (name === 'removesticky') {
        const targetChannelId = opt('channel');
        const sticky = stickyData[targetChannelId];
        if (!sticky) return replyEphemeral(id, token, `❌ No sticky found in <#${targetChannelId}>.`);
        if (sticky.messageId) await rest('DELETE', `/channels/${targetChannelId}/messages/${sticky.messageId}`).catch(() => {});
        delete stickyData[targetChannelId];
        saveJSON('sticky.json', stickyData);
        return replyEphemeral(id, token, `✅ Sticky removed from <#${targetChannelId}>.`);
      }

      // /saychannel
      if (name === 'saychannel') {
        const targetChannelId = opt('channel');
        pendingState[userId] = { cmd: 'say', channelId: targetChannelId };
        return showModal(id, token, MODAL_SAY);
      }

      // /saythread
      if (name === 'saythread') {
        const threadId2 = opt('thread_id');
        pendingState[userId] = { cmd: 'say', channelId: threadId2 };
        return showModal(id, token, MODAL_SAY);
      }

      // /sayreply
      if (name === 'sayreply') {
        const msgId  = opt('message_id');
        const chanId = opt('channel_id') || channel_id;
        pendingState[userId] = { cmd: 'sayreply', channelId: chanId, messageId: msgId };
        return showModal(id, token, { ...MODAL_SAY, custom_id: 'modal_sayreply', title: 'Reply to Message' });
      }

      // /createforum
      if (name === 'createforum') {
        const forumId2 = opt('forum_id');
        const title2   = opt('title');
        pendingState[userId] = { cmd: 'createforum', forumId: forumId2, title: title2 };
        return showModal(id, token, MODAL_FORUM);
      }

      // /scheduled
      if (name === 'scheduled') {
        if (!scheduled.length) return replyEphemeral(id, token, 'No scheduled messages.');
        const fields = scheduled.slice(0, 10).map((item, i) => ({
          name: `${i+1}. ${item.embed?.title || 'Untitled'}`,
          value: `**Channel:** <#${item.channelId}>\n**Time:** ${new Date(item.sendAt).toUTCString()}`,
          inline: false,
        }));
        return respond(id, token, 4, { embeds: [{ title: 'Scheduled Messages', color: COLOR, fields }], flags: 64 });
      }

      // /mute
      if (name === 'mute') {
        const targetId  = opt('user');
        const durStr    = opt('duration') || '10m';
        const reason    = opt('reason') || 'No reason provided';
        const secs      = parseDuration(durStr);
        if (!secs) return replyEphemeral(id, token, '❌ Invalid duration. Use e.g. 10m, 2h, 1d');
        const until = new Date(Date.now() + secs * 1000).toISOString();
        await rest('PATCH', `/guilds/${guild_id}/members/${targetId}`, { communication_disabled_until: until });
        await rest('PUT',   `/guilds/${guild_id}/members/${targetId}/roles/${MUTED_ROLE_ID}`).catch(() => {});
        await dmUser(targetId, { title: 'You Have Been Muted', color: 0xffa500, fields: [{ name: 'Duration', value: durStr, inline: true }, { name: 'Reason', value: reason, inline: false }] });
        await logModAction(guild_id, 'mute', targetId, 'user', userId, reason, durStr);
        return replyEphemeral(id, token, `✅ <@${targetId}> muted for ${durStr}.`);
      }

      // /unmute
      if (name === 'unmute') {
        const targetId = opt('user');
        const reason   = opt('reason') || 'No reason provided';
        await rest('PATCH', `/guilds/${guild_id}/members/${targetId}`, { communication_disabled_until: null });
        await rest('DELETE', `/guilds/${guild_id}/members/${targetId}/roles/${MUTED_ROLE_ID}`).catch(() => {});
        await dmUser(targetId, { title: 'You Have Been Unmuted', color: 0x00ff00, fields: [{ name: 'Reason', value: reason }] });
        await logModAction(guild_id, 'unmute', targetId, 'user', userId, reason);
        return replyEphemeral(id, token, `✅ <@${targetId}> unmuted.`);
      }

      // /warn
      if (name === 'warn') {
        const targetId = opt('user');
        const reason   = opt('reason');
        if (!warnings[targetId]) warnings[targetId] = [];
        warnings[targetId].push({ ts: new Date().toISOString(), reason, mod: userId });
        saveJSON('warnings.json', warnings);
        const total  = warnings[targetId].length;
        const recent = warnings[targetId].filter(w => Date.now() - new Date(w.ts).getTime() < 7 * 86400000).length;
        await dmUser(targetId, { title: 'You Have Been Warned', color: 0xffff00, fields: [{ name: 'Reason', value: reason }, { name: 'Total Warnings', value: String(total), inline: true }, { name: 'Last 7 Days', value: String(recent), inline: true }] });
        await logModAction(guild_id, 'warn', targetId, 'user', userId, reason);

        if (total >= 5) {
          const archiveContent = `**${targetId} has been auto-detained after 5 warnings.**\n\nLatest: ${reason}`;
          const threadMsg = await rest('POST', `/channels/${THREAD_CHANNEL_ID}/messages`, { content: archiveContent });
          if (threadMsg.id) {
            const thread = await rest('POST', `/channels/${THREAD_CHANNEL_ID}/messages/${threadMsg.id}/threads`, { name: `Review - ${targetId}`, auto_archive_duration: 1440 });
            if (thread.id) {
              await rest('PUT',  `/channels/${thread.id}/thread-members/${targetId}`).catch(() => {});
              await rest('PUT',  `/guilds/${guild_id}/members/${targetId}/roles/${DETAINED_ROLE_ID}`).catch(() => {});
              await rest('POST', `/channels/${thread.id}/messages`, { content: `<@${targetId}>, your access is pending moderator review.\n<@&${MOD_ROLE_ID}> - new auto-detention.` });
              await rest('POST', `/channels/${RECEIPT_CHANNEL_ID}/messages`, { content: `**AUTO-DETENTION:** <@${targetId}> reached 5 warnings.\nLatest: ${reason}\nThread: <#${thread.id}>` });
              return replyEphemeral(id, token, `✅ <@${targetId}> warned (warning #${total}). **Auto-detained.** Thread: <#${thread.id}>`);
            }
          }
        }
        let reply = `✅ <@${targetId}> warned (warning #${total}).`;
        if (recent >= 2) reply += `\n⚠️ ${recent} warnings in last 7 days.`;
        return replyEphemeral(id, token, reply);
      }

      // /kick
      if (name === 'kick') {
        const targetId = opt('user');
        const reason   = opt('reason') || 'No reason provided';
        await rest('DELETE', `/guilds/${guild_id}/members/${targetId}`, { reason });
        await logModAction(guild_id, 'kick', targetId, 'user', userId, reason);
        return replyEphemeral(id, token, `✅ <@${targetId}> kicked.`);
      }

      // /ban
      if (name === 'ban') {
        const targetId = opt('user');
        const reason   = opt('reason') || 'No reason provided';
        await dmUser(targetId, { title: 'You Have Been Banned', color: 0x8b0000, fields: [{ name: 'Server', value: guild_id }, { name: 'Reason', value: reason }] });
        await rest('PUT', `/guilds/${guild_id}/bans/${targetId}`, { reason });
        await logModAction(guild_id, 'ban', targetId, 'user', userId, reason);
        return replyEphemeral(id, token, `✅ <@${targetId}> banned.`);
      }

      // /unban
      if (name === 'unban') {
        const targetId = opt('user_id');
        const reason   = opt('reason') || 'No reason provided';
        await rest('DELETE', `/guilds/${guild_id}/bans/${targetId}`);
        await logModAction(guild_id, 'unban', targetId, targetId, userId, reason);
        return replyEphemeral(id, token, `✅ <@${targetId}> unbanned.`);
      }

      // /purge
      if (name === 'purge') {
        const amount = Math.min(Math.max(parseInt(opt('amount')), 1), 100);
        const msgs   = await rest('GET', `/channels/${channel_id}/messages?limit=${amount}`);
        if (!Array.isArray(msgs) || !msgs.length) return replyEphemeral(id, token, '❌ No messages found.');
        const ids = msgs.map(m => m.id);
        if (ids.length === 1) await rest('DELETE', `/channels/${channel_id}/messages/${ids[0]}`);
        else await rest('POST', `/channels/${channel_id}/messages/bulk-delete`, { messages: ids });
        return replyEphemeral(id, token, `✅ Deleted ${ids.length} message${ids.length !== 1 ? 's' : ''}.`);
      }

      // /detain
      if (name === 'detain') {
        const targetId = opt('user');
        const reason   = opt('reason') || 'No reason provided.';
        const msgs     = await rest('GET', `/channels/${channel_id}/messages?limit=100`);
        const archive  = Array.isArray(msgs) ? msgs.reverse().map(m => `[${m.timestamp}] ${m.author?.username}: ${(m.content||'').slice(0,100)}`).join('\n') : '';
        const archiveContent = `**Archive from <#${channel_id}>:**\n\`\`\`\n${archive.slice(0,1900)}\n\`\`\``;
        const threadMsg = await rest('POST', `/channels/${THREAD_CHANNEL_ID}/messages`, { content: archiveContent });
        if (!threadMsg.id) return replyEphemeral(id, token, '❌ Failed to create detention thread.');
        const thread = await rest('POST', `/channels/${THREAD_CHANNEL_ID}/messages/${threadMsg.id}/threads`, { name: `Review - ${targetId}`, auto_archive_duration: 1440 });
        if (thread.id) {
          await rest('PUT',  `/channels/${thread.id}/thread-members/${targetId}`).catch(() => {});
          await rest('PUT',  `/guilds/${guild_id}/members/${targetId}/roles/${DETAINED_ROLE_ID}`).catch(() => {});
          await rest('POST', `/channels/${thread.id}/messages`, { content: `<@${targetId}>, your access is pending moderator review.\n*${reason}*\n<@&${MOD_ROLE_ID}> - please assist.` });
          await rest('POST', `/channels/${RECEIPT_CHANNEL_ID}/messages`, { content: `<@${userId}> detained <@${targetId}>\n**Reason:** ${reason}\n**Thread:** <#${thread.id}>` });
          return replyEphemeral(id, token, `✅ Detained <@${targetId}>. Thread: <#${thread.id}>`);
        }
        return replyEphemeral(id, token, '❌ Thread creation failed.');
      }

      // /release
      if (name === 'release') {
        const targetId = opt('user');
        const reason   = opt('reason') || 'No reason provided.';
        await rest('DELETE', `/guilds/${guild_id}/members/${targetId}/roles/${DETAINED_ROLE_ID}`).catch(() => {});
        await rest('POST',   `/channels/${RECEIPT_CHANNEL_ID}/messages`, { content: `<@${userId}> released <@${targetId}>\n**Reason:** ${reason}` });
        return replyEphemeral(id, token, `✅ Released <@${targetId}>.`);
      }
    }

    // ── BUTTON / SELECT INTERACTIONS (type 3) ─────────────────

    if (type === 3) {
      const cid = data.custom_id;

      // Marketplace
      if (cid === 'create_marketplace_listing') {
        if (cooldowns[userId]) {
          const diffDays = (Date.now() - new Date(cooldowns[userId]).getTime()) / 86400000;
          if (diffDays < COOLDOWN_DAYS) {
            const daysLeft = Math.ceil(COOLDOWN_DAYS - diffDays);
            const nextDate = new Date(new Date(cooldowns[userId]).getTime() + COOLDOWN_DAYS * 86400000).toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
            return replyEphemeral(id, token, `❌ You can only open a shop once every ${COOLDOWN_DAYS} days.\nNext available: **${nextDate}** (~${daysLeft} day${daysLeft !== 1 ? 's' : ''}).`);
          }
        }
        return showModal(id, token, await buildOpenShopModal());
      }

      if (cid === 'add_listing_item') {
        if (!threads[userId]) return replyEphemeral(id, token, '❌ No active shop found. Open a shop first.');
        return showModal(id, token, buildListItemModal());
      }

      // Jump to My Shop
      if (cid === 'jump_to_my_shop') {
        const threadId = threads[userId];
        if (!threadId) return replyEphemeral(id, token, "❌ You don't have an active shop. Click **Open Shop** to create one!");
        return replyEphemeral(id, token, `Here's your shop: <#${threadId}>`);
      }

      // Edit Item button — custom_id: edit_item_<msgId>
      if (cid.startsWith('edit_item_')) {
        const msgId = cid.slice('edit_item_'.length);
        const item  = itemMessages[msgId];
        if (!item)              return replyEphemeral(id, token, '❌ Item data not found. It may predate this feature.');
        if (item.userId !== userId) return replyEphemeral(id, token, '❌ You can only edit your own items.');
        return showModal(id, token, buildEditItemModal(msgId));
      }

      // Remove Item button — custom_id: remove_item_<msgId>
      if (cid.startsWith('remove_item_')) {
        const msgId = cid.slice('remove_item_'.length);
        const item  = itemMessages[msgId];
        if (item && item.userId !== userId) return replyEphemeral(id, token, '❌ You can only remove your own items.');
        const targetThread = item?.threadId || channel_id;
        await rest('DELETE', `/channels/${targetThread}/messages/${msgId}`).catch(() => {});
        if (item) { delete itemMessages[msgId]; saveJSON('item_messages.json', itemMessages); }
        return replyEphemeral(id, token, '✅ Item removed from your shop.');
      }

      // ISO
      if (cid === 'add_iso_item')    return showModal(id, token, await buildIsoAddModal());
      if (cid === 'edit_iso_item') {
        const modal = await buildIsoEditModal(userId);
        return modal ? showModal(id, token, modal) : replyEphemeral(id, token, "❌ You don't have any active ISO listings.");
      }
      if (cid === 'remove_iso_item') {
        const modal = await buildIsoRemoveModal(userId);
        return modal ? showModal(id, token, modal) : replyEphemeral(id, token, "❌ You don't have any active ISO listings.");
      }

      // Brownie points
      if (cid === 'rep_button') {
        return respond(id, token, 4, {
          flags: 64,
          content: '**Give Brownie Points**\nSelect a user from the dropdown below:',
          components: [{ type: 1, components: [{ type: 5, custom_id: 'rep_user_select', placeholder: 'Select a user to give brownie points', min_values: 1, max_values: 1 }] }],
        });
      }
      if (cid === 'rep_user_select') {
        const targetId = data.values?.[0];
        if (targetId === userId) return updateMessage(id, token, { content: '❌ You cannot give brownie points to yourself!', components: [] });
        if (!brownie[targetId]) brownie[targetId] = 0;
        brownie[targetId]++;
        saveJSON('brownie.json', brownie);
        return updateMessage(id, token, { content: `✅ <@${userId}> gave 1 brownie point to <@${targetId}>!\n**Total:** ${brownie[targetId]}`, components: [] });
      }

      // Support tickets
      if (cid === 'general_ticket') {
        const channel2 = await createTicketChannel(guild_id, GENERAL_TICKET_CAT_ID, `ticket-${username}`, userId, MOD_ROLE_ID);
        if (!channel2?.id) return replyEphemeral(id, token, '❌ Failed to create ticket channel.');
        const embed = { title: 'Support Ticket', color: COLOR, description: `Welcome <@${userId}>! Please describe your issue and a moderator will assist you shortly.` };
        await rest('POST', `/channels/${channel2.id}/messages`, {
          embeds: [embed],
          components: [{ type: 1, components: [
            { type: 2, style: 2, label: 'Close Ticket',  custom_id: 'ticket_close'  },
            { type: 2, style: 4, label: 'Delete Ticket', custom_id: 'ticket_delete' },
          ]}],
        });
        await rest('POST', `/channels/${channel2.id}/messages`, { content: `<@${userId}> - a moderator will be with you shortly.` });
        return replyEphemeral(id, token, `✅ Ticket created: <#${channel2.id}>`);
      }

      if (cid === 'report_incident') {
        pendingState[userId] = { cmd: 'report', anonymous: false };
        return respond(id, token, 4, {
          flags: 64,
          content: '**Report User or Incident**\nSelect a user to report (optional), then click Submit:',
          components: [
            { type: 1, components: [{ type: 5, custom_id: 'report_user_select', placeholder: 'Select user to report (optional)', min_values: 0, max_values: 1 }] },
            { type: 1, components: [
              { type: 2, style: 2, label: 'Submit',             custom_id: 'report_submit'     },
              { type: 2, style: 2, label: 'Submit Anonymously', custom_id: 'report_submit_anon' },
            ]},
          ],
        });
      }

      if (cid === 'report_user_select') {
        const reportedId = data.values?.[0] || null;
        if (!pendingState[userId]) pendingState[userId] = { cmd: 'report' };
        pendingState[userId].reportedUserId = reportedId;
        return updateMessage(id, token, { content: `User selected: ${reportedId ? `<@${reportedId}>` : 'none'}. Click Submit to continue.`, components: [
          { type: 1, components: [{ type: 5, custom_id: 'report_user_select', placeholder: 'Select user to report (optional)', min_values: 0, max_values: 1 }] },
          { type: 1, components: [
            { type: 2, style: 2, label: 'Submit',             custom_id: 'report_submit'     },
            { type: 2, style: 2, label: 'Submit Anonymously', custom_id: 'report_submit_anon' },
          ]},
        ]});
      }

      if (cid === 'report_submit' || cid === 'report_submit_anon') {
        if (!pendingState[userId]) pendingState[userId] = { cmd: 'report' };
        pendingState[userId].anonymous = (cid === 'report_submit_anon');
        return showModal(id, token, MODAL_REPORT);
      }

      if (cid === 'ticket_close') {
        await rest('PATCH', `/channels/${channel_id}`, { archived: true, locked: true }).catch(async () => {
          await rest('PUT', `/channels/${channel_id}/permissions/${guild_id}`, { type: 0, deny: '1024' }).catch(() => {});
        });
        return replyEphemeral(id, token, '✅ Ticket closed.');
      }

      if (cid === 'ticket_delete') {
        await replyEphemeral(id, token, 'Deleting in 5 seconds...');
        setTimeout(() => rest('DELETE', `/channels/${channel_id}`).catch(() => {}), 5000);
        return;
      }

      // Role buttons
      if (cid.startsWith('btn_role_')) {
        const roleId = cid.replace('btn_role_', '').split('_')[0];
        const memberRoles = member?.roles || [];
        if (memberRoles.includes(roleId)) {
          await rest('DELETE', `/guilds/${guild_id}/members/${userId}/roles/${roleId}`);
          return replyEphemeral(id, token, `➖ Removed role <@&${roleId}>`);
        } else {
          await rest('PUT', `/guilds/${guild_id}/members/${userId}/roles/${roleId}`);
          return replyEphemeral(id, token, `✅ Added role <@&${roleId}>`);
        }
      }

      // Role dropdown
      if (cid.startsWith('rd_') && data.values?.length) {
        const added = [], removed = [];
        const memberRoles = member?.roles || [];
        for (const roleId of data.values) {
          if (memberRoles.includes(roleId)) {
            await rest('DELETE', `/guilds/${guild_id}/members/${userId}/roles/${roleId}`);
            removed.push(roleId);
          } else {
            await rest('PUT', `/guilds/${guild_id}/members/${userId}/roles/${roleId}`);
            added.push(roleId);
          }
        }
        const lines = [];
        if (added.length)   lines.push(`✅ Added: ${added.map(r => `<@&${r}>`).join(', ')}`);
        if (removed.length) lines.push(`➖ Removed: ${removed.map(r => `<@&${r}>`).join(', ')}`);
        return replyEphemeral(id, token, lines.join('\n') || 'ℹ️ No changes made.');
      }
    }

    // ── MODAL SUBMISSIONS (type 5) ─────────────────────────────

    if (type === 5) {
      const cid = data.custom_id;
      const getVal = (key) => data.components?.flatMap(r => r.components || []).find(c => c.custom_id === key)?.value?.trim() || '';

      // Marketplace modals
      if (cid === 'mp_open_shop') {
        const fields = getFields(data.components, data.resolved);
        const comps  = Object.values(fields);
        let transactions = [], payment = [], shipping = '', tags = [], notes = '';
        for (const c of comps) {
          if (c.type === 22 && c.values) {
            if (c.values.every(v => TRANSACTION_OPTS.some(o => o.value === v))) transactions = c.values;
            else if (c.values.every(v => PAYMENT_OPTS.some(o => o.value === v))) payment = c.values;
          }
          if (c.type === 21 && c.value && SHIPPING_OPTS.some(o => o.value === c.value)) shipping = c.value;
          if (c.type === 3  && c.values?.every(v => TAG_IDS.includes(v))) tags = c.values;
          if (c.type === 4)  notes = c.value?.trim() || '';
        }
        if (!transactions.length) return replyEphemeral(id, token, '❌ Please select at least one transaction type.');
        if (!payment.length)      return replyEphemeral(id, token, '❌ Please select at least one payment method.');
        if (!shipping)            return replyEphemeral(id, token, '❌ Please select a shipping option.');
        if (!tags.length)         return replyEphemeral(id, token, '❌ Please select at least one tag.');
        return postShop(id, token, userId, username, avatarUrl, { transactions, payment, shipping, tags, notes });
      }

      if (cid === 'mp_list_item') {
        const fields      = getFields(data.components, data.resolved);
        const comps       = Object.values(fields);
        const textComps   = comps.filter(c => c.type === 4);
        const selectComps = comps.filter(c => c.type === 3);
        const fileComps   = comps.filter(c => c.type === 19);
        const name2     = textComps[0]?.value?.trim() || '';
        const price     = textComps[1]?.value?.trim().replace(/[$,]/g, '') || '';
        const notes     = textComps[2]?.value?.trim() || '';
        const condition = selectComps[0]?.values?.[0] || '';
        const photoUrls = (fileComps[0]?.files || []).map(f => f.url);
        const parsed = parseFloat(price);
        if (!name2)              return replyEphemeral(id, token, '❌ Item name is required.');
        if (isNaN(parsed) || parsed <= 0) return replyEphemeral(id, token, '❌ Price must be a positive number.');
        if (!condition)          return replyEphemeral(id, token, '❌ Please select a condition.');
        if (!photoUrls.length)   return replyEphemeral(id, token, '❌ Please upload at least one photo.');
        return postItem(id, token, userId, username, avatarUrl, { name: name2, price: parsed.toFixed(2), condition, notes, photoUrls });
      }

      // Edit item modal — custom_id: mp_edit_item_<msgId>
      if (cid.startsWith('mp_edit_item_')) {
        const msgId       = cid.slice('mp_edit_item_'.length);
        const item        = itemMessages[msgId];
        if (!item)                    return replyEphemeral(id, token, '❌ Original item not found.');
        if (item.userId !== userId)   return replyEphemeral(id, token, '❌ You can only edit your own items.');

        const fields      = getFields(data.components, data.resolved);
        const comps       = Object.values(fields);
        const textComps   = comps.filter(c => c.type === 4);
        const selectComps = comps.filter(c => c.type === 3);
        const fileComps   = comps.filter(c => c.type === 19);

        const name2     = textComps[0]?.value?.trim() || '';
        const priceRaw  = textComps[1]?.value?.trim().replace(/[$,]/g, '') || '';
        const notes     = textComps[2]?.value?.trim() || '';
        const condition = selectComps[0]?.values?.[0] || '';
        const photoUrls = (fileComps[0]?.files || []).map(f => f.url);

        const parsed = parseFloat(priceRaw);
        if (!name2)                       return replyEphemeral(id, token, '❌ Item name is required.');
        if (isNaN(parsed) || parsed <= 0) return replyEphemeral(id, token, '❌ Price must be a positive number.');
        if (!condition)                   return replyEphemeral(id, token, '❌ Please select a condition.');
        if (!photoUrls.length)            return replyEphemeral(id, token, '❌ Please upload at least one photo.');

        const price2       = parsed.toFixed(2);
        const updatedEmbed = buildItemEmbed(item.username, item.avatarUrl, name2, price2, condition, notes, photoUrls);

        await rest('PATCH', `/channels/${item.threadId}/messages/${msgId}`, {
          embeds:     [updatedEmbed],
          components: [itemButtonRow(msgId)],
        });

        // Update stored state
        itemMessages[msgId] = { ...item, name: name2, price: price2, condition, notes, photoUrls };
        saveJSON('item_messages.json', itemMessages);

        return replyEphemeral(id, token, '✅ Item updated!');
      }

      // ISO modals
      if (cid === 'mp_iso_submit') {
        const fields    = getFields(data.components, data.resolved);
        const comps     = Object.values(fields);
        const threadId2 = comps.filter(c => c.type === 3)[0]?.values?.[0];
        const content2  = comps.filter(c => c.type === 4)[0]?.value?.trim() || '';
        const photoUrls = (comps.filter(c => c.type === 19)[0]?.files || []).map(f => f.url);
        if (!threadId2) return replyEphemeral(id, token, '❌ Please select an IP category.');
        if (!content2)  return replyEphemeral(id, token, "❌ Please describe what you're looking for.");
        return upsertIsoListing(id, token, userId, username, avatarUrl, threadId2, content2, photoUrls);
      }

      if (cid === 'mp_iso_edit') {
        const fields    = getFields(data.components, data.resolved);
        const comps     = Object.values(fields);
        const threadId2 = comps.filter(c => c.type === 3)[0]?.values?.[0];
        const content2  = comps.filter(c => c.type === 4)[0]?.value?.trim() || '';
        if (!threadId2) return replyEphemeral(id, token, '❌ Please select a listing.');
        if (!content2)  return replyEphemeral(id, token, "❌ Updated content is required.");
        return editIsoListing(id, token, userId, username, avatarUrl, threadId2, content2);
      }

      if (cid === 'mp_iso_remove') {
        const fields    = getFields(data.components, data.resolved);
        const comps     = Object.values(fields);
        const threadId2 = comps.filter(c => c.type === 3)[0]?.values?.[0];
        if (!threadId2) return replyEphemeral(id, token, '❌ Please select a listing.');
        return removeIsoListing(id, token, userId, threadId2);
      }

      // Say modals
      if (cid === 'modal_say' || cid === 'modal_sayreply') {
        const state    = pendingState[userId] || {};
        const content2 = getVal('message_content');
        const imgUrl   = getVal('image_url');
        const embeds   = imgUrl ? [{ color: COLOR, image: { url: imgUrl } }] : undefined;
        if (cid === 'modal_sayreply' && state.messageId) {
          await rest('POST', `/channels/${state.channelId}/messages`, { content: content2, ...(embeds ? { embeds } : {}), message_reference: { message_id: state.messageId } });
        } else {
          await rest('POST', `/channels/${state.channelId}/messages`, { content: content2, ...(embeds ? { embeds } : {}) });
        }
        delete pendingState[userId];
        return respond(id, token, 4, { content: '✅ Message sent!', flags: 64 });
      }

      // Create forum modal
      if (cid === 'modal_createforum') {
        const state    = pendingState[userId] || {};
        const content2 = getVal('post_content');
        const result   = await rest('POST', `/channels/${state.forumId}/threads`, { name: state.title, message: { content: content2 } });
        delete pendingState[userId];
        return result.id ? respond(id, token, 4, { content: `✅ Forum post created: <#${result.id}>`, flags: 64 }) : replyEphemeral(id, token, '❌ Failed to create forum post.');
      }

      // Create embed modal
      if (cid === 'modal_createembed') {
        const state         = pendingState[userId] || {};
        const embedContent  = getVal('embed_content');
        const compConfig    = getVal('components_config');
        const sections      = embedContent.split('===').filter(s => s.trim());
        const embeds        = sections.map(s => buildDiscordEmbed(parseEmbedSection(s)));
        if (!embeds.length) return replyEphemeral(id, token, '❌ No valid embed content found.');
        if (embeds.length > 10) return replyEphemeral(id, token, '❌ Max 10 embeds per message.');
        const components = buildComponentsFromConfig(compConfig);
        const msg = await rest('POST', `/channels/${state.channelId}/messages`, {
          content: state.content || undefined,
          embeds,
          ...(components.length ? { components } : {}),
        });
        if (msg.id && components.length) { embedButtons[msg.id] = compConfig; saveJSON('embed_buttons.json', embedButtons); }
        delete pendingState[userId];
        return respond(id, token, 4, { content: `✅ ${embeds.length} embed${embeds.length !== 1 ? 's' : ''} sent to <#${state.channelId}>!`, flags: 64 });
      }

      // Create sticky modal
      if (cid === 'modal_createsticky') {
        const state        = pendingState[userId] || {};
        const embedContent = getVal('embed_content');
        const plainContent = getVal('plain_content') || null;
        const compConfig   = getVal('components_config');
        const embedData    = parseEmbedSection(embedContent);
        const embed        = buildDiscordEmbed(embedData);
        const components   = buildComponentsFromConfig(compConfig);
        const msg = await rest('POST', `/channels/${state.channelId}/messages`, {
          content: plainContent || undefined,
          embeds: [embed],
          ...(components.length ? { components } : {}),
        });
        if (!msg.id) return replyEphemeral(id, token, '❌ Failed to send sticky message.');
        stickyData[state.channelId] = { messageId: msg.id, embed: embedData, components: components.length ? components : [], content: plainContent };
        saveJSON('sticky.json', stickyData);
        delete pendingState[userId];
        return respond(id, token, 4, { content: `✅ Sticky message created in <#${state.channelId}>!`, flags: 64 });
      }

      // Report modal
      if (cid === 'modal_report') {
        const state      = pendingState[userId] || {};
        const summary    = getVal('incident_summary');
        const media      = getVal('media_links');
        const reportedId = state.reportedUserId || null;
        const anonymous  = state.anonymous || false;

        const embed = {
          title: 'Incident Report', color: COLOR,
          fields: [
            { name: 'Reported User', value: reportedId ? `<@${reportedId}>` : 'N/A', inline: true },
            { name: 'Reporter',      value: anonymous ? 'Anonymous' : `<@${userId}>`, inline: true },
            { name: 'Summary',       value: summary, inline: false },
            ...(media ? [{ name: 'Media Links', value: media, inline: false }] : []),
          ],
          timestamp: new Date().toISOString(),
        };

        if (anonymous) {
          const msg = await rest('POST', `/channels/${RECEIPT_CHANNEL_ID}/messages`, { embeds: [embed] });
          if (msg.id) {
            const thread = await rest('POST', `/channels/${RECEIPT_CHANNEL_ID}/messages/${msg.id}/threads`, { name: `Report: ${reportedId || 'Anonymous'}`, auto_archive_duration: 1440 });
            if (thread.id) {
              await rest('POST', `/channels/${thread.id}/messages`, { content: `**Ticket Controls:**`, components: [{ type: 1, components: [{ type: 2, style: 2, label: 'Close Ticket', custom_id: 'ticket_close' }, { type: 2, style: 4, label: 'Delete Ticket', custom_id: 'ticket_delete' }] }] });
              await rest('POST', `/channels/${thread.id}/messages`, { content: `<@&${EMERGENCY_ROLE_ID}> - New anonymous report` });
            }
          }
        } else {
          const name2    = `report-${username}-${Date.now()}`;
          const channel2 = await createTicketChannel(guild_id, REPORT_TICKET_CAT_ID, name2, userId, MOD_ROLE_ID);
          if (channel2?.id) {
            await rest('POST', `/channels/${channel2.id}/messages`, { embeds: [embed] });
            await rest('POST', `/channels/${channel2.id}/messages`, { content: `**Ticket Controls:**`, components: [{ type: 1, components: [{ type: 2, style: 2, label: 'Close Ticket', custom_id: 'ticket_close' }, { type: 2, style: 4, label: 'Delete Ticket', custom_id: 'ticket_delete' }] }] });
            await rest('POST', `/channels/${channel2.id}/messages`, { content: `<@${userId}> - Your report has been created. A moderator will assist you shortly.` });
            delete pendingState[userId];
            return respond(id, token, 4, { content: `✅ Report channel created: <#${channel2.id}>`, flags: 64 });
          }
        }
        delete pendingState[userId];
        return respond(id, token, 4, { content: '✅ Your anonymous report has been submitted.', flags: 64 });
      }
    }

  } catch (err) {
    console.error('Interaction error:', err);
    try { await replyEphemeral(id, token, `❌ An error occurred: ${err.message}`); } catch {}
  }
}

function buildComponentsFromConfig(configText) {
  if (!configText?.trim()) return [];
  const lines  = configText.trim().split('\n').filter(l => l.trim() && !l.startsWith('#'));
  const byRow  = {};
  lines.forEach((line, i) => {
    const parts = line.split('|').map(p => p.trim());
    let row = i;
    if (/^\d+$/.test(parts[0])) { row = parseInt(parts.shift()); line = parts.join('|'); }
    const comp = parseComponentLine(parts.join('|'), row);
    if (comp) { if (!byRow[row]) byRow[row] = []; byRow[row].push(comp); }
  });
  return Object.keys(byRow).sort((a,b) => a-b).slice(0,5).map(r => ({
    type: 1,
    components: byRow[r].map(buildComponentRow).filter(Boolean).slice(0,5),
  })).filter(r => r.components.length);
}
// ============================================================
// GATEWAY EVENT HANDLERS
// ============================================================

let appIdCache = null;

async function handleGatewayEvent(t, d) {
  try {

    if (t === 'READY') {
      appIdCache = d.application.id;
      console.log(`TrinketBot ready - ${d.user.username}#${d.user.discriminator}`);
      await registerCommands(appIdCache);
      getIpOptions().catch(() => {}); // pre-warm cache on startup
      setInterval(checkScheduled, 60000);
    }

    if (t === 'MESSAGE_CREATE') {
      const msg = d;
      if (msg.author?.bot && msg.channel_id !== AUTO_FORWARD_CHANNEL_ID) return;

      if (msg.channel_id === AUTO_FORWARD_CHANNEL_ID && msg.author?.bot) {
        await handleYouTubeForward(msg.content || '');
        return;
      }
      if (msg.author?.bot) return;

      if (msg.channel_id === WORKBENCH_CHANNEL_ID && msg.content?.toLowerCase().includes('#project')) {
        const thread = await rest('POST', `/channels/${WORKBENCH_CHANNEL_ID}/messages/${msg.id}/threads`, {
          name: `${msg.author.username}'s workbench`,
          auto_archive_duration: 1440,
        }).catch(() => null);
        if (thread?.id) {
          await rest('POST', `/channels/${thread.id}/messages`, { content: `Welcome to your workbench, <@${msg.author.id}>! Use this thread to discuss your project.` });
        }
        return;
      }

      // FIX: never trigger sticky repost on the bot's own messages to avoid double-posting panels
      if (msg.author?.id === appIdCache) return;

      if (stickyData[msg.channel_id]) {
        await repostSticky(msg.channel_id);
      }
    }

    if (t === 'MESSAGE_DELETE') {
      const embed = {
        title: 'Message Deleted', color: 0xff0000,
        fields: [
          { name: 'Channel',    value: `<#${d.channel_id}>`, inline: true },
          { name: 'Message ID', value: `\`${d.id}\``,        inline: true },
        ],
        timestamp: new Date().toISOString(),
      };
      await rest('POST', `/channels/${MESSAGE_LOG_CHANNEL_ID}/messages`, { embeds: [embed] }).catch(() => {});
    }

    if (t === 'MESSAGE_UPDATE') {
      if (d.author?.bot) return;
      if (!d.content || !d.edited_timestamp) return;
      const embed = {
        title: 'Message Edited', color: 0xffa500,
        fields: [
          { name: 'Author',  value: `<@${d.author?.id}> (${d.author?.username})`, inline: true },
          { name: 'Channel', value: `<#${d.channel_id}>`, inline: true },
          { name: 'New Content', value: d.content.slice(0, 1024) || '*empty*', inline: false },
          { name: 'Jump', value: `[Jump to message](https://discord.com/channels/${d.guild_id}/${d.channel_id}/${d.id})`, inline: false },
        ],
        footer: { text: `Message ID: ${d.id}` },
        timestamp: new Date().toISOString(),
      };
      await rest('POST', `/channels/${MESSAGE_LOG_CHANNEL_ID}/messages`, { embeds: [embed] }).catch(() => {});
    }

    if (t === 'GUILD_MEMBER_ADD') {
      const member = d;
      const userId2 = member.user?.id;
      const created = new Date(member.user?.id ? Number((BigInt(member.user.id) >> 22n) + 1420070400000n) : Date.now());
      const ageDays = Math.floor((Date.now() - created.getTime()) / 86400000);
      let ageText;
      if (ageDays < 1)       ageText = 'Less than 1 day old ⚠️';
      else if (ageDays < 7)  ageText = `${ageDays} days old ⚠️`;
      else if (ageDays < 30) ageText = `${ageDays} days old`;
      else                   ageText = `~${Math.floor(ageDays/30)} months old`;

      const avatarUrl2 = member.user?.avatar
        ? `https://cdn.discordapp.com/avatars/${userId2}/${member.user.avatar}.png`
        : 'https://cdn.discordapp.com/embed/avatars/0.png';

      const embed = {
        title: 'New Member Joined', color: 0x00ff00,
        thumbnail: { url: avatarUrl2 },
        fields: [
          { name: 'User',       value: `<@${userId2}>`,          inline: true },
          { name: 'Username',   value: member.user?.username,     inline: true },
          { name: 'User ID',    value: `\`${userId2}\``,          inline: true },
          { name: 'Account Age',value: ageText,                   inline: true },
          { name: 'Avatar',     value: `[Link](${avatarUrl2})`,   inline: true },
        ],
        timestamp: new Date().toISOString(),
      };
      await rest('POST', `/channels/${JOIN_LOG_CHANNEL_ID}/messages`, { embeds: [embed] }).catch(() => {});
      if (userId2) {
        await rest('PUT', `/guilds/${member.guild_id}/members/${userId2}/roles/${WELCOME_ROLE_ID}`).catch(() => {});
      }
    }

    if (t === 'MESSAGE_REACTION_ADD') {
      const { user_id, channel_id: reactionChannelId, message_id, emoji, guild_id: guildId2 } = d;
      if (user_id === appIdCache) return;

      // ISO bell reaction
      if (emoji?.name === ISO_EMOJI) {
        try {
          const channel2 = await rest('GET', `/channels/${reactionChannelId}`);
          const isIsoThread = channel2.type === 11 && channel2.parent_id === ISO_FORUM_ID;
          if (!isIsoThread) return;

          const message2 = await rest('GET', `/channels/${reactionChannelId}/messages/${message_id}`);
          const authorId  = message2.author?.id;
          if (!authorId || authorId === user_id) return;

          let notifyChannelId = DEFAULT_CHANNEL_ID;
          const invForum = await rest('GET', `/channels/${INVENTORY_FORUM_ID}`);
          if (invForum.threads) {
            const reactorThread = invForum.threads.find(t2 => t2.owner_id === user_id);
            if (reactorThread) notifyChannelId = reactorThread.id;
          }

          await rest('POST', `/channels/${notifyChannelId}/messages`, {
            content: `<@${authorId}>, <@${user_id}> reacted to your ISO and may have one of your items available!\n**Original message:** https://discord.com/channels/${guildId2}/${reactionChannelId}/${message_id}`,
          }).catch(() => {});

          const dm = await rest('POST', '/users/@me/channels', { recipient_id: user_id }).catch(() => null);
          if (dm?.id) {
            await rest('POST', `/channels/${dm.id}/messages`, {
              content: `✅ **ISO Reaction Confirmed**\nYou reacted to <@${authorId}>'s ISO message.\nThey've been notified!\nhttps://discord.com/channels/${guildId2}/${reactionChannelId}/${message_id}`,
            }).catch(() => {});
          }
        } catch (e) {
          console.error('ISO reaction error:', e.message);
        }
      }

      // Starboard
      if (emoji?.id === STARBOARD_EMOJI_ID) {
        if (reactionChannelId === STARBOARD_CHANNEL_ID) return;
        try {
          const message2 = await rest('GET', `/channels/${reactionChannelId}/messages/${message_id}`);
          const jumpUrl   = `https://discord.com/channels/${guildId2}/${reactionChannelId}/${message_id}`;
          const embed = {
            color: 0xffd700,
            description: message2.content || '*No text content*',
            author: { name: message2.author?.username, icon_url: message2.author?.avatar ? `https://cdn.discordapp.com/avatars/${message2.author.id}/${message2.author.avatar}.png` : undefined },
            ...(message2.attachments?.[0] ? { image: { url: message2.attachments[0].url } } : {}),
            fields: [{ name: 'Source', value: `[Jump to Message](${jumpUrl})`, inline: false }],
            footer: { text: `#${reactionChannelId}` },
            timestamp: message2.timestamp,
          };
          await rest('POST', `/channels/${STARBOARD_CHANNEL_ID}/messages`, { embeds: [embed] }).catch(() => {});
        } catch (e) {
          console.error('Starboard error:', e.message);
        }
      }
    }

  } catch (e) {
    console.error(`Gateway event ${t} error:`, e.message);
  }
}


// ============================================================
// GATEWAY (WebSocket connection with reconnect/resume)
// ============================================================

let heartbeatInterval = null;
let resumeGatewayUrl  = null;
let sessionId         = null;
let sequence          = null;
let ws                = null;
let reconnectTimeout  = null;
let reconnectDelay    = 1000;
let isConnecting      = false;

function scheduleReconnect(useResume = true) {
  if (reconnectTimeout) return;
  console.log(`Reconnecting in ${reconnectDelay / 1000}s…`);
  reconnectTimeout = setTimeout(() => {
    reconnectTimeout = null;
    connect(useResume && resumeGatewayUrl ? resumeGatewayUrl : GATEWAY, useResume);
  }, reconnectDelay);
  reconnectDelay = Math.min(reconnectDelay * 2, 30000);
}

function connect(url = GATEWAY, tryResume = false) {
  if (isConnecting) return;
  isConnecting = true;
  if (ws) { ws.removeAllListeners(); try { ws.close(); } catch {} ws = null; }
  clearInterval(heartbeatInterval);
  heartbeatInterval = null;

  ws = new WebSocket(url);

  ws.on('open', () => {
    isConnecting   = false;
    reconnectDelay = 1000;
    // FIX: claim instance lock on connect and keep refreshing every 4s
    refreshLock();
    setInterval(refreshLock, 4000);
  });

  ws.on('message', async raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    const { op, d, s, t } = msg;
    if (s != null) sequence = s;

    if (op === 10) {
      clearInterval(heartbeatInterval);
      heartbeatInterval = setInterval(() => {
        if (ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ op: 1, d: sequence }));
      }, d.heartbeat_interval);
      if (tryResume && sessionId && sequence) {
        ws.send(JSON.stringify({ op: 6, d: { token: TOKEN, session_id: sessionId, seq: sequence } }));
      } else {
        ws.send(JSON.stringify({ op: 2, d: { token: TOKEN, intents: INTENTS, properties: { os: 'linux', browser: 'bot', device: 'bot' } } }));
      }
    }

    if (op === 1)  ws.send(JSON.stringify({ op: 1, d: sequence }));
    if (op === 7)  { console.log('Discord requested reconnect (op 7)'); ws.close(4000); }
    if (op === 9)  { const resumable = d === true; console.log(`Invalid session (resumable: ${resumable})`); if (!resumable) { sessionId = null; sequence = null; } ws.close(4000); }

    if (op === 0) {
      if (t === 'READY') {
        sessionId        = d.session_id;
        resumeGatewayUrl = d.resume_gateway_url;
        reconnectDelay   = 1000;
      }
      if (t === 'RESUMED') {
        console.log('Session resumed.');
        reconnectDelay = 1000;
      }
      if (t === 'INTERACTION_CREATE') {
        const t0 = Date.now();
        const interactionKey = d.data?.custom_id ?? d.data?.name ?? 'unknown';
        console.log('INTERACTION:', JSON.stringify({ type: d.type, id: interactionKey, user: d.member?.user?.id ?? d.user?.id }));
        try {
          await handleInteraction(d);
        } catch (e) {
          console.error(`INTERACTION ERROR [${interactionKey}]:`, e.message, e.stack?.split('\n')[1]);
        }
        console.log(`Handled in ${Date.now() - t0}ms`);
      }
      await handleGatewayEvent(t, d);
    }
  });

  ws.on('close', code => {
    isConnecting = false;
    clearInterval(heartbeatInterval);
    heartbeatInterval = null;
    ws = null;
    console.log(`WebSocket closed (${code}).`);
    const unrecoverable = [4004, 4010, 4011, 4012, 4013, 4014];
    if (unrecoverable.includes(code)) { console.error(`Unrecoverable close code ${code}. Stopping.`); return; }
    scheduleReconnect(code !== 1000 && code !== 4009 && sessionId != null);
  });

  ws.on('error', err => {
    isConnecting = false;
    console.error('WebSocket error:', err.message);
  });
}

connect();
