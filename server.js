import express from 'express';
import http from 'http';
import { Server as SocketIOServer } from 'socket.io';
import { createHttpClient } from '@ably/pubsub-server';
import cors from 'cors';
import dotenv from 'dotenv';
import crypto from 'crypto';
import nodemailer from 'nodemailer';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { query, withTransaction } from './db/db.js';
import { authenticateUser, requireAdmin, generateToken, hasAdminRights, invalidateUserAuthCache } from './middleware/auth.js';

const __serverDir = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__serverDir, '.env') });

const app = express();
const httpServer = http.createServer(app);
const io = new SocketIOServer(httpServer, {
  cors: { origin: '*', methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE'] },
  pingTimeout: 30000,
  pingInterval: 15000
});

const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors({ origin: '*', credentials: true }));
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));
app.use(authenticateUser);

let _ablyRest = null;
function getAblyRest() {
  const key = (process.env.ABLY_API_KEY || '').trim();
  if (!key) return null;
  if (!_ablyRest) {
    _ablyRest = createHttpClient({ key });
  }
  return _ablyRest;
}

async function publishAblyEvent(channelName, eventName, data) {
  const client = getAblyRest();
  if (!client) return false;
  try {
    await client.channels.get(channelName).publish(eventName, data);
    return true;
  } catch (err) {
    console.warn(`[Ably Publish Warning] ${channelName} -> ${eventName}:`, err.message);
    return false;
  }
}

// Password Hashing Helper
function hashPassword(password) {
  const saltPassword = password + '__sjvps_salt_2024__';
  return crypto.createHash('sha256').update(saltPassword).digest('hex');
}

function parseJsonField(val, fallback) {
  if (val === null || val === undefined) return fallback;
  if (typeof val === 'string') {
    try {
      return JSON.parse(val || JSON.stringify(fallback));
    } catch {
      return fallback;
    }
  }
  return val;
}

// Column & Page Normalization Helpers
function normalizeColumns(columns) {
  const cols = parseJsonField(columns, []);
  if (!Array.isArray(cols)) return [];
  return cols.map((c, idx) => ({
    ...c,
    id: c.id ?? c.key ?? ('col_' + Math.random().toString(36).substring(2, 9)),
    key: c.key ?? c.id ?? ('col_' + Math.random().toString(36).substring(2, 9)),
    position: c.position !== undefined ? c.position : idx
  }));
}

function normalizePages(pages) {
  const raw = parseJsonField(pages, []);
  if (!Array.isArray(raw) || raw.length === 0) return [{ id: 1, name: 'Page 1', index: 0 }];
  return raw.map((p, idx) => {
    if (typeof p === 'string') return { id: idx + 1, name: p, index: idx };
    return { id: p.id || (idx + 1), name: p.name || ('Page ' + (idx + 1)), index: p.index ?? idx };
  });
}

function parseBigInt(val) {
  if (val === null || val === undefined || val === '' || val === 'undefined' || val === 'null') return null;
  const num = Number(val);
  if (!Number.isFinite(num) || !Number.isInteger(num)) return null;
  return num;
}

function toIdValue(val) {
  if (val === null || val === undefined) return val;
  const num = Number(val);
  return !isNaN(num) && String(val).trim() !== '' ? num : val;
}

function generateServerEntryId() {
  return Date.now() * 1000 + Math.floor(Math.random() * 1000);
}

function formatEntry(e) {
  if (!e) return null;
  return {
    ...e,
    id: toIdValue(e.id),
    registerId: toIdValue(e.register_id),
    rowNumber: Number(e.row_number ?? 1),
    row_number: Number(e.row_number ?? 1),
    pageIndex: Number(e.page_index ?? 0),
    page_index: Number(e.page_index ?? 0),
    cellStyles: parseJsonField(e.cell_styles, {}),
    cells: parseJsonField(e.cells, {}),
    createdAt: e.created_at
  };
}

function formatRegister(r, entries) {
  if (!r) return null;
  const folderIdVal = r.folder_id !== null && r.folder_id !== undefined ? toIdValue(r.folder_id) : null;
  const sharedArr = parseJsonField(r.shared_with, []);
  const deletedItemsArr = parseJsonField(r.deleted_items, []);
  const formatted = {
    ...r,
    id: toIdValue(r.id),
    businessId: Number(r.business_id || 1),
    business_id: Number(r.business_id || 1),
    folderId: folderIdVal ?? undefined,
    folder_id: folderIdVal,
    icon: r.icon || 'BookOpen',
    iconColor: r.icon_color || '#1a73e8',
    icon_color: r.icon_color || '#1a73e8',
    category: r.category || 'General',
    template: r.template || '',
    createdAt: r.created_at,
    updatedAt: r.updated_at,
    entryCount: Number(r.entry_count ?? (Array.isArray(entries) ? entries.length : 0)),
    entry_count: Number(r.entry_count ?? (Array.isArray(entries) ? entries.length : 0)),
    deletedAt: r.deleted_at,
    deletedBy: r.deleted_by,
    deletedByEmail: r.deleted_by_email,
    deletedById: r.deleted_by_id,
    columns: normalizeColumns(r.columns),
    pages: normalizePages(r.pages),
    shareLink: r.share_link,
    sharedWith: sharedArr,
    shared_with: sharedArr,
    deletedItems: deletedItemsArr,
    deleted_items: deletedItemsArr,
    migrationCompleted: r.migration_completed ?? true,
    entriesPerChunk: r.entries_per_chunk ?? 50
  };
  if (entries !== undefined) {
    formatted.entries = entries;
  }
  return formatted;
}

async function renumberEntriesInDb(client, regId) {
  await client.query(`
    WITH numbered AS (
      SELECT id, ROW_NUMBER() OVER (ORDER BY row_number ASC, id ASC) AS rn
      FROM entries
      WHERE register_id = $1
    )
    UPDATE entries e
    SET row_number = numbered.rn
    FROM numbered
    WHERE e.id = numbered.id
  `, [regId]);
}

function getAutoIncrementColKey(columns) {
  const cols = normalizeColumns(columns);
  const autoCol = cols.find(c => c.type === 'auto_increment');
  return autoCol ? String(autoCol.id) : null;
}

// Helper to send login notification email
async function sendLoginNotificationEmail(userEmail, userName, role) {
  try {
    if (!userEmail || !userEmail.includes('@') || userEmail.endsWith('@sjvps.com')) return;
    const smtpHost = process.env.SMTP_HOST || 'smtp.gmail.com';
    const smtpPort = parseInt(process.env.SMTP_PORT || '587', 10);
    const smtpUser = process.env.SMTP_USER;
    const smtpPass = process.env.SMTP_PASS;

    if (!smtpUser || !smtpPass) return;

    const loginTime = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata', dateStyle: 'medium', timeStyle: 'short' });
    const mailOptions = {
      from: process.env.SMTP_FROM || `"AG Admin Alert" <${smtpUser}>`,
      to: userEmail,
      subject: `🔒 Security Alert: Account Login Detected (${userName})`,
      html: `
        <div style="font-family: Arial, sans-serif; max-width: 500px; padding: 20px; border: 1px solid #e2e8f0; border-radius: 8px;">
          <h2 style="color: #0f172a;">Account Login Alert</h2>
          <p>Hello <strong>${userName}</strong>,</p>
          <p>A new login was recorded for your account at <strong>${loginTime} (IST)</strong>.</p>
          <p>Role: <strong>${role}</strong></p>
        </div>
      `
    };

    const transporter = nodemailer.createTransport({
      host: smtpHost, port: smtpPort, secure: smtpPort === 465,
      auth: { user: smtpUser, pass: smtpPass.replace(/\s+/g, '') },
      tls: { rejectUnauthorized: false }
    });
    await transporter.sendMail(mailOptions);
  } catch (err) {
    console.error('[Email Notification Error]', err.message);
  }
}

// ----------------------------------------------------
// REAL-TIME MULTI-USER STATE TRACKING (IN-MEMORY)
// ----------------------------------------------------
// Map<registerId, Map<socketId, { socketId, name, id, avatar, color, joinedAt }>>
const activeRoomUsers = new Map();
// Map<registerId, Map<cellKey, { socketId, entryId, colId, userName, userId, color }>>
const cellLocks = new Map();

const USER_COLORS = ['#3b82f6', '#ef4444', '#10b981', '#f59e0b', '#8b5cf6', '#ec4899', '#06b6d4', '#f97316', '#14b8a6', '#6366f1'];
function getUserColor(userId) {
  const hash = String(userId).split('').reduce((a, c) => a + c.charCodeAt(0), 0);
  return USER_COLORS[hash % USER_COLORS.length];
}

function makeCellKey(entryId, colId) {
  return JSON.stringify([String(entryId), String(colId)]);
}

function getDeduplicatedRoomUsers(regId) {
  if (!activeRoomUsers.has(regId)) return [];
  const byUserId = new Map();
  for (const u of activeRoomUsers.get(regId).values()) {
    const key = String(u.id || u.socketId);
    if (!byUserId.has(key)) {
      byUserId.set(key, {
        id: u.id,
        name: u.name,
        avatar: u.avatar,
        color: u.color,
        joinedAt: u.joinedAt
      });
    }
  }
  return Array.from(byUserId.values());
}

function releaseSocketLocks(socketId, regId) {
  const regKey = String(regId);
  if (!cellLocks.has(regKey)) return;
  const locks = cellLocks.get(regKey);
  const toUnlock = [];
  for (const [cellKey, lock] of locks.entries()) {
    if (lock.socketId === socketId) {
      toUnlock.push({ cellKey, entryId: lock.entryId, colId: lock.colId });
    }
  }
  for (const item of toUnlock) {
    locks.delete(item.cellKey);
    io.to(`register:${regKey}`).emit('cell:unlocked', {
      registerId: regKey,
      entryId: item.entryId,
      colId: item.colId
    });
  }
  if (locks.size === 0) cellLocks.delete(regKey);
}

// ----------------------------------------------------
// SOCKET.IO REAL-TIME EVENT ENGINE
// ----------------------------------------------------
io.on('connection', (socket) => {
  const joinedRegisters = new Set();

  function removeUserFromRoom(socketId, registerId) {
    const regId = String(registerId);
    releaseSocketLocks(socketId, regId);
    if (activeRoomUsers.has(regId)) {
      const roomUsers = activeRoomUsers.get(regId);
      roomUsers.delete(socketId);
      io.to(`register:${regId}`).emit('presence:update', {
        registerId: regId,
        users: getDeduplicatedRoomUsers(regId)
      });
      if (roomUsers.size === 0) activeRoomUsers.delete(regId);
    }
  }

  // 1. Join Register Room
  socket.on('join-register', (data) => {
    const registerId = String(typeof data === 'object' ? data.registerId : data);
    if (!registerId || registerId === 'undefined' || registerId === '0') return;
    const userName = typeof data === 'object' ? (data.userName || 'User') : 'User';
    const userId = typeof data === 'object' ? (data.userId || socket.id) : socket.id;
    const avatar = typeof data === 'object' ? (data.avatar || '') : '';

    const room = `register:${registerId}`;
    socket.join(room);
    joinedRegisters.add(registerId);

    if (!activeRoomUsers.has(registerId)) activeRoomUsers.set(registerId, new Map());
    const roomUsers = activeRoomUsers.get(registerId);
    roomUsers.set(socket.id, {
      socketId: socket.id,
      name: userName,
      id: userId,
      avatar,
      color: getUserColor(userId),
      joinedAt: Date.now()
    });

    io.to(room).emit('presence:update', {
      registerId,
      users: getDeduplicatedRoomUsers(registerId)
    });

    // Send current cell locks snapshot to the newly joined client
    const currentLocks = cellLocks.has(registerId)
      ? Array.from(cellLocks.get(registerId).values()).map(l => ({
          entryId: l.entryId,
          colId: l.colId,
          userName: l.userName,
          userId: l.userId,
          color: l.color,
          lockerSocketId: l.socketId
        }))
      : [];
    socket.emit('cell:locks-snapshot', { registerId, locks: currentLocks });
  });

  // 2. Leave Register Room
  socket.on('leave-register', (data) => {
    const regId = String(typeof data === 'object' ? data.registerId : data);
    if (!regId) return;
    socket.leave(`register:${regId}`);
    joinedRegisters.delete(regId);
    removeUserFromRoom(socket.id, regId);
  });

  // 3. Live Keystroke Relay (Character-by-Character live typing)
  socket.on('cell:live-typing', (data) => {
    if (!data || !data.registerId) return;
    const regId = String(data.registerId);
    socket.to(`register:${regId}`).emit('cell:live-typing', {
      registerId: regId,
      entryId: data.entryId,
      colId: String(data.colId),
      value: data.value,
      userName: data.userName || 'User',
      userId: data.userId || socket.id,
      color: data.color || getUserColor(data.userId || socket.id),
      clientSessionId: data.clientSessionId
    });
  });

  // 4. Cell Focus (Lock)
  socket.on('cell:focus', (data) => {
    if (!data || !data.registerId || data.entryId === undefined || data.colId === undefined) return;
    const regId = String(data.registerId);
    const colIdStr = String(data.colId);
    const cellKey = makeCellKey(data.entryId, colIdStr);
    const color = getUserColor(data.userId || socket.id);

    if (!cellLocks.has(regId)) cellLocks.set(regId, new Map());
    const locks = cellLocks.get(regId);

    const existing = locks.get(cellKey);
    if (existing && existing.socketId !== socket.id) {
      socket.emit('cell:lock-rejected', {
        registerId: regId,
        entryId: data.entryId,
        colId: colIdStr,
        lockedBy: existing.userName,
        color: existing.color
      });
      return;
    }

    locks.set(cellKey, {
      socketId: socket.id,
      entryId: data.entryId,
      colId: colIdStr,
      userName: data.userName || 'User',
      userId: data.userId || socket.id,
      color
    });
    io.to(`register:${regId}`).emit('cell:locked', {
      registerId: regId,
      entryId: data.entryId,
      colId: colIdStr,
      userName: data.userName || 'User',
      userId: data.userId || socket.id,
      color,
      lockerSocketId: socket.id
    });
  });

  // 5. Cell Blur (Unlock)
  socket.on('cell:blur', (data) => {
    if (!data || !data.registerId || data.entryId === undefined || data.colId === undefined) return;
    const regId = String(data.registerId);
    const colIdStr = String(data.colId);
    const cellKey = makeCellKey(data.entryId, colIdStr);

    if (cellLocks.has(regId)) {
      const locks = cellLocks.get(regId);
      const existing = locks.get(cellKey);
      if (existing && existing.socketId === socket.id) {
        locks.delete(cellKey);
        io.to(`register:${regId}`).emit('cell:unlocked', {
          registerId: regId,
          entryId: data.entryId,
          colId: colIdStr
        });
        if (locks.size === 0) cellLocks.delete(regId);
      }
    }
  });

  // 6. Direct Socket Row & Column Broadcast Events (fallback relay)
  socket.on('row:add', (data) => {
    if (!data || !data.registerId) return;
    socket.to(`register:${String(data.registerId)}`).emit('row:add', data);
  });

  socket.on('row:delete', (data) => {
    if (!data || !data.registerId) return;
    socket.to(`register:${String(data.registerId)}`).emit('row:delete', data);
  });

  socket.on('column:update', (data) => {
    if (!data || !data.registerId) return;
    socket.to(`register:${String(data.registerId)}`).emit('column:update', data);
  });

  // 7. Disconnect Cleanup
  socket.on('disconnect', () => {
    for (const registerId of joinedRegisters) {
      removeUserFromRoom(socket.id, registerId);
    }
    joinedRegisters.clear();
  });
});

// ----------------------------------------------------
// 1. HEALTH & SYSTEM CHECK
// ----------------------------------------------------
app.get('/api/health', async (req, res) => {
  try {
    const dbCheck = await query('SELECT NOW()');
    res.json({ status: 'ok', database: 'connected', websocket: 'active', ably: Boolean(getAblyRest()), timestamp: dbCheck.rows[0].now });
  } catch (err) {
    res.status(500).json({ status: 'error', database: err.message });
  }
});

async function handleAblyTokenRequest(req, res) {
  try {
    const ably = getAblyRest();
    if (!ably) {
      return res.status(501).json({ error: 'ably_not_configured' });
    }
    const userId = req.user?.id && req.user.id !== 'anonymous' ? String(req.user.id) : null;
    const clientId = userId || `guest_${crypto.randomBytes(6).toString('hex')}`;
    const isAdmin = hasAdminRights(req.user);

    const capability = {
      'register:*': ['publish', 'subscribe', 'presence'],
      'workspace:presence': ['subscribe', 'presence']
    };
    if (userId) {
      capability[`user:${userId}`] = ['subscribe'];
    }
    if (isAdmin) {
      capability['admin:events'] = ['subscribe'];
    }

    const tokenRequest = await ably.auth.createTokenRequest({
      clientId,
      ttl: 3600 * 1000,
      capability: JSON.stringify(capability)
    });
    res.json(tokenRequest);
  } catch (err) {
    console.error('[Ably Token Error]', err);
    res.status(500).json({ error: err.message });
  }
}

app.get('/api/ably/token', handleAblyTokenRequest);
app.post('/api/ably/token', handleAblyTokenRequest);

// ----------------------------------------------------
// DEFAULT BUILT-IN SYSTEM USERS (GUARANTEED AUTHENTICATION)
// ----------------------------------------------------
const DEFAULT_SYSTEM_USERS = [
  {
    id: 'usr_superadmin_1',
    name: 'Super Admin',
    email: 'admin@sjvps.com',
    password_hash: hashPassword('admin123'),
    raw_password: 'admin123',
    role: 'superadmin',
    status: 'active',
    phone: '9876543210',
    avatar: '',
    permissions: { canView: true, canEdit: true, canDownload: true, isAdmin: true, fullSheetAccess: true, canCreateSheets: true, canSelectBackDates: true }
  },
  {
    id: 'usr_collab_2',
    name: 'Collaborator 1',
    email: 'collaborator@sjvps.com',
    password_hash: hashPassword('user123'),
    raw_password: 'user123',
    role: 'user',
    status: 'active',
    phone: '9876543211',
    avatar: '',
    permissions: { canView: true, canEdit: true, canDownload: true, isAdmin: false, fullSheetAccess: true }
  }
];

function formatUser(u) {
  if (!u) return null;
  const basePerms = parseJsonField(u.permissions, {});
  const isSuperOrAdmin = u.role === 'superadmin' || u.role === 'admin';
  const permissions = isSuperOrAdmin
    ? { canView: true, canEdit: true, canDownload: true, isAdmin: true, fullSheetAccess: true, ...basePerms }
    : { canView: true, ...basePerms };
  return {
    id: u.id,
    name: u.name,
    email: u.email,
    role: u.role,
    status: u.status || 'active',
    phone: u.phone || '',
    avatar: u.avatar || '',
    createdAt: u.created_at,
    lastLogin: u.last_login,
    permissions
  };
}

async function createDbNotification(clientOrNull, userId, { title, message, type = 'info', meta = {} }) {
  if (!userId) return null;
  const runQuery = clientOrNull ? (sql, params) => clientOrNull.query(sql, params) : query;
  const id = Date.now().toString(36) + Math.random().toString(36).slice(2, 9);
  await runQuery(
    `INSERT INTO notifications (id, user_id, title, message, type, meta, is_read, created_at)
     VALUES ($1, $2, $3, $4, $5, $6, false, NOW())`,
    [id, String(userId), title, message, type, JSON.stringify(meta || {})]
  );
  return { id, userId: String(userId), title, message, type, meta, isRead: false, createdAt: new Date().toISOString() };
}

async function notifyAllAdmins(clientOrNull, { title, message, type = 'info', meta = {} }) {
  try {
    const runQuery = clientOrNull ? (sql, params) => clientOrNull.query(sql, params) : query;
    const adminsRes = await runQuery(
      "SELECT id FROM users WHERE (role IN ('admin', 'superadmin') OR (permissions->>'isAdmin')::boolean IS TRUE) AND (status IS NULL OR status = 'active')"
    );
    const created = [];
    for (const row of adminsRes.rows) {
      const n = await createDbNotification(clientOrNull, row.id, { title, message, type, meta });
      if (n) created.push(n);
    }
    if (created.length > 0) {
      const payload = { notifications: created, title, message, type, meta };
      io.emit('admin:notification', payload);
      await publishAblyEvent('admin:events', 'admin:notification', payload);
    }
  } catch (err) {
    console.warn('[notifyAllAdmins] Warning:', err.message);
  }
}

// ----------------------------------------------------
// 2. AUTHENTICATION, USER MANAGEMENT & ACCESS REQUESTS
// ----------------------------------------------------
app.post('/api/auth/login', async (req, res) => {
  try {
    const rawIdent = (req.body.email || req.body.username || req.body.name || req.body.userIdentifier || '').trim();
    const loginIdentifier = rawIdent.toLowerCase();
    const { password } = req.body || {};
    if (!loginIdentifier || !password) {
      return res.status(400).json({ error: 'Email/username and password are required' });
    }

    const hashedPassword = hashPassword(password);
    let lookupEmail = loginIdentifier;
    if (loginIdentifier === 'admin') lookupEmail = 'admin@sjvps.com';
    else if (loginIdentifier === 'collaborator' || loginIdentifier === 'user') lookupEmail = 'collaborator@sjvps.com';

    // 1. Always query PostgreSQL users table FIRST so real DB accounts, roles, passwords & statuses are authoritative
    const userRes = await query(
      `SELECT id, name, email, role, status, phone, avatar, permissions, password_hash, created_at, last_login
       FROM users
       WHERE LOWER(email) = LOWER($1)
          OR LOWER(name) = LOWER($2)
          OR (phone IS NOT NULL AND phone <> '' AND TRIM(phone) = TRIM($2))
       ORDER BY (role = 'superadmin') DESC, created_at ASC
       LIMIT 1`,
      [lookupEmail, rawIdent]
    );

    if (userRes.rowCount > 0) {
      const u = userRes.rows[0];
      if (u.status === 'pending') {
        return res.status(403).json({ error: 'Your account request is still pending admin approval.' });
      }
      if (u.status === 'blocked' || u.status === 'inactive' || u.status === 'rejected') {
        return res.status(403).json({ error: 'Account is deactivated or blocked. Contact your administrator.' });
      }

      const trimmedPassword = typeof password === 'string' ? password.trim() : '';
      const trimmedHash = trimmedPassword && trimmedPassword !== password ? hashPassword(trimmedPassword) : null;
      const passwordMatches =
        u.password_hash === hashedPassword ||
        u.password_hash === password ||
        (trimmedHash !== null && (u.password_hash === trimmedHash || u.password_hash === trimmedPassword));
      if (!passwordMatches) {
        return res.status(401).json({ error: 'Invalid username or password' });
      }

      await query('UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = $1', [u.id]).catch(() => {});
      const logId = Date.now().toString(36) + Math.random().toString(36).slice(2, 9);
      query(
        `INSERT INTO activity_logs (id, user_id, user_name, action, details, timestamp)
         VALUES ($1, $2, $3, $4, $5, NOW())`,
        [
          logId,
          u.id,
          u.name,
          u.role === 'admin' || u.role === 'superadmin' ? 'admin_login' : 'login',
          `User logged in: ${u.email}`
        ]
      ).catch(() => {});

      // Notify admins in-app when non-self user logs in
      notifyAllAdmins(null, {
        title: 'User Login Alert',
        message: `${u.name} (${u.email}) signed into the workspace`,
        type: 'info',
        meta: { userId: u.id, userName: u.name, userEmail: u.email, role: u.role, event: 'login' }
      }).catch(() => {});

      sendLoginNotificationEmail(u.email, u.name, u.role).catch(() => {});
      invalidateUserAuthCache(u.id);

      const formatted = formatUser({ ...u, last_login: new Date().toISOString() });
      const token = generateToken(formatted);
      return res.json({ token, user: formatted });
    }

    // 2. Check if there is a pending/rejected account access request for this identifier
    const reqCheck = await query(
      `SELECT status, admin_response FROM download_requests
       WHERE type = 'account_access'
         AND (LOWER(COALESCE(scope->>'email', '')) = LOWER($1) OR LOWER(COALESCE(user_name, '')) = LOWER($2))
       ORDER BY created_at DESC LIMIT 1`,
      [lookupEmail, rawIdent]
    ).catch(() => ({ rowCount: 0, rows: [] }));

    if (reqCheck.rowCount > 0) {
      const latestReq = reqCheck.rows[0];
      if (latestReq.status === 'pending') {
        return res.status(403).json({ error: 'Your login access request is pending administrator approval.' });
      }
      if (latestReq.status === 'rejected') {
        return res.status(403).json({
          error: latestReq.admin_response
            ? `Your login request was rejected: ${latestReq.admin_response}`
            : 'Your login request was rejected by the administrator.'
        });
      }
    }

    // 3. Fallback to built-in default accounts ONLY if user is absent from database
    const builtIn = DEFAULT_SYSTEM_USERS.find(u =>
      u.email.toLowerCase() === loginIdentifier ||
      u.name.toLowerCase() === loginIdentifier ||
      (loginIdentifier === 'admin' && u.email === 'admin@sjvps.com') ||
      (loginIdentifier === 'collaborator' && u.email === 'collaborator@sjvps.com') ||
      (loginIdentifier === 'user' && u.email === 'collaborator@sjvps.com')
    );

    if (builtIn && (password === builtIn.raw_password || hashedPassword === builtIn.password_hash)) {
      const formattedBuiltIn = formatUser(builtIn);
      const token = generateToken(formattedBuiltIn);
      return res.json({ token, user: formattedBuiltIn });
    }

    return res.status(401).json({ error: 'Invalid username or password' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Public endpoint: submit a login access request from LoginPage
app.post('/api/auth/request-access', async (req, res) => {
  try {
    const { name, email, phone, password, reason, requestedRole } = req.body || {};
    const cleanName = String(name || '').trim();
    const normEmail = String(email || '').trim().toLowerCase();
    const rawPassword = String(password || '');

    if (!cleanName || !normEmail || !rawPassword) {
      return res.status(400).json({ error: 'Full name, email, and password are required' });
    }
    if (rawPassword.length < 6) {
      return res.status(400).json({ error: 'Password must be at least 6 characters' });
    }

    const existingUser = await query('SELECT id, status FROM users WHERE LOWER(email) = LOWER($1)', [normEmail]);
    if (existingUser.rowCount > 0) {
      const st = existingUser.rows[0].status;
      if (st === 'active') {
        return res.status(400).json({ error: 'An active account with this email already exists. Please log in.' });
      }
    }

    const pendingReq = await query(
      `SELECT id FROM download_requests
       WHERE type = 'account_access' AND status = 'pending' AND LOWER(COALESCE(scope->>'email', '')) = LOWER($1)`,
      [normEmail]
    );
    if (pendingReq.rowCount > 0) {
      return res.status(400).json({ error: 'A login access request for this email is already pending approval.' });
    }

    const id = Date.now().toString(36) + Math.random().toString(36).slice(2, 9);
    const scope = {
      name: cleanName,
      email: normEmail,
      phone: String(phone || '').trim(),
      passwordHash: hashPassword(rawPassword),
      requestedRole: requestedRole === 'sheet_admin' ? 'sheet_admin' : 'user'
    };

    await query(
      `INSERT INTO download_requests (id, user_id, user_name, type, register_id, register_name, description, scope, status, created_at)
       VALUES ($1, $2, $3, 'account_access', NULL, $4, $5, $6, 'pending', NOW())`,
      [id, normEmail, cleanName, `Login Request (${normEmail})`, String(reason || 'Requesting workspace login access').trim(), JSON.stringify(scope)]
    );

    await notifyAllAdmins(null, {
      title: 'New Login Access Request',
      message: `${cleanName} (${normEmail}) requested login access`,
      type: 'info',
      meta: { requestId: id, email: normEmail, event: 'request_access' }
    });
    const reqUpdatedPayload = { requestId: id, type: 'account_access', status: 'pending' };
    io.emit('admin:requests-updated', reqUpdatedPayload);
    await publishAblyEvent('admin:events', 'admin:requests-updated', reqUpdatedPayload);

    res.status(201).json({
      id,
      status: 'pending',
      message: 'Your login access request has been sent to the administrator for approval.'
    });
  } catch (err) {
    console.error('[Request Access Error]', err);
    res.status(500).json({ error: err.message });
  }
});

// Public endpoint: check account access request status by email
app.get('/api/auth/request-status', async (req, res) => {
  try {
    const email = String(req.query.email || '').trim().toLowerCase();
    if (!email) return res.status(400).json({ error: 'Email is required' });

    const existingUser = await query('SELECT name, status FROM users WHERE LOWER(email) = LOWER($1)', [email]);
    if (existingUser.rowCount > 0) {
      const u = existingUser.rows[0];
      return res.json({
        status: u.status === 'active' ? 'approved' : u.status,
        name: u.name,
        adminResponse: u.status === 'active'
          ? 'Your account is active. You can sign in using your email and password.'
          : 'Your account is currently deactivated.'
      });
    }

    const reqRes = await query(
      `SELECT user_name, status, admin_response, created_at, responded_at
       FROM download_requests
       WHERE type = 'account_access' AND LOWER(COALESCE(scope->>'email', '')) = LOWER($1)
       ORDER BY created_at DESC LIMIT 1`,
      [email]
    );

    if (reqRes.rowCount === 0) {
      return res.json({ status: 'not_found' });
    }
    const r = reqRes.rows[0];
    return res.json({
      status: r.status,
      name: r.user_name,
      adminResponse: r.admin_response || '',
      createdAt: r.created_at,
      respondedAt: r.responded_at
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/auth/me', async (req, res) => {
  try {
    const uid = req.user?.id;
    if (!uid || uid === 'anonymous') {
      return res.status(401).json({ error: 'Not authenticated' });
    }
    invalidateUserAuthCache(uid);
    const userRes = await query(
      'SELECT id, name, email, role, status, phone, avatar, permissions, created_at, last_login FROM users WHERE id = $1',
      [String(uid)]
    );
    if (userRes.rowCount > 0) {
      const u = userRes.rows[0];
      if (u.status && u.status !== 'active') {
        return res.status(403).json({ error: 'Account is deactivated. Contact your administrator.' });
      }
      return res.json({ user: formatUser(u) });
    }
    const builtIn = DEFAULT_SYSTEM_USERS.find(u => u.id === String(uid));
    if (builtIn) {
      return res.json({ user: formatUser(builtIn) });
    }
    return res.status(401).json({ error: 'User session no longer exists' });
  } catch (err) {
    return res.json({ user: req.user });
  }
});

app.get('/api/auth/users', async (req, res) => {
  try {
    const usersRes = await query('SELECT id, name, email, role, status, phone, avatar, permissions, created_at, last_login FROM users ORDER BY name ASC');
    const parsed = usersRes.rows.map(formatUser);
    res.json({ users: parsed });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/auth/users', requireAdmin, async (req, res) => {
  try {
    const { id, name, email, password, role, status, phone, avatar, permissions } = req.body;
    if (!name || !email || !password) return res.status(400).json({ error: 'Name, email, and password are required' });

    const normEmail = email.toLowerCase().trim();
    const existing = await query('SELECT 1 FROM users WHERE LOWER(email) = LOWER($1)', [normEmail]);
    if (existing.rowCount > 0) return res.status(400).json({ error: 'Email already exists' });

    const newId = id || 'usr_' + Date.now();
    const hashedPassword = hashPassword(password);
    const userRole = role || 'user';
    const userPerms = permissions || {
      canView: true,
      canEdit: true,
      canDownload: userRole === 'admin' || userRole === 'superadmin',
      isAdmin: userRole === 'admin' || userRole === 'superadmin',
      fullSheetAccess: userRole === 'admin' || userRole === 'superadmin' || userRole === 'sheet_admin'
    };

    const userRes = await query(
      `INSERT INTO users (id, name, email, password_hash, role, status, phone, avatar, permissions)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
       RETURNING id, name, email, role, status, phone, avatar, permissions, created_at, last_login`,
      [newId, name.trim(), normEmail, hashedPassword, userRole, status || 'active', phone || '', avatar || '', JSON.stringify(userPerms)]
    );

    res.status(201).json({ user: formatUser(userRes.rows[0]), message: 'User created' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.put('/api/auth/users/:id', async (req, res) => {
  try {
    const userId = req.params.id;
    const data = req.body || {};
    const callerIsAdmin = hasAdminRights(req.user);
    const callerIsSelf = req.user && String(req.user.id) === String(userId);

    if (!callerIsAdmin && !callerIsSelf) {
      return res.status(403).json({ error: 'Admin authority required to modify other users' });
    }

    const curRes = await query('SELECT * FROM users WHERE id = $1', [userId]);
    if (curRes.rowCount === 0) return res.status(404).json({ error: 'User not found' });
    const cur = curRes.rows[0];

    const updates = [];
    const params = [];
    const pushUpdate = (col, val) => {
      params.push(val);
      updates.push(`${col} = $${params.length}`);
    };

    if (data.name !== undefined) pushUpdate('name', String(data.name).trim());
    if (data.phone !== undefined) pushUpdate('phone', String(data.phone));
    if (data.avatar !== undefined) pushUpdate('avatar', String(data.avatar));

    if (callerIsAdmin) {
      if (data.email !== undefined) pushUpdate('email', String(data.email).toLowerCase().trim());
      if (data.role !== undefined && cur.role !== 'superadmin') pushUpdate('role', data.role);
      if (data.status !== undefined && cur.role !== 'superadmin') pushUpdate('status', data.status);
      if (data.permissions !== undefined) pushUpdate('permissions', JSON.stringify(data.permissions || {}));
      if (data.password) pushUpdate('password_hash', hashPassword(String(data.password)));
    } else if (data.role !== undefined || data.status !== undefined || data.permissions !== undefined || data.password !== undefined) {
      return res.status(403).json({ error: 'Admin authority required for role, status, or permission changes' });
    }

    if (updates.length === 0) {
      return res.json({ user: formatUser(cur), message: 'No changes applied' });
    }

    params.push(userId);
    const updatedRes = await query(
      `UPDATE users SET ${updates.join(', ')} WHERE id = $${params.length}
       RETURNING id, name, email, role, status, phone, avatar, permissions, created_at, last_login`,
      params
    );

    invalidateUserAuthCache(userId);
    const formatted = formatUser(updatedRes.rows[0]);
    if (data.status !== undefined || data.permissions !== undefined || data.role !== undefined) {
      const permPayload = { userId: String(userId), user: formatted };
      io.emit('user:permissions-updated', permPayload);
      await publishAblyEvent(`user:${String(userId)}`, 'user:permissions-updated', permPayload);
    }
    res.json({ user: formatted, message: 'User updated successfully' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.put('/api/auth/users/:id/permissions', requireAdmin, async (req, res) => {
  try {
    const userId = req.params.id;
    const { permissions } = req.body || {};
    const updatedRes = await query(
      `UPDATE users SET permissions = $1 WHERE id = $2
       RETURNING id, name, email, role, status, phone, avatar, permissions, created_at, last_login`,
      [JSON.stringify(permissions || {}), userId]
    );
    if (updatedRes.rowCount === 0) return res.status(404).json({ error: 'User not found' });
    invalidateUserAuthCache(userId);
    const formatted = formatUser(updatedRes.rows[0]);
    const permPayload = { userId: String(userId), user: formatted };
    io.emit('user:permissions-updated', permPayload);
    await publishAblyEvent(`user:${String(userId)}`, 'user:permissions-updated', permPayload);
    res.json({ user: formatted, message: 'Permissions updated' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/auth/users/:id', requireAdmin, async (req, res) => {
  try {
    const targetId = req.params.id;
    const targetRes = await query('SELECT id, role FROM users WHERE id = $1', [targetId]);
    if (targetRes.rowCount > 0 && targetRes.rows[0].role === 'superadmin') {
      return res.status(403).json({ error: 'Super Admin account cannot be deleted' });
    }
    await query('DELETE FROM users WHERE id = $1', [targetId]);
    invalidateUserAuthCache(targetId);
    res.json({ message: 'User deleted' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/auth/change-password', async (req, res) => {
  try {
    const { userId, currentPassword, newPassword } = req.body || {};
    const callerIsAdmin = hasAdminRights(req.user);
    const targetId = (callerIsAdmin && userId) ? userId : req.user?.id;
    if (!targetId || targetId === 'anonymous' || !newPassword) {
      return res.status(400).json({ error: 'Authenticated user and new password are required' });
    }
    if (String(newPassword).length < 6) {
      return res.status(400).json({ error: 'New password must be at least 6 characters' });
    }

    const userRes = await query('SELECT id, password_hash FROM users WHERE id = $1', [targetId]);
    if (userRes.rowCount === 0) {
      return res.status(404).json({ error: 'User account not found' });
    }

    if (currentPassword || !callerIsAdmin) {
      if (!currentPassword) {
        return res.status(400).json({ error: 'Current password is required' });
      }
      const stored = userRes.rows[0].password_hash;
      if (stored !== hashPassword(currentPassword) && stored !== currentPassword) {
        return res.status(401).json({ error: 'Current password is incorrect' });
      }
    }

    const hashedNew = hashPassword(newPassword);
    await query('UPDATE users SET password_hash = $1 WHERE id = $2', [hashedNew, targetId]);
    invalidateUserAuthCache(targetId);
    res.json({ message: 'Password updated successfully' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ----------------------------------------------------
// 3. BUSINESSES & FOLDERS
// ----------------------------------------------------
app.get('/api/businesses', async (req, res) => {
  try {
    // Sync any business_id used by existing registers so the workspace list always finds its sheets
    await query(
      `INSERT INTO businesses (id, name, owner_id)
       SELECT DISTINCT business_id, 'AG Trust', 1
       FROM registers
       WHERE business_id IS NOT NULL
       ON CONFLICT (id) DO NOTHING`
    ).catch(() => {});

    const bizRes = await query(`
      SELECT b.*,
             (SELECT COUNT(*)::int FROM registers r WHERE r.business_id = b.id AND r.deleted_at IS NULL) AS reg_count
      FROM businesses b
      ORDER BY reg_count DESC, b.id ASC
    `);
    if (bizRes.rowCount === 0) {
      const newBiz = await query("INSERT INTO businesses (id, name, owner_id) VALUES (1, 'AG Trust', 1) ON CONFLICT (id) DO NOTHING RETURNING *");
      if (newBiz.rowCount > 0) {
        return res.json(newBiz.rows.map(b => ({ id: Number(b.id), name: b.name, ownerId: Number(b.owner_id || 1), createdAt: b.created_at })));
      }
      return res.json([{ id: 1, name: 'AG Trust', ownerId: 1, createdAt: new Date().toISOString() }]);
    }
    res.json(bizRes.rows.map(b => ({ id: Number(b.id), name: b.name, ownerId: Number(b.owner_id || 1), createdAt: b.created_at })));
  } catch (err) {
    res.json([{ id: 1, name: 'AG Trust', ownerId: 1, createdAt: new Date().toISOString() }]);
  }
});

app.post('/api/businesses', async (req, res) => {
  try {
    const { name } = req.body;
    if (!name) return res.status(400).json({ error: 'Business name is required' });
    const id = Date.now();
    await query('INSERT INTO businesses (id, name, owner_id, created_at) VALUES ($1, $2, 1, NOW())', [id, name]);
    res.status(201).json({ id, name, ownerId: 1, createdAt: new Date().toISOString() });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/folders', async (req, res) => {
  try {
    const businessId = parseBigInt(req.query.businessId);
    const foldersRes = businessId
      ? await query('SELECT * FROM folders WHERE (business_id = $1 OR business_id IS NULL) AND deleted_at IS NULL ORDER BY name ASC', [businessId])
      : await query('SELECT * FROM folders WHERE deleted_at IS NULL ORDER BY name ASC');
    const parsed = foldersRes.rows.map(f => ({
      ...f,
      id: toIdValue(f.id),
      businessId: Number(f.business_id || 1),
      createdAt: f.created_at
    }));
    res.json(parsed);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/folders', async (req, res) => {
  try {
    const { name, color, businessId } = req.body;
    if (!name) return res.status(400).json({ error: 'Folder name is required' });
    const id = Date.now();
    const busId = parseBigInt(businessId) || 1;
    const folderRes = await query(
      'INSERT INTO folders (id, business_id, name, color, created_at) VALUES ($1, $2, $3, $4, NOW()) RETURNING *',
      [id, busId, name, color || '#1a73e8']
    );
    const f = folderRes.rows[0];
    res.status(201).json({ ...f, id: toIdValue(f.id), businessId: Number(f.business_id || busId), createdAt: f.created_at });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.put('/api/folders/:id', async (req, res) => {
  try {
    const folderId = parseBigInt(req.params.id);
    const { name, color } = req.body;
    const folderRes = await query(
      'UPDATE folders SET name = COALESCE($1, name), color = COALESCE($2, color), updated_at = NOW() WHERE id = $3 RETURNING *',
      [name, color, folderId]
    );
    if (folderRes.rowCount === 0) return res.status(404).json({ error: 'Folder not found' });
    const f = folderRes.rows[0];
    res.json({ ...f, id: toIdValue(f.id), businessId: Number(f.business_id || 1), createdAt: f.created_at });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/folders/:id', async (req, res) => {
  try {
    const folderId = parseBigInt(req.params.id);
    await withTransaction(async (client) => {
      await client.query('UPDATE folders SET deleted_at = NOW() WHERE id = $1', [folderId]);
      await client.query('UPDATE registers SET folder_id = NULL WHERE folder_id = $1', [folderId]);
    });
    res.json({ success: true, message: 'Folder deleted' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ----------------------------------------------------
// 4. REGISTERS (CRUD + METADATA + REAL-TIME BROADCAST)
// ----------------------------------------------------
app.get('/api/registers', async (req, res) => {
  try {
    const businessId = parseBigInt(req.query.businessId);
    const regRes = businessId
      ? await query('SELECT * FROM registers WHERE business_id = $1 AND deleted_at IS NULL ORDER BY id ASC', [businessId])
      : await query('SELECT * FROM registers WHERE deleted_at IS NULL ORDER BY id ASC');
    res.json(regRes.rows.map(r => formatRegister(r)));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/registers/deleted', async (req, res) => {
  try {
    const businessId = parseBigInt(req.query.businessId);
    const regRes = businessId
      ? await query('SELECT * FROM registers WHERE business_id = $1 AND deleted_at IS NOT NULL ORDER BY deleted_at DESC', [businessId])
      : await query('SELECT * FROM registers WHERE deleted_at IS NOT NULL ORDER BY deleted_at DESC');
    res.json(regRes.rows.map(r => formatRegister(r)));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/registers/:id', async (req, res) => {
  try {
    const regId = parseBigInt(req.params.id);
    if (!regId) return res.status(400).json({ error: 'Invalid register ID' });
    const regRes = await query('SELECT * FROM registers WHERE id = $1', [regId]);
    if (regRes.rowCount === 0) return res.status(404).json({ error: 'Register not found' });

    const hasPagination = req.query.limit !== undefined || req.query.offset !== undefined;
    if (hasPagination) {
      const limit = Math.min(Math.max(parseInt(String(req.query.limit || '1500'), 10) || 1500, 1), 5000);
      const offset = Math.max(parseInt(String(req.query.offset || '0'), 10) || 0, 0);
      const [countRes, entriesRes] = await Promise.all([
        query('SELECT COUNT(*)::int AS total FROM entries WHERE register_id = $1', [regId]),
        query('SELECT * FROM entries WHERE register_id = $1 ORDER BY row_number ASC, id ASC LIMIT $2 OFFSET $3', [regId, limit, offset])
      ]);
      const totalEntries = Number(countRes.rows[0]?.total ?? 0);
      const parsedEntries = entriesRes.rows.map(formatEntry);
      const payload = formatRegister(regRes.rows[0], parsedEntries);
      payload.totalEntries = totalEntries;
      payload.offset = offset;
      payload.limit = limit;
      payload.hasMore = offset + parsedEntries.length < totalEntries;
      return res.json(payload);
    }

    const entriesRes = await query('SELECT * FROM entries WHERE register_id = $1 ORDER BY row_number ASC, id ASC', [regId]);
    const parsedEntries = entriesRes.rows.map(formatEntry);

    res.json(formatRegister(regRes.rows[0], parsedEntries));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/registers/:id/columns', async (req, res) => {
  try {
    const regId = parseBigInt(req.params.id);
    if (!regId) return res.status(400).json({ error: 'Invalid register ID' });
    const regRes = await query('SELECT * FROM registers WHERE id = $1', [regId]);
    if (regRes.rowCount === 0) return res.status(404).json({ error: 'Register not found' });
    res.json(formatRegister(regRes.rows[0]));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/registers', async (req, res) => {
  try {
    const data = req.body;
    const regId = parseBigInt(data.id) || Date.now();
    const name = data.name || 'Untitled Register';
    const folderId = data.folderId !== undefined ? parseBigInt(data.folderId) : (data.folder_id !== undefined ? parseBigInt(data.folder_id) : null);
    const icon = data.icon || 'BookOpen';
    const iconColor = data.iconColor || data.icon_color || '#1a73e8';
    const category = data.category || 'General';
    const template = data.template || '';
    const businessId = parseBigInt(data.businessId || data.business_id) || 1;
    const columns = normalizeColumns(data.columns || []);
    const pages = normalizePages(data.pages || []);
    const sharedWith = data.sharedWith || data.shared_with || [];

    const regRes = await query(
      `INSERT INTO registers (id, business_id, name, folder_id, icon, icon_color, category, template, columns, pages, shared_with, entry_count, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW(), NOW())
       RETURNING *`,
      [regId, businessId, name, folderId, icon, iconColor, category, template, JSON.stringify(columns), JSON.stringify(pages), JSON.stringify(sharedWith), 0]
    );

    res.status(201).json(formatRegister(regRes.rows[0], []));
  } catch (err) {
    console.error('[Create Register Error]', err);
    res.status(500).json({ error: err.message });
  }
});

// PUT /api/registers/:id (Update metadata, columns, pages, settings, and optionally entries)
app.put('/api/registers/:id', async (req, res) => {
  try {
    const regId = parseBigInt(req.params.id);
    if (!regId) return res.status(400).json({ error: 'Invalid register ID' });
    const data = req.body || {};
    const entriesIncluded = Array.isArray(data.entries);

    const { updatedReg, syncedEntries } = await withTransaction(async (client) => {
      const existingRes = await client.query('SELECT * FROM registers WHERE id = $1 FOR UPDATE', [regId]);
      if (existingRes.rowCount === 0) throw new Error('Register not found');
      const existing = existingRes.rows[0];

      const cols = data.columns !== undefined ? normalizeColumns(data.columns) : normalizeColumns(existing.columns);
      const pages = data.pages !== undefined ? normalizePages(data.pages) : normalizePages(existing.pages);
      const sharedWith = data.sharedWith !== undefined ? data.sharedWith : (data.shared_with !== undefined ? data.shared_with : parseJsonField(existing.shared_with, []));
      const deletedItems = data.deletedItems !== undefined ? data.deletedItems : (data.deleted_items !== undefined ? data.deleted_items : parseJsonField(existing.deleted_items, []));

      const hasExplicitFolder = data.folderId !== undefined || data.folder_id !== undefined;
      const nextFolderId = hasExplicitFolder
        ? (data.folderId === null || data.folder_id === null ? null : parseBigInt(data.folderId ?? data.folder_id))
        : existing.folder_id;

      const nextShareLink = data.shareLink !== undefined ? data.shareLink : (data.share_link !== undefined ? data.share_link : existing.share_link);

      const updateRes = await client.query(`
        UPDATE registers SET
          name = COALESCE($1, name),
          folder_id = $2,
          icon = COALESCE($3, icon),
          icon_color = COALESCE($4, icon_color),
          category = COALESCE($5, category),
          columns = $6,
          pages = $7,
          shared_with = $8,
          deleted_items = $9,
          share_link = $10,
          entry_count = COALESCE($11, entry_count),
          updated_at = NOW()
        WHERE id = $12
        RETURNING *
      `, [
        data.name ?? null,
        nextFolderId,
        data.icon ?? null,
        data.iconColor || data.icon_color || null,
        data.category ?? null,
        JSON.stringify(cols),
        JSON.stringify(pages),
        JSON.stringify(sharedWith || []),
        JSON.stringify(deletedItems || []),
        nextShareLink ?? null,
        data.entryCount !== undefined ? Number(data.entryCount) : null,
        regId
      ]);

      let savedEntries;
      if (entriesIncluded) {
        const entries = data.entries;
        const entryIds = entries.map(e => parseBigInt(e.id)).filter(id => id !== null);

        if (entryIds.length > 0) {
          const placeholders = entryIds.map((_, idx) => `$${idx + 2}`).join(', ');
          await client.query(`DELETE FROM entries WHERE register_id = $1 AND id NOT IN (${placeholders})`, [regId, ...entryIds]);
        } else {
          await client.query('DELETE FROM entries WHERE register_id = $1', [regId]);
        }

        const CHUNK_SIZE = 250;
        for (let offset = 0; offset < entries.length; offset += CHUNK_SIZE) {
          const chunk = entries.slice(offset, offset + CHUNK_SIZE);
          const valueClauses = [];
          const params = [];
          let p = 1;
          for (let j = 0; j < chunk.length; j++) {
            const entry = chunk[j];
            const eId = parseBigInt(entry.id) || generateServerEntryId();
            const rNum = Number(entry.rowNumber || entry.row_number || (offset + j + 1));
            const cellsJson = JSON.stringify(entry.cells || {});
            const stylesJson = entry.cellStyles ? JSON.stringify(entry.cellStyles) : null;
            const pIdx = Number(entry.pageIndex ?? entry.page_index ?? 0);
            valueClauses.push(`($${p++}, $${p++}, $${p++}, $${p++}, $${p++}, $${p++})`);
            params.push(eId, regId, rNum, cellsJson, stylesJson, pIdx);
          }
          await client.query(`
            INSERT INTO entries (id, register_id, row_number, cells, cell_styles, page_index)
            VALUES ${valueClauses.join(', ')}
            ON CONFLICT (id) DO UPDATE SET
              row_number = EXCLUDED.row_number,
              cells = EXCLUDED.cells,
              cell_styles = EXCLUDED.cell_styles,
              page_index = EXCLUDED.page_index
          `, params);
        }

        await client.query('UPDATE registers SET entry_count = $1 WHERE id = $2', [entries.length, regId]);
        const freshEntries = await client.query('SELECT * FROM entries WHERE register_id = $1 ORDER BY row_number ASC, id ASC', [regId]);
        savedEntries = freshEntries.rows.map(formatEntry);
        updateRes.rows[0].entry_count = savedEntries.length;
      }

      return { updatedReg: updateRes.rows[0], syncedEntries: savedEntries };
    });

    const formatted = formatRegister(updatedReg, syncedEntries);
    const formattedMeta = formatRegister(updatedReg);
    const regIdStr = String(regId);

    // Emit single authoritative update AFTER transaction commit
    const socketPayload = {
      registerId: regIdStr,
      columns: formatted.columns,
      pages: formatted.pages,
      entries: syncedEntries,
      register: formattedMeta,
      clientSessionId: data.clientSessionId
    };
    io.to(`register:${regIdStr}`).emit('register:update', socketPayload);

    const ablyPayload = {
      registerId: regIdStr,
      columns: formatted.columns,
      pages: formatted.pages,
      register: formattedMeta,
      clientSessionId: data.clientSessionId
    };
    if (entriesIncluded && Array.isArray(syncedEntries)) {
      if (Buffer.byteLength(JSON.stringify(syncedEntries), 'utf8') <= 48000) {
        ablyPayload.entries = syncedEntries;
      } else {
        ablyPayload.syncRequired = true;
      }
    }
    await publishAblyEvent(`register:${regIdStr}`, 'register:update', ablyPayload);

    res.json(formatted);
  } catch (err) {
    console.error('[PUT Register Error]', err);
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/registers/:id', async (req, res) => {
  try {
    const regId = parseBigInt(req.params.id);
    if (!regId) return res.status(400).json({ error: 'Invalid register ID' });
    const { deletedBy, deletedByEmail, deletedById } = req.body || {};
    await query(
      'UPDATE registers SET deleted_at = NOW(), deleted_by = $1, deleted_by_email = $2, deleted_by_id = $3 WHERE id = $4',
      [deletedBy || req.user?.name || null, deletedByEmail || req.user?.email || null, deletedById ? String(deletedById) : null, regId]
    );
    const deletePayload = { registerId: String(regId) };
    io.to(`register:${String(regId)}`).emit('register:delete', deletePayload);
    await publishAblyEvent(`register:${String(regId)}`, 'register:delete', deletePayload);
    res.json({ success: true, message: 'Register moved to recycle bin' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/registers/:id/restore', async (req, res) => {
  try {
    const regId = parseBigInt(req.params.id);
    await query('UPDATE registers SET deleted_at = NULL, deleted_by = NULL WHERE id = $1', [regId]);
    res.json({ success: true, message: 'Register restored' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/registers/:id/hard', async (req, res) => {
  try {
    const regId = parseBigInt(req.params.id);
    await withTransaction(async (client) => {
      await client.query('DELETE FROM entries WHERE register_id = $1', [regId]);
      await client.query('DELETE FROM registers WHERE id = $1', [regId]);
    });
    res.json({ success: true, message: 'Register permanently deleted' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ----------------------------------------------------
// 5. ATOMIC SINGLE-CELL UPSERT & LINKED COLUMN SYNC
// ----------------------------------------------------
app.patch('/api/registers/:id/cells', async (req, res) => {
  try {
    const regIdNum = parseBigInt(req.params.id);
    if (!regIdNum) return res.status(400).json({ error: 'Invalid register ID' });
    const regId = String(regIdNum);
    const { rowId, colKey, value, updatedBy, sourceEntryId, rowNumber, clientSessionId } = req.body;

    const col = colKey || req.body.colId;
    if (!col) return res.status(400).json({ error: 'Missing colKey' });

    const valStr = JSON.stringify(value ?? '');
    const userStr = updatedBy || req.user?.name || 'User';

    const result = await withTransaction(async (client) => {
      let entryRow = null;
      const targetId = parseBigInt(rowId || sourceEntryId);

      if (targetId) {
        const lockRes = await client.query(
          'SELECT * FROM entries WHERE register_id = $1 AND id = $2 FOR UPDATE',
          [regIdNum, targetId]
        );
        if (lockRes.rowCount > 0) entryRow = lockRes.rows[0];
      }

      if (!entryRow && rowNumber) {
        const lockRes = await client.query(
          'SELECT * FROM entries WHERE register_id = $1 AND row_number = $2 FOR UPDATE',
          [regIdNum, Number(rowNumber)]
        );
        if (lockRes.rowCount > 0) entryRow = lockRes.rows[0];
      }

      if (!entryRow) {
        await client.query(`
          INSERT INTO registers (id, business_id, name, entry_count, columns, pages)
          VALUES ($1, 1, 'Sample Multi-User Register', 0, '[]', '[{"id":1,"name":"Page 1","index":0}]')
          ON CONFLICT (id) DO NOTHING
        `, [regIdNum]);

        const newEntryId = targetId || generateServerEntryId();
        const insertRes = await client.query(
          `INSERT INTO entries (id, register_id, row_number, cells)
           VALUES ($1, $2, $3, jsonb_build_object($4::text, $5::jsonb))
           ON CONFLICT (id) DO UPDATE SET cells = jsonb_set(COALESCE(entries.cells, '{}'::jsonb), ARRAY[$4::text], $5::jsonb)
           RETURNING *`,
          [newEntryId, regIdNum, Number(rowNumber) || 1, String(col), valStr]
        );
        entryRow = insertRes.rows[0];
      } else {
        const updateRes = await client.query(
          `UPDATE entries
           SET cells = jsonb_set(COALESCE(cells, '{}'::jsonb), ARRAY[$1::text], $2::jsonb)
           WHERE id = $3 RETURNING *`,
          [String(col), valStr, entryRow.id]
        );
        entryRow = updateRes.rows[0];
      }

      await client.query('UPDATE registers SET updated_at = NOW() WHERE id = $1', [regIdNum]);
      return entryRow;
    });

    const parsedEntry = formatEntry(result);

    // Broadcast AFTER commit
    const cellPayload = {
      registerId: regId,
      entryId: parsedEntry.id,
      rowNumber: parsedEntry.rowNumber,
      colKey: String(col),
      value: value ?? '',
      updatedBy: userStr,
      clientSessionId
    };
    io.to(`register:${regId}`).emit('cell:update', cellPayload);
    await publishAblyEvent(`register:${regId}`, 'cell:update', cellPayload);

    res.json({ success: true, cell: parsedEntry });
  } catch (err) {
    console.error('[Patch Cell Error]', err);
    res.status(500).json({ error: err.message });
  }
});

// Linked Column Sync (target register lookup by __sourceEntryId or fallback row_number, auto-creates missing target row)
app.post('/api/registers/:id/entries/sync-cell', async (req, res) => {
  try {
    const targetRegId = parseBigInt(req.params.id);
    if (!targetRegId) return res.status(400).json({ error: 'Invalid register ID' });
    const { columnId, colKey: rawColKey, sourceEntryId, rowNumber, value, clientSessionId } = req.body;
    const colKey = String(columnId || rawColKey || '');
    if (!colKey) return res.status(400).json({ error: 'Missing columnId' });

    const valStr = JSON.stringify(value ?? '');
    const { updatedRow, createdRow } = await withTransaction(async (client) => {
      await client.query('SELECT id FROM registers WHERE id = $1 FOR UPDATE', [targetRegId]);

      let resUpdate = await client.query(`
        UPDATE entries
        SET cells = jsonb_set(COALESCE(cells, '{}'::jsonb), ARRAY[$1::text], $2::jsonb)
        WHERE register_id = $3 AND cells->>'__sourceEntryId' = $4
        RETURNING *
      `, [colKey, valStr, targetRegId, String(sourceEntryId)]);

      if (resUpdate.rowCount === 0 && rowNumber) {
        resUpdate = await client.query(`
          UPDATE entries
          SET cells = jsonb_set(
            jsonb_set(COALESCE(cells, '{}'::jsonb), ARRAY[$1::text], $2::jsonb),
            ARRAY['__sourceEntryId'],
            $4::jsonb
          )
          WHERE register_id = $3 AND row_number = $5
          RETURNING *
        `, [colKey, valStr, targetRegId, JSON.stringify(String(sourceEntryId)), Number(rowNumber)]);
      }

      if (resUpdate.rowCount === 0) {
        const maxRowRes = await client.query(
          'SELECT COALESCE(MAX(row_number), 0)::int AS max_row FROM entries WHERE register_id = $1',
          [targetRegId]
        );
        const maxRow = maxRowRes.rows[0]?.max_row || 0;
        const desiredRow = Math.max(maxRow + 1, Number(rowNumber) || (maxRow + 1));

        // Create any intermediate rows if rowNumber jumps ahead, keeping alignment smooth
        for (let r = maxRow + 1; r < desiredRow; r++) {
          await client.query(
            `INSERT INTO entries (id, register_id, row_number, cells, page_index)
             VALUES ($1, $2, $3, $4, 0)`,
            [generateServerEntryId(), targetRegId, r, JSON.stringify({})]
          );
        }

        const newCells = {
          [colKey]: value ?? '',
          ...(sourceEntryId !== undefined && sourceEntryId !== null ? { __sourceEntryId: String(sourceEntryId) } : {})
        };
        const resInsert = await client.query(
          `INSERT INTO entries (id, register_id, row_number, cells, page_index)
           VALUES ($1, $2, $3, $4, 0)
           RETURNING *`,
          [generateServerEntryId(), targetRegId, desiredRow, JSON.stringify(newCells)]
        );
        await client.query(
          'UPDATE registers SET entry_count = (SELECT COUNT(*) FROM entries WHERE register_id = $1), updated_at = NOW() WHERE id = $1',
          [targetRegId]
        );
        return { updatedRow: resInsert.rows[0], createdRow: true };
      }

      await client.query('UPDATE registers SET updated_at = NOW() WHERE id = $1', [targetRegId]);
      return { updatedRow: resUpdate.rows[0] || null, createdRow: false };
    });

    let parsed = null;
    if (updatedRow) {
      parsed = formatEntry(updatedRow);
      if (createdRow) {
        const rowAddPayload = {
          registerId: String(targetRegId),
          entry: parsed,
          clientSessionId
        };
        io.to(`register:${String(targetRegId)}`).emit('row:add', rowAddPayload);
        await publishAblyEvent(`register:${String(targetRegId)}`, 'row:add', rowAddPayload);
      } else {
        const syncCellPayload = {
          registerId: String(targetRegId),
          entryId: parsed.id,
          rowNumber: parsed.rowNumber,
          colKey,
          value: value ?? '',
          updatedBy: req.user?.name || 'System Link',
          clientSessionId
        };
        io.to(`register:${String(targetRegId)}`).emit('cell:update', syncCellPayload);
        await publishAblyEvent(`register:${String(targetRegId)}`, 'cell:update', syncCellPayload);
      }
    }

    res.json({ success: true, updated: !!updatedRow, created: createdRow, entry: parsed });
  } catch (err) {
    console.error('[Sync Cell Error]', err);
    res.status(500).json({ error: err.message });
  }
});

// ----------------------------------------------------
// 6. ROW CREATION, INSERTION, UPDATE, DELETION & CENTRALIZED SYNC
// ----------------------------------------------------
// Add row(s)
app.post('/api/registers/:id/entries', async (req, res) => {
  try {
    const regIdNum = parseBigInt(req.params.id);
    if (!regIdNum) return res.status(400).json({ error: 'Invalid register ID' });
    const regId = String(regIdNum);
    const body = req.body || {};
    const entriesList = Array.isArray(body.entries) ? body.entries : [body];
    const clientSessionId = body.clientSessionId;
    const clientTempId = body.clientTempId;

    const insertedRows = await withTransaction(async (client) => {
      await client.query(`
        INSERT INTO registers (id, business_id, name, entry_count, columns, pages)
        VALUES ($1, 1, 'Sample Multi-User Register', 0, '[]', '[{"id":1,"name":"Page 1","index":0}]')
        ON CONFLICT (id) DO NOTHING
      `, [regIdNum]);

      const regLockRes = await client.query('SELECT columns FROM registers WHERE id = $1 FOR UPDATE', [regIdNum]);
      const autoIncColKey = getAutoIncrementColKey(regLockRes.rows[0]?.columns);

      const maxRowRes = await client.query(
        'SELECT COALESCE(MAX(row_number), 0) as max_row FROM entries WHERE register_id = $1',
        [regIdNum]
      );
      let currentMaxRow = parseInt(maxRowRes.rows[0].max_row, 10);

      let currentMaxAutoInc = 0;
      if (autoIncColKey) {
        const maxAutoRes = await client.query(
          `SELECT COALESCE(MAX(CASE WHEN (cells->>$1) ~ '^[0-9]+$' THEN (cells->>$1)::bigint ELSE 0 END), 0) AS max_auto
           FROM entries WHERE register_id = $2`,
          [autoIncColKey, regIdNum]
        );
        currentMaxAutoInc = parseInt(maxAutoRes.rows[0]?.max_auto || '0', 10);
      }

      const rows = [];
      for (const item of entriesList) {
        currentMaxRow += 1;
        const nextRow = currentMaxRow;

        const rawId = parseBigInt(item.id);
        const entryId = (rawId && rawId > 0) ? rawId : generateServerEntryId();

        const rowNum = (Array.isArray(body.entries) && item.rowNumber && Number.isInteger(Number(item.rowNumber)))
          ? Number(item.rowNumber)
          : nextRow;

        const cells = { ...(item.cells || {}) };
        if (autoIncColKey && !cells[autoIncColKey]) {
          currentMaxAutoInc += 1;
          cells[autoIncColKey] = String(currentMaxAutoInc);
        }

        const cellsJson = JSON.stringify(cells);
        const stylesJson = item.cellStyles ? JSON.stringify(item.cellStyles) : null;
        const pageIdx = Number(item.pageIndex ?? item.page_index ?? 0);

        const resInsert = await client.query(
          `INSERT INTO entries (id, register_id, row_number, cells, cell_styles, page_index)
           VALUES ($1, $2, $3, $4, $5, $6)
           ON CONFLICT (id) DO UPDATE SET
             cells = EXCLUDED.cells,
             cell_styles = EXCLUDED.cell_styles,
             row_number = EXCLUDED.row_number,
             page_index = EXCLUDED.page_index
           RETURNING *`,
          [entryId, regIdNum, rowNum, cellsJson, stylesJson, pageIdx]
        );

        rows.push(formatEntry(resInsert.rows[0]));
      }

      await client.query(
        'UPDATE registers SET entry_count = (SELECT COUNT(*) FROM entries WHERE register_id = $1), updated_at = NOW() WHERE id = $1',
        [regIdNum]
      );
      return rows;
    });

    // Broadcast AFTER transaction commits
    for (const parsed of insertedRows) {
      io.to(`register:${regId}`).emit('row:add', {
        registerId: regId,
        entry: parsed,
        clientSessionId,
        clientTempId
      });
    }
    if (insertedRows.length <= 20) {
      for (const parsed of insertedRows) {
        await publishAblyEvent(`register:${regId}`, 'row:add', {
          registerId: regId,
          entry: parsed,
          clientSessionId,
          clientTempId
        });
      }
    } else {
      await publishAblyEvent(`register:${regId}`, 'register:update', {
        registerId: regId,
        syncRequired: true,
        clientSessionId
      });
    }

    if (!Array.isArray(body.entries)) {
      return res.status(201).json(insertedRows[0]);
    }
    res.status(201).json({ success: true, entries: insertedRows });
  } catch (err) {
    console.error('[Add Entry Error]', err);
    res.status(500).json({ error: err.message });
  }
});

// Insert row at exact index (shifts subsequent rows atomically in PostgreSQL)
app.post('/api/registers/:id/entries/insert', async (req, res) => {
  try {
    const regIdNum = parseBigInt(req.params.id);
    if (!regIdNum) return res.status(400).json({ error: 'Invalid register ID' });
    const regId = String(regIdNum);
    const { cells = {}, pageIndex = 0, atIndex = 0, clientSessionId } = req.body || {};

    const { insertedEntry, targetRowNumber } = await withTransaction(async (client) => {
      const regLockRes = await client.query('SELECT columns FROM registers WHERE id = $1 FOR UPDATE', [regIdNum]);
      if (regLockRes.rowCount === 0) throw new Error('Register not found');

      const autoIncColKey = getAutoIncrementColKey(regLockRes.rows[0]?.columns);
      const countRes = await client.query('SELECT COUNT(*)::int AS cnt FROM entries WHERE register_id = $1', [regIdNum]);
      const totalCount = countRes.rows[0]?.cnt || 0;
      const clampedIndex = Math.max(0, Math.min(Number(atIndex) || 0, totalCount));
      const rowNum = clampedIndex + 1;

      await client.query(
        'UPDATE entries SET row_number = row_number + 1 WHERE register_id = $1 AND row_number >= $2',
        [regIdNum, rowNum]
      );

      const finalCells = { ...cells };
      if (autoIncColKey && !finalCells[autoIncColKey]) {
        const maxAutoRes = await client.query(
          `SELECT COALESCE(MAX(CASE WHEN (cells->>$1) ~ '^[0-9]+$' THEN (cells->>$1)::bigint ELSE 0 END), 0) AS max_auto
           FROM entries WHERE register_id = $2`,
          [autoIncColKey, regIdNum]
        );
        finalCells[autoIncColKey] = String(parseInt(maxAutoRes.rows[0]?.max_auto || '0', 10) + 1);
      }

      const newEntryId = parseBigInt(req.body?.id) || generateServerEntryId();
      const resInsert = await client.query(
        `INSERT INTO entries (id, register_id, row_number, cells, page_index)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING *`,
        [newEntryId, regIdNum, rowNum, JSON.stringify(finalCells), Number(pageIndex || 0)]
      );

      await client.query(
        'UPDATE registers SET entry_count = (SELECT COUNT(*) FROM entries WHERE register_id = $1), updated_at = NOW() WHERE id = $1',
        [regIdNum]
      );

      return { insertedEntry: formatEntry(resInsert.rows[0]), targetRowNumber: rowNum };
    });

    const rowInsertPayload = {
      registerId: regId,
      entry: insertedEntry,
      atIndex: targetRowNumber - 1,
      rowNumber: targetRowNumber,
      clientSessionId
    };
    io.to(`register:${regId}`).emit('row:insert', rowInsertPayload);
    await publishAblyEvent(`register:${regId}`, 'row:insert', rowInsertPayload);

    res.status(201).json(insertedEntry);
  } catch (err) {
    console.error('[Insert Entry Error]', err);
    res.status(500).json({ error: err.message });
  }
});

// Update row cells / styles
app.put('/api/registers/:id/entries/:entryId', async (req, res) => {
  try {
    const { id: regIdRaw, entryId: entryIdRaw } = req.params;
    const regIdNum = parseBigInt(regIdRaw);
    const entryIdNum = parseBigInt(entryIdRaw);
    if (!regIdNum || !entryIdNum) return res.status(400).json({ error: 'Invalid register or entry ID' });
    const regId = String(regIdNum);

    const { cells, cellStyles, row_number, rowNumber, pageIndex, clientSessionId } = req.body || {};

    const updatedRow = await withTransaction(async (client) => {
      await client.query(`
        INSERT INTO registers (id, business_id, name, entry_count, columns, pages)
        VALUES ($1, 1, 'Sample Multi-User Register', 0, '[]', '[{"id":1,"name":"Page 1","index":0}]')
        ON CONFLICT (id) DO NOTHING
      `, [regIdNum]);

      const existingRes = await client.query(
        'SELECT * FROM entries WHERE register_id = $1 AND id = $2 FOR UPDATE',
        [regIdNum, entryIdNum]
      );

      let nextCellStylesJson = null;
      if (cellStyles !== undefined && cellStyles !== null) {
        const existingStyles = existingRes.rowCount > 0 ? parseJsonField(existingRes.rows[0].cell_styles, {}) : {};
        const mergedStyles = { ...existingStyles };
        for (const [k, v] of Object.entries(cellStyles)) {
          if (!v || Object.keys(v).length === 0) {
            delete mergedStyles[k];
          } else {
            mergedStyles[k] = v;
          }
        }
        nextCellStylesJson = JSON.stringify(mergedStyles);
      }

      let resUpdate;
      if (existingRes.rowCount > 0) {
        resUpdate = await client.query(
          `UPDATE entries
           SET cells = COALESCE(cells, '{}'::jsonb) || $1::jsonb,
               cell_styles = COALESCE($2::jsonb, cell_styles),
               row_number = COALESCE($3, row_number),
               page_index = COALESCE($4, page_index)
           WHERE register_id = $5 AND id = $6 RETURNING *`,
          [
            JSON.stringify(cells || {}),
            nextCellStylesJson,
            row_number ?? rowNumber ?? null,
            pageIndex ?? null,
            regIdNum,
            entryIdNum
          ]
        );
      } else {
        resUpdate = await client.query(
          `INSERT INTO entries (id, register_id, row_number, cells, cell_styles, page_index)
           VALUES ($1, $2, $3, $4, $5, $6)
           ON CONFLICT (id) DO UPDATE SET
             cells = entries.cells || EXCLUDED.cells,
             cell_styles = COALESCE(EXCLUDED.cell_styles, entries.cell_styles)
           RETURNING *`,
          [
            entryIdNum,
            regIdNum,
            row_number ?? rowNumber ?? 1,
            JSON.stringify(cells || {}),
            nextCellStylesJson,
            pageIndex ?? 0
          ]
        );
      }

      await client.query('UPDATE registers SET updated_at = NOW() WHERE id = $1', [regIdNum]);
      return resUpdate.rows[0];
    });

    const parsedRow = formatEntry(updatedRow);

    // Broadcast AFTER commit
    const entryUpdatePayload = {
      registerId: regId,
      entryId: parsedRow.id,
      rowNumber: parsedRow.rowNumber,
      cells: cells && Object.keys(cells).length > 0 ? cells : undefined,
      cellStyles: cellStyles || undefined,
      updatedBy: req.user?.name || 'User',
      clientSessionId
    };
    io.to(`register:${regId}`).emit('cell:update', entryUpdatePayload);
    await publishAblyEvent(`register:${regId}`, 'cell:update', entryUpdatePayload);

    res.json(parsedRow);
  } catch (err) {
    console.error('[PUT Entry Error]', err);
    res.status(500).json({ error: err.message });
  }
});

// Delete single row
app.delete('/api/registers/:id/entries/:entryId', async (req, res) => {
  try {
    const regIdNum = parseBigInt(req.params.id);
    const entryIdNum = parseBigInt(req.params.entryId);
    if (!regIdNum || !entryIdNum) return res.status(400).json({ error: 'Invalid register or entry ID' });
    const regId = String(regIdNum);
    const clientSessionId = req.query?.clientSessionId || req.body?.clientSessionId;

    await withTransaction(async (client) => {
      await client.query('SELECT id FROM registers WHERE id = $1 FOR UPDATE', [regIdNum]);
      await client.query('DELETE FROM entries WHERE register_id = $1 AND id = $2', [regIdNum, entryIdNum]);
      await renumberEntriesInDb(client, regIdNum);
      await client.query(
        'UPDATE registers SET entry_count = (SELECT COUNT(*) FROM entries WHERE register_id = $1), updated_at = NOW() WHERE id = $1',
        [regIdNum]
      );
    });

    const rowDeletePayload = {
      registerId: regId,
      entryId: entryIdNum,
      clientSessionId
    };
    io.to(`register:${regId}`).emit('row:delete', rowDeletePayload);
    await publishAblyEvent(`register:${regId}`, 'row:delete', rowDeletePayload);

    res.json({ success: true, message: 'Entry deleted successfully', entryId: entryIdNum });
  } catch (err) {
    console.error('[Delete Entry Error]', err);
    res.status(500).json({ error: err.message });
  }
});

// Bulk Delete rows
app.post('/api/registers/:id/entries/bulk-delete', async (req, res) => {
  try {
    const regIdNum = parseBigInt(req.params.id);
    if (!regIdNum) return res.status(400).json({ error: 'Invalid register ID' });
    const regId = String(regIdNum);
    const { entryIds, deletedItems, clientSessionId } = req.body || {};

    const cleanIds = Array.isArray(entryIds) ? entryIds.map(parseBigInt).filter(id => id !== null) : [];
    if (cleanIds.length === 0) {
      return res.status(400).json({ error: 'entryIds array is required' });
    }

    await withTransaction(async (client) => {
      const regRes = await client.query('SELECT deleted_items FROM registers WHERE id = $1 FOR UPDATE', [regIdNum]);
      const placeholders = cleanIds.map((_, idx) => `$${idx + 2}`).join(', ');
      await client.query(`DELETE FROM entries WHERE register_id = $1 AND id IN (${placeholders})`, [regIdNum, ...cleanIds]);
      await renumberEntriesInDb(client, regIdNum);

      let nextDeletedItems = parseJsonField(regRes.rows[0]?.deleted_items, []);
      if (Array.isArray(deletedItems) && deletedItems.length > 0) {
        nextDeletedItems = [...nextDeletedItems, ...deletedItems];
      }

      await client.query(
        'UPDATE registers SET entry_count = (SELECT COUNT(*) FROM entries WHERE register_id = $1), deleted_items = $2, updated_at = NOW() WHERE id = $1',
        [regIdNum, JSON.stringify(nextDeletedItems)]
      );
    });

    const bulkDeletePayload = {
      registerId: regId,
      entryIds: cleanIds,
      clientSessionId
    };
    io.to(`register:${regId}`).emit('row:bulk-delete', bulkDeletePayload);
    await publishAblyEvent(`register:${regId}`, 'row:bulk-delete', bulkDeletePayload);

    res.json({ success: true, message: `Deleted ${cleanIds.length} entries`, entryIds: cleanIds });
  } catch (err) {
    console.error('[Bulk Delete Entries Error]', err);
    res.status(500).json({ error: err.message });
  }
});

// Reorder rows
app.post('/api/registers/:id/reorder-rows', async (req, res) => {
  try {
    const regIdNum = parseBigInt(req.params.id);
    if (!regIdNum) return res.status(400).json({ error: 'Invalid register ID' });
    const regId = String(regIdNum);
    const { sortedEntryIds, clientSessionId } = req.body || {};

    if (!Array.isArray(sortedEntryIds)) {
      return res.status(400).json({ error: 'sortedEntryIds array is required' });
    }

    await withTransaction(async (client) => {
      await client.query('SELECT id FROM registers WHERE id = $1 FOR UPDATE', [regIdNum]);
      for (let i = 0; i < sortedEntryIds.length; i++) {
        const eId = parseBigInt(sortedEntryIds[i]);
        if (eId !== null) {
          await client.query(
            'UPDATE entries SET row_number = $1 WHERE register_id = $2 AND id = $3',
            [i + 1, regIdNum, eId]
          );
        }
      }
      await client.query('UPDATE registers SET updated_at = NOW() WHERE id = $1', [regIdNum]);
    });

    const reorderPayload = {
      registerId: regId,
      sortedEntryIds,
      clientSessionId
    };
    io.to(`register:${regId}`).emit('row:reorder', reorderPayload);
    if (Buffer.byteLength(JSON.stringify(reorderPayload), 'utf8') <= 48000) {
      await publishAblyEvent(`register:${regId}`, 'row:reorder', reorderPayload);
    } else {
      await publishAblyEvent(`register:${regId}`, 'register:update', {
        registerId: regId,
        syncRequired: true,
        clientSessionId
      });
    }

    res.json({ success: true, message: 'Rows reordered successfully' });
  } catch (err) {
    console.error('[Reorder Rows Error]', err);
    res.status(500).json({ error: err.message });
  }
});

// ----------------------------------------------------
// 7. SAVED TEMPLATES, FORMULAS, DROPDOWNS & LOGS
// ----------------------------------------------------
app.get('/api/saved-formulas', async (req, res) => {
  try {
    const businessId = parseBigInt(req.query.businessId);
    const result = businessId
      ? await query('SELECT * FROM saved_formulas WHERE business_id = $1 ORDER BY created_at DESC', [businessId])
      : await query('SELECT * FROM saved_formulas ORDER BY name ASC');
    res.json({
      formulas: result.rows.map(r => ({
        id: r.id,
        businessId: Number(r.business_id || 1),
        name: r.name,
        formula: r.formula,
        createdBy: r.created_by,
        createdAt: r.created_at
      }))
    });
  } catch (err) {
    res.json({ formulas: [] });
  }
});

app.post('/api/saved-formulas', async (req, res) => {
  try {
    const { id, businessId, name, formula, createdBy } = req.body;
    const newId = id || ('f_' + Date.now());
    const busId = parseBigInt(businessId) || 1;
    await query(
      'INSERT INTO saved_formulas (id, business_id, name, formula, created_by, created_at) VALUES ($1, $2, $3, $4, $5, NOW())',
      [newId, busId, name, formula, createdBy || null]
    );
    res.status(201).json({ id: newId, businessId: busId, name, formula, createdBy: createdBy || null });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/saved-formulas/:id', async (req, res) => {
  try {
    await query('DELETE FROM saved_formulas WHERE id = $1', [req.params.id]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/saved-dropdowns', async (req, res) => {
  try {
    const businessId = parseBigInt(req.query.businessId);
    const result = businessId
      ? await query('SELECT * FROM saved_dropdowns WHERE business_id = $1 ORDER BY created_at DESC', [businessId])
      : await query('SELECT * FROM saved_dropdowns ORDER BY name ASC');
    res.json({
      dropdowns: result.rows.map(r => ({
        id: r.id,
        businessId: Number(r.business_id || 1),
        name: r.name,
        options: r.options,
        createdBy: r.created_by,
        createdAt: r.created_at
      }))
    });
  } catch (err) {
    res.json({ dropdowns: [] });
  }
});

app.post('/api/saved-dropdowns', async (req, res) => {
  try {
    const { id, businessId, name, options, createdBy } = req.body;
    const newId = id || ('dd_' + Date.now());
    const busId = parseBigInt(businessId) || 1;
    const optsVal = typeof options === 'string' ? options : JSON.stringify(options || []);
    await query(
      'INSERT INTO saved_dropdowns (id, business_id, name, options, created_by, created_at) VALUES ($1, $2, $3, $4, $5, NOW())',
      [newId, busId, name, optsVal, createdBy || null]
    );
    res.status(201).json({ id: newId, businessId: busId, name, options: optsVal, createdBy: createdBy || null });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/saved-dropdowns/:id', async (req, res) => {
  try {
    await query('DELETE FROM saved_dropdowns WHERE id = $1', [req.params.id]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/saved-templates', async (req, res) => {
  try {
    const businessId = parseBigInt(req.query.businessId);
    const result = businessId
      ? await query('SELECT * FROM saved_templates WHERE business_id = $1 ORDER BY created_at DESC', [businessId])
      : await query('SELECT * FROM saved_templates ORDER BY created_at DESC');
    res.json({
      templates: result.rows.map(r => ({
        id: r.id,
        businessId: Number(r.business_id || 1),
        name: r.name,
        columns: parseJsonField(r.columns, []),
        createdAt: r.created_at
      }))
    });
  } catch (err) {
    res.json({ templates: [] });
  }
});

app.post('/api/saved-templates', async (req, res) => {
  try {
    const { businessId, name, columns } = req.body;
    const id = Date.now().toString(36) + Math.random().toString(36).slice(2, 9);
    const busId = parseBigInt(businessId) || 1;
    await query(
      'INSERT INTO saved_templates (id, business_id, name, columns, created_at) VALUES ($1, $2, $3, $4, NOW())',
      [id, busId, (name || '').trim(), typeof columns === 'string' ? columns : JSON.stringify(columns || [])]
    );
    res.status(201).json({ id, businessId: busId, name: (name || '').trim(), columns: typeof columns === 'string' ? JSON.parse(columns) : columns });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/saved-templates/:id', async (req, res) => {
  try {
    await query('DELETE FROM saved_templates WHERE id = $1', [req.params.id]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Activity Logs (supports both /api/activity and /api/activity-logs)
async function fetchActivityLogs(req, res) {
  try {
    const { registerId, entryId, userId, date, startDate, endDate } = req.query;
    const limitVal = parseInt(req.query.limit || '2000', 10);
    const offsetVal = parseInt(req.query.offset || '0', 10);

    let queryText = 'SELECT * FROM activity_logs';
    const params = [];
    const conditions = [];

    if (registerId && registerId !== 'all') {
      params.push(String(registerId));
      conditions.push(`register_id = $${params.length}`);
    }
    if (entryId) {
      params.push(String(entryId));
      conditions.push(`entry_id = $${params.length}`);
    }
    if (userId && userId !== 'all') {
      params.push(String(userId));
      conditions.push(`(user_id = $${params.length} OR user_name = $${params.length})`);
    }
    if (date) {
      params.push(String(date));
      conditions.push(`(timestamp::timestamptz AT TIME ZONE 'Asia/Kolkata')::date = $${params.length}::date`);
    }
    if (startDate) {
      params.push(String(startDate));
      conditions.push(`(timestamp::timestamptz AT TIME ZONE 'Asia/Kolkata')::date >= $${params.length}::date`);
    }
    if (endDate) {
      params.push(String(endDate));
      conditions.push(`(timestamp::timestamptz AT TIME ZONE 'Asia/Kolkata')::date <= $${params.length}::date`);
    }
    if (conditions.length > 0) {
      queryText += ' WHERE ' + conditions.join(' AND ');
    }
    const safeLimit = isNaN(limitVal) ? 2000 : limitVal;
    const safeOffset = isNaN(offsetVal) ? 0 : offsetVal;
    queryText += ` ORDER BY timestamp DESC NULLS LAST LIMIT ${safeLimit} OFFSET ${safeOffset}`;

    const result = await query(queryText, params);
    const activities = result.rows.map(r => ({
      id: r.id,
      userId: r.user_id,
      userName: r.user_name,
      action: r.action,
      details: r.details,
      timestamp: r.timestamp || r.created_at,
      registerId: r.register_id,
      registerName: r.register_name,
      entryId: r.entry_id ? Number(r.entry_id) : undefined
    }));
    res.json({ activities, logs: activities });
  } catch (err) {
    res.json({ activities: [], logs: [] });
  }
}

app.get('/api/activity', fetchActivityLogs);
app.get('/api/activity-logs', fetchActivityLogs);

app.get('/api/activity/user/:userId', async (req, res) => {
  try {
    const result = await query('SELECT * FROM activity_logs WHERE user_id = $1 ORDER BY timestamp DESC NULLS LAST', [req.params.userId]);
    const activities = result.rows.map(r => ({
      id: r.id,
      userId: r.user_id,
      userName: r.user_name,
      action: r.action,
      details: r.details,
      timestamp: r.timestamp || r.created_at,
      registerId: r.register_id,
      registerName: r.register_name,
      entryId: r.entry_id ? Number(r.entry_id) : undefined
    }));
    res.json({ activities });
  } catch (err) {
    res.json({ activities: [] });
  }
});

async function createActivityLog(req, res) {
  try {
    const data = req.body || {};
    const id = data.id || Date.now().toString(36) + Math.random().toString(36).slice(2, 9);
    const userId = data.userId || (req.user?.id !== 'anonymous' ? req.user?.id : null);
    const userName = data.userName || req.user?.name || 'User';
    const ts = data.timestamp ? new Date(data.timestamp).toISOString() : new Date().toISOString();
    await query(
      `INSERT INTO activity_logs (id, user_id, user_name, action, details, timestamp, register_id, register_name, entry_id)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      [
        id,
        userId || null,
        userName,
        data.action || '',
        data.details || '',
        ts,
        data.registerId ? String(data.registerId) : null,
        data.registerName || null,
        data.entryId ? String(data.entryId) : null
      ]
    );
    res.status(201).json({ success: true, id });
  } catch (err) {
    res.status(201).json({ success: false });
  }
}

app.post('/api/activity', createActivityLog);
app.post('/api/activity-logs', createActivityLog);

// ----------------------------------------------------
// 8. PRESENCE & HEARTBEAT
// ----------------------------------------------------
app.post('/api/presence/heartbeat', (req, res) => {
  res.json({ status: 'ok', timestamp: Date.now() });
});

app.get('/api/presence/online', (req, res) => {
  const onlineUsers = [];
  for (const [regId, users] of activeRoomUsers.entries()) {
    for (const u of users.values()) {
      onlineUsers.push({ ...u, registerId: regId });
    }
  }
  res.json({ count: onlineUsers.length || 1, users: onlineUsers });
});

// ----------------------------------------------------
// 9. DASHBOARD CONFIGURATION & SHORTCUTS
// ----------------------------------------------------
app.get('/api/dashboard-config', async (req, res) => {
  try {
    const businessId = parseBigInt(req.query.businessId) || 1;
    const result = await query('SELECT * FROM dashboard_configurations WHERE business_id = $1', [businessId]);
    if (result.rowCount === 0) {
      return res.json({ configuredSumMetrics: [], shortcutsOrder: [] });
    }
    const row = result.rows[0];
    res.json({
      configuredSumMetrics: parseJsonField(row.configured_sum_metrics, []),
      shortcutsOrder: parseJsonField(row.shortcuts_order, [])
    });
  } catch (err) {
    res.json({ configuredSumMetrics: [], shortcutsOrder: [] });
  }
});

app.post('/api/dashboard-config', async (req, res) => {
  try {
    const { businessId, configuredSumMetrics, shortcutsOrder } = req.body;
    const busId = parseBigInt(businessId) || 1;
    await query(`
      INSERT INTO dashboard_configurations (business_id, configured_sum_metrics, shortcuts_order)
      VALUES ($1, $2, $3)
      ON CONFLICT (business_id) DO UPDATE SET
        configured_sum_metrics = EXCLUDED.configured_sum_metrics,
        shortcuts_order = EXCLUDED.shortcuts_order
    `, [busId, JSON.stringify(configuredSumMetrics || []), JSON.stringify(shortcutsOrder || [])]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/saved-shortcuts', async (req, res) => {
  try {
    const businessId = parseBigInt(req.query.businessId) || 1;
    const result = await query('SELECT * FROM saved_register_shortcuts WHERE business_id = $1 ORDER BY created_at DESC', [businessId]);
    res.json({
      shortcuts: result.rows.map(r => ({
        id: r.id,
        businessId: Number(r.business_id),
        name: r.name,
        registerId: Number(r.register_id),
        registerName: r.register_name,
        searchQuery: r.search_query || '',
        filters: parseJsonField(r.filters, []),
        createdAt: r.created_at
      }))
    });
  } catch (err) {
    res.json({ shortcuts: [] });
  }
});

app.post('/api/saved-shortcuts', async (req, res) => {
  try {
    const data = req.body;
    const id = Date.now().toString(36) + Math.random().toString(36).slice(2, 9);
    await query(`
      INSERT INTO saved_register_shortcuts (id, business_id, name, register_id, register_name, search_query, filters, created_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
    `, [
      id,
      parseBigInt(data.businessId) || 1,
      (data.name || 'Shortcut').trim(),
      parseBigInt(data.registerId) || 1,
      data.registerName || '',
      data.searchQuery || '',
      JSON.stringify(data.filters || [])
    ]);
    res.status(201).json({ id, ...data });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.put('/api/saved-shortcuts/:id', async (req, res) => {
  try {
    const name = (req.body?.name || '').trim();
    if (!name) return res.status(400).json({ error: 'name is required' });
    await query('UPDATE saved_register_shortcuts SET name = $1 WHERE id = $2', [name, req.params.id]);
    res.json({ id: req.params.id, name });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/saved-shortcuts/:id', async (req, res) => {
  try {
    await query('DELETE FROM saved_register_shortcuts WHERE id = $1', [req.params.id]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ----------------------------------------------------
// 10. REQUESTS, NOTIFICATIONS & RECYCLE BIN
// ----------------------------------------------------
function formatRequest(r) {
  const scope = parseJsonField(r.scope, {});
  // Strip passwordHash before returning to client
  const safeScope = scope && typeof scope === 'object' ? { ...scope } : {};
  if ('passwordHash' in safeScope) {
    delete safeScope.passwordHash;
  }
  return {
    id: r.id,
    userId: r.user_id,
    userName: r.user_name,
    type: r.type,
    registerId: r.register_id ? Number(r.register_id) : undefined,
    registerName: r.register_name,
    description: r.description,
    scope: safeScope,
    status: r.status,
    createdAt: r.created_at,
    adminResponse: r.admin_response,
    respondedAt: r.responded_at
  };
}

app.post('/api/requests', async (req, res) => {
  try {
    const data = req.body || {};
    const uid = (req.user && req.user.id !== 'anonymous') ? String(req.user.id) : (data.userId ? String(data.userId) : null);
    const uName = (req.user && req.user.id !== 'anonymous') ? req.user.name : (data.userName || 'User');
    const reqType = data.type || 'download';
    const regName = data.registerName || 'Workspace';
    const desc = String(data.description || '').trim();
    if (!desc) return res.status(400).json({ error: 'Description / reason is required' });

    const id = data.id || Date.now().toString(36) + Math.random().toString(36).slice(2, 9);
    const inserted = await query(
      `INSERT INTO download_requests (id, user_id, user_name, type, register_id, register_name, description, scope, status, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending', NOW())
       RETURNING *`,
      [id, uid, uName, reqType, data.registerId ? String(data.registerId) : null, regName, desc, JSON.stringify(data.scope || {})]
    );

    await notifyAllAdmins(null, {
      title: `New ${reqType.replace(/_/g, ' ')} Request`,
      message: `${uName} requested ${reqType.replace(/_/g, ' ')} for "${regName}"`,
      type: 'info',
      meta: { requestId: id, type: reqType, registerId: data.registerId, registerName: regName }
    });
    const reqCreatedPayload = { requestId: id, type: reqType, status: 'pending' };
    io.emit('admin:requests-updated', reqCreatedPayload);
    await publishAblyEvent('admin:events', 'admin:requests-updated', reqCreatedPayload);

    res.status(201).json({ id, request: formatRequest(inserted.rows[0]) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/requests/my', async (req, res) => {
  try {
    const targetUserId = (req.query.userId ? String(req.query.userId) : null) || (req.user?.id !== 'anonymous' ? String(req.user.id) : null);
    if (!targetUserId) return res.json({ requests: [] });
    const result = await query('SELECT * FROM download_requests WHERE user_id = $1 ORDER BY created_at DESC', [targetUserId]);
    res.json({ requests: result.rows.map(formatRequest) });
  } catch {
    res.json({ requests: [] });
  }
});

app.get('/api/requests/pending', async (req, res) => {
  try {
    const result = await query("SELECT * FROM download_requests WHERE status = 'pending' ORDER BY created_at DESC");
    res.json({ requests: result.rows.map(formatRequest) });
  } catch {
    res.json({ requests: [] });
  }
});

app.get(['/api/requests', '/api/requests/all'], async (req, res) => {
  try {
    const result = await query('SELECT * FROM download_requests ORDER BY created_at DESC');
    res.json({ requests: result.rows.map(formatRequest) });
  } catch {
    res.json({ requests: [] });
  }
});

app.post('/api/requests/:id/respond', requireAdmin, async (req, res) => {
  try {
    const { status, adminResponse, adminName } = req.body || {};
    if (status !== 'approved' && status !== 'rejected') {
      return res.status(400).json({ error: 'Invalid status. Must be approved or rejected.' });
    }

    const result = await withTransaction(async (client) => {
      const reqRes = await client.query('SELECT * FROM download_requests WHERE id = $1 FOR UPDATE', [req.params.id]);
      if (reqRes.rowCount === 0) {
        const e = new Error('Request not found');
        e.statusCode = 404;
        throw e;
      }
      const reqRow = reqRes.rows[0];
      const scope = parseJsonField(reqRow.scope, {});
      let affectedUser = null;

      await client.query(
        'UPDATE download_requests SET status = $1, admin_response = $2, responded_at = NOW() WHERE id = $3',
        [status, adminResponse || '', req.params.id]
      );

      if (status === 'approved') {
        if (reqRow.type === 'account_access' && scope?.email) {
          const normEmail = String(scope.email).trim().toLowerCase();
          const uName = scope.name || reqRow.user_name || normEmail;
          const uPhone = scope.phone || '';
          const uRole = scope.requestedRole === 'sheet_admin' ? 'sheet_admin' : 'user';
          const uPassHash = scope.passwordHash || hashPassword('user123');
          const defaultPerms = {
            canView: true,
            canEdit: false,
            canDownload: false,
            isAdmin: false,
            fullSheetAccess: true
          };

          const existUser = await client.query('SELECT * FROM users WHERE LOWER(email) = LOWER($1)', [normEmail]);
          if (existUser.rowCount > 0) {
            const uUpd = await client.query(
              `UPDATE users SET status = 'active', name = COALESCE(NULLIF($1, ''), name), phone = COALESCE(NULLIF($2, ''), phone), password_hash = COALESCE($3, password_hash)
               WHERE id = $4
               RETURNING id, name, email, role, status, phone, avatar, permissions, created_at, last_login`,
              [uName, uPhone, uPassHash, existUser.rows[0].id]
            );
            affectedUser = formatUser(uUpd.rows[0]);
          } else {
            const newUid = 'usr_' + Date.now();
            const uIns = await client.query(
              `INSERT INTO users (id, name, email, password_hash, role, status, phone, avatar, permissions)
               VALUES ($1, $2, $3, $4, $5, 'active', $6, '', $7)
               RETURNING id, name, email, role, status, phone, avatar, permissions, created_at, last_login`,
              [newUid, uName, normEmail, uPassHash, uRole, uPhone, JSON.stringify(defaultPerms)]
            );
            affectedUser = formatUser(uIns.rows[0]);
          }
          // Link request to created user id so they can view it after logging in
          await client.query('UPDATE download_requests SET user_id = $1 WHERE id = $2', [affectedUser.id, req.params.id]);
        } else if (reqRow.user_id && ['download', 'edit_access', 'view_access', 'sheet_access'].includes(reqRow.type)) {
          const uRes = await client.query('SELECT * FROM users WHERE id = $1 FOR UPDATE', [String(reqRow.user_id)]);
          if (uRes.rowCount > 0) {
            const curUser = uRes.rows[0];
            const perms = parseJsonField(curUser.permissions, {});
            const regKey = reqRow.register_id ? String(reqRow.register_id) : null;

            if (regKey && Array.isArray(perms.allowedRegisters) && !perms.allowedRegisters.map(String).includes(regKey)) {
              perms.allowedRegisters = [...perms.allowedRegisters, regKey];
            }
            if (reqRow.type === 'download') {
              perms.canDownload = true;
              if (regKey && perms.downloadRestrictions && typeof perms.downloadRestrictions === 'object') {
                delete perms.downloadRestrictions[regKey];
              }
            } else if (reqRow.type === 'edit_access') {
              perms.canEdit = true;
              if (regKey && perms.editRestrictions && typeof perms.editRestrictions === 'object') {
                delete perms.editRestrictions[regKey];
              }
            } else if (reqRow.type === 'view_access' || reqRow.type === 'sheet_access') {
              perms.canView = true;
            }

            const uUpd = await client.query(
              `UPDATE users SET permissions = $1 WHERE id = $2
               RETURNING id, name, email, role, status, phone, avatar, permissions, created_at, last_login`,
              [JSON.stringify(perms), curUser.id]
            );
            affectedUser = formatUser(uUpd.rows[0]);
          }
        }
      }

      // Notify requesting user in DB
      const notifyTargetId = affectedUser?.id || reqRow.user_id;
      let userNotif = null;
      if (notifyTargetId) {
        const label = reqRow.type.replace(/_/g, ' ');
        const msg = status === 'approved'
          ? `Your ${label} request (${reqRow.register_name || 'Workspace'}) was approved${adminName ? ` by ${adminName}` : ''}.${adminResponse ? ` Note: ${adminResponse}` : ''}`
          : `Your ${label} request (${reqRow.register_name || 'Workspace'}) was rejected${adminName ? ` by ${adminName}` : ''}.${adminResponse ? ` Reason: ${adminResponse}` : ''}`;
        userNotif = await createDbNotification(client, notifyTargetId, {
          title: status === 'approved' ? 'Request Approved' : 'Request Rejected',
          message: msg,
          type: status === 'approved' ? 'success' : 'error',
          meta: { requestId: reqRow.id, requestType: reqRow.type, status, registerId: reqRow.register_id }
        });
      }

      return { reqRow, affectedUser, userNotif };
    });

    if (result.affectedUser) {
      invalidateUserAuthCache(result.affectedUser.id);
      const permUpdPayload = { userId: String(result.affectedUser.id), user: result.affectedUser };
      io.emit('user:permissions-updated', permUpdPayload);
      await publishAblyEvent(`user:${String(result.affectedUser.id)}`, 'user:permissions-updated', permUpdPayload);
    }
    if (result.userNotif) {
      io.emit('user:notification', result.userNotif);
      await publishAblyEvent(`user:${String(result.userNotif.userId)}`, 'user:notification', result.userNotif);
    }
    const respondReqPayload = { requestId: req.params.id, status };
    io.emit('admin:requests-updated', respondReqPayload);
    await publishAblyEvent('admin:events', 'admin:requests-updated', respondReqPayload);

    res.json({
      message: `Request status set to ${status}`,
      user: result.affectedUser || undefined
    });
  } catch (err) {
    res.status(err.statusCode || 500).json({ error: err.message });
  }
});

app.delete('/api/requests/:id', async (req, res) => {
  try {
    const reqRes = await query('SELECT id, user_id, status FROM download_requests WHERE id = $1', [req.params.id]);
    if (reqRes.rowCount === 0) return res.status(404).json({ error: 'Request not found' });
    const row = reqRes.rows[0];
    const isAdmin = hasAdminRights(req.user);
    const isOwner = req.user && String(req.user.id) === String(row.user_id);
    if (!isAdmin && !isOwner) {
      return res.status(403).json({ error: 'Not authorized to remove this request' });
    }
    await query('DELETE FROM download_requests WHERE id = $1', [req.params.id]);
    const delReqPayload = { requestId: req.params.id, deleted: true };
    io.emit('admin:requests-updated', delReqPayload);
    await publishAblyEvent('admin:events', 'admin:requests-updated', delReqPayload);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/notifications', async (req, res) => {
  try {
    const result = await query('SELECT * FROM notifications WHERE user_id = $1 ORDER BY created_at DESC', [req.query.userId]);
    res.json({
      notifications: result.rows.map(r => ({
        id: r.id,
        userId: r.user_id,
        title: r.title,
        message: r.message,
        type: r.type,
        meta: r.meta || {},
        isRead: r.is_read,
        createdAt: r.created_at
      }))
    });
  } catch {
    res.json({ notifications: [] });
  }
});

app.post('/api/notifications', async (req, res) => {
  try {
    const data = req.body || {};
    const id = data.id || Date.now().toString(36) + Math.random().toString(36).slice(2, 9);
    await query(
      'INSERT INTO notifications (id, user_id, title, message, type, meta, is_read, created_at) VALUES ($1, $2, $3, $4, $5, $6, false, NOW())',
      [id, data.userId, data.title, data.message, data.type, JSON.stringify(data.meta || {})]
    );
    res.status(201).json({ id });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.put('/api/notifications/:id/read', async (req, res) => {
  try {
    await query('UPDATE notifications SET is_read = true WHERE id = $1', [req.params.id]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/notifications/read-all', async (req, res) => {
  try {
    await query('UPDATE notifications SET is_read = true WHERE user_id = $1', [req.body?.userId]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/recycle-bin', async (req, res) => {
  try {
    const businessId = parseBigInt(req.query.businessId) || 1;
    const result = await query('SELECT deleted_items FROM registers WHERE business_id = $1', [businessId]);
    const allItems = [];
    for (const row of result.rows) {
      const items = parseJsonField(row.deleted_items, []);
      if (Array.isArray(items)) allItems.push(...items);
    }
    allItems.sort((a, b) => (b.deletedAt || '').localeCompare(a.deletedAt || ''));
    res.json({ deletedItems: allItems, items: allItems });
  } catch (err) {
    res.json({ deletedItems: [], items: [] });
  }
});

// ----------------------------------------------------
// 11. BACKUPS, RESTORE, EMAIL EXPORT & CRON
// ----------------------------------------------------
app.get('/api/backups', async (req, res) => {
  try {
    const businessId = parseBigInt(req.query.businessId);
    if (!businessId) return res.status(400).json({ error: 'businessId is required' });
    const result = await query(
      'SELECT id, business_id, created_at, label, register_count, folder_count, total_entries, size_kb FROM backups WHERE business_id = $1 ORDER BY created_at DESC',
      [businessId]
    );
    res.json(result.rows.map(r => ({
      id: r.id,
      businessId: Number(r.business_id),
      createdAt: r.created_at,
      label: r.label,
      registerCount: r.register_count,
      folderCount: r.folder_count,
      totalEntries: r.total_entries,
      sizeKb: r.size_kb
    })));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/backups', async (req, res) => {
  try {
    const { businessId: rawBizId, label } = req.body || {};
    const businessId = parseBigInt(rawBizId);
    if (!businessId) return res.status(400).json({ error: 'businessId is required' });

    const foldersRes = await query('SELECT * FROM folders WHERE business_id = $1 ORDER BY name ASC', [businessId]);
    const folders = foldersRes.rows.map(r => ({
      id: toIdValue(r.id),
      businessId: Number(r.business_id),
      name: r.name,
      createdAt: r.created_at
    }));

    const regsRes = await query('SELECT * FROM registers WHERE business_id = $1 AND deleted_at IS NULL ORDER BY name ASC', [businessId]);
    const validRegisters = [];
    for (const row of regsRes.rows) {
      const regId = parseBigInt(row.id);
      const entriesRes = await query('SELECT * FROM entries WHERE register_id = $1 ORDER BY row_number ASC, id ASC', [regId]);
      validRegisters.push(formatRegister(row, entriesRes.rows.map(formatEntry)));
    }

    const totalEntries = validRegisters.reduce((sum, r) => sum + (r.entries?.length ?? 0), 0);
    const id = `backup_${Date.now()}`;
    const now = new Date().toISOString();
    const displayLabel = label || `Backup ${new Date(now).toLocaleDateString('en-IN', { day: 'numeric', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' })}`;

    const snapshot = {
      meta: {
        id,
        businessId,
        createdAt: now,
        label: displayLabel,
        registerCount: validRegisters.length,
        folderCount: folders.length,
        totalEntries,
        sizeKb: 0
      },
      registers: validRegisters,
      folders
    };

    const jsonSize = Math.round(JSON.stringify(snapshot).length / 1024);
    snapshot.meta.sizeKb = jsonSize;

    await query(
      `INSERT INTO backups (id, business_id, created_at, label, register_count, folder_count, total_entries, size_kb, snapshot)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      [id, businessId, now, displayLabel, validRegisters.length, folders.length, totalEntries, jsonSize, JSON.stringify(snapshot)]
    );

    const logId = Date.now().toString(36) + Math.random().toString(36).slice(2, 9);
    await query(
      `INSERT INTO activity_logs (id, user_id, user_name, action, details, timestamp)
       VALUES ($1, $2, $3, 'Backup Created', $4, NOW())`,
      [logId, req.user?.id || 'system', req.user?.name || 'System', `Created backup: ${displayLabel} (${validRegisters.length} registers, ${totalEntries} entries)`]
    ).catch(() => {});

    res.status(201).json(snapshot.meta);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/backups/send-email', async (req, res) => {
  try {
    const { targetEmail = 'jackyme1291@gmail.com', businessId: rawBizId } = req.body || {};
    const mailBizId = parseBigInt(rawBizId);
    if (!mailBizId) return res.status(400).json({ error: 'businessId is required' });

    const [foldersRes, regsRes, entriesRes] = await Promise.all([
      query('SELECT id, name FROM folders WHERE business_id = $1 ORDER BY name ASC', [mailBizId]),
      query('SELECT id, name, folder_id, columns FROM registers WHERE business_id = $1 AND deleted_at IS NULL ORDER BY name ASC', [mailBizId]),
      query(
        `SELECT e.id, e.register_id, e.row_number, e.cells
         FROM entries e
         INNER JOIN registers r ON r.id = e.register_id
         WHERE r.business_id = $1 AND r.deleted_at IS NULL
         ORDER BY e.register_id, e.row_number ASC, e.id ASC`,
        [mailBizId]
      )
    ]);

    const folderMap = new Map();
    foldersRes.rows.forEach(r => folderMap.set(Number(r.id), r.name));

    const entriesByRegId = new Map();
    entriesRes.rows.forEach(e => {
      const rid = Number(e.register_id);
      if (!entriesByRegId.has(rid)) entriesByRegId.set(rid, []);
      entriesByRegId.get(rid).push(e);
    });

    const { default: JSZip } = await import('jszip');
    const zip = new JSZip();
    const totalEntries = entriesRes.rows.length;

    const escCsv = (val) => {
      const s = String(val ?? '');
      if (s.includes(',') || s.includes('"') || s.includes('\n')) {
        return '"' + s.replace(/"/g, '""') + '"';
      }
      return s;
    };

    for (const row of regsRes.rows) {
      const regId = Number(row.id);
      const regName = row.name || `Register_${regId}`;
      const folderId = row.folder_id ? Number(row.folder_id) : null;
      const folderName = folderId ? (folderMap.get(folderId) || 'Unorganized') : 'Unorganized';
      const columns = normalizeColumns(row.columns).sort((a, b) => (a.position || 0) - (b.position || 0));
      const visibleCols = columns.filter(c => c.type !== 'image' && c.type !== 'signature');
      const regEntries = entriesByRegId.get(regId) || [];

      const headerRow = ['S.No.', ...visibleCols.map(c => escCsv(c.name))].join(',');
      const dataRows = regEntries.map((entry, idx) => {
        const cells = parseJsonField(entry.cells, {});
        const rowData = [idx + 1];
        visibleCols.forEach(c => {
          const val = cells[String(c.id)] ?? '';
          rowData.push(c.type === 'checkbox' ? (String(val) === 'true' ? 'YES' : '') : escCsv(val));
        });
        return rowData.join(',');
      });

      const csvContent = [headerRow, ...dataRows].join('\n');
      const safeRegName = regName.replace(/[\\/:*?"<>|]/g, '_');
      zip.file(`${folderName}/${safeRegName}.csv`, csvContent);
    }

    const zipBuffer = await zip.generateAsync({
      type: 'nodebuffer',
      compression: 'DEFLATE',
      compressionOptions: { level: 2 }
    });

    const smtpHost = process.env.SMTP_HOST || 'smtp.gmail.com';
    const smtpPort = parseInt(process.env.SMTP_PORT || '465', 10);
    const smtpUser = process.env.SMTP_USER;
    const smtpPass = process.env.SMTP_PASS;
    if (!smtpUser || !smtpPass) {
      return res.status(400).json({ error: 'SMTP_USER or SMTP_PASS is missing in environment configuration.' });
    }

    const sentTime = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata', dateStyle: 'full', timeStyle: 'short' });
    const now = new Date();
    const ts = `${now.getDate().toString().padStart(2, '0')}-${(now.getMonth() + 1).toString().padStart(2, '0')}-${now.getFullYear()}_${now.getHours().toString().padStart(2, '0')}-${now.getMinutes().toString().padStart(2, '0')}`;
    const filename = `AG_Trust_Backup_[${ts}].zip`;
    const registerCount = regsRes.rows.length;
    const zipSizeKB = Math.round(zipBuffer.length / 1024);

    const cleanPass = smtpPass.replace(/\s+/g, '');
    const isGmail = smtpHost.includes('gmail.com') || !process.env.SMTP_HOST;
    const transporter = nodemailer.createTransport({
      host: isGmail ? 'smtp.gmail.com' : smtpHost,
      port: isGmail ? 465 : smtpPort,
      secure: isGmail ? true : (smtpPort === 465),
      auth: { user: smtpUser, pass: cleanPass },
      tls: { rejectUnauthorized: false }
    });

    await transporter.sendMail({
      from: process.env.SMTP_FROM || `"AG Trust Backup" <no-reply@sjvps.com>`,
      to: targetEmail,
      subject: `AG Trust Backup - ${registerCount} registers, ${totalEntries} entries`,
      text: `AG Trust Workspace Backup\n\nSent: ${sentTime}\nRegisters: ${registerCount}\nEntries: ${totalEntries}\nFile Size: ${zipSizeKB} KB`,
      attachments: [{ filename, content: zipBuffer }]
    });

    res.json({ message: `Backup email sent successfully to ${targetEmail}`, registerCount, totalEntries });
  } catch (err) {
    res.status(500).json({ error: 'Failed to send mail backup: ' + err.message });
  }
});

app.post('/api/backups/:id/restore', async (req, res) => {
  try {
    const backupRes = await query('SELECT * FROM backups WHERE id = $1', [req.params.id]);
    if (backupRes.rowCount === 0) return res.status(404).json({ error: 'Backup not found' });

    const snapshot = parseJsonField(backupRes.rows[0].snapshot, {});
    const { meta, folders = [], registers = [] } = snapshot;
    const businessId = parseBigInt(meta?.businessId);
    if (!businessId) return res.status(400).json({ error: 'Invalid backup snapshot' });

    await withTransaction(async (client) => {
      await client.query('DELETE FROM entries WHERE register_id IN (SELECT id FROM registers WHERE business_id = $1)', [businessId]);
      await client.query('DELETE FROM registers WHERE business_id = $1', [businessId]);
      await client.query('DELETE FROM folders WHERE business_id = $1', [businessId]);

      for (const folder of folders) {
        await client.query(
          'INSERT INTO folders (id, business_id, name, created_at) VALUES ($1, $2, $3, $4)',
          [parseBigInt(folder.id), parseBigInt(folder.businessId) || businessId, folder.name, folder.createdAt || new Date().toISOString()]
        );
      }

      for (const reg of registers) {
        const rId = parseBigInt(reg.id);
        await client.query(
          `INSERT INTO registers (
             id, business_id, folder_id, name, icon, icon_color, category, template,
             created_at, updated_at, entry_count, deleted_at, deleted_by, deleted_by_email, deleted_by_id,
             columns, pages, share_link, shared_with, deleted_items, migration_completed, entries_per_chunk
           ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)`,
          [
            rId,
            businessId,
            reg.folderId ? parseBigInt(reg.folderId) : null,
            reg.name,
            reg.icon || '',
            reg.iconColor || '',
            reg.category || '',
            reg.template || '',
            reg.createdAt || new Date().toISOString(),
            reg.updatedAt || new Date().toISOString(),
            reg.entryCount || 0,
            reg.deletedAt || null,
            reg.deletedBy || null,
            reg.deletedByEmail || null,
            reg.deletedById ? String(reg.deletedById) : null,
            JSON.stringify(reg.columns || []),
            JSON.stringify(reg.pages || []),
            reg.shareLink || null,
            JSON.stringify(reg.sharedWith || []),
            JSON.stringify(reg.deletedItems || []),
            reg.migrationCompleted ?? true,
            reg.entriesPerChunk || 50
          ]
        );

        const entries = reg.entries || [];
        const batchSize = 200;
        for (let i = 0; i < entries.length; i += batchSize) {
          const batch = entries.slice(i, i + batchSize);
          const valuePhrases = [];
          const queryParams = [];
          batch.forEach((entry, idx) => {
            const offset = idx * 7;
            valuePhrases.push(`($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7})`);
            queryParams.push(
              parseBigInt(entry.id) || generateServerEntryId(),
              rId,
              Number(entry.rowNumber || 1),
              JSON.stringify(entry.cells || {}),
              entry.cellStyles ? JSON.stringify(entry.cellStyles) : null,
              Number(entry.pageIndex || 0),
              entry.createdAt || new Date().toISOString()
            );
          });
          await client.query(
            `INSERT INTO entries (id, register_id, row_number, cells, cell_styles, page_index, created_at)
             VALUES ${valuePhrases.join(', ')}`,
            queryParams
          );
        }
      }

      const logId = Date.now().toString(36) + Math.random().toString(36).slice(2, 9);
      await client.query(
        `INSERT INTO activity_logs (id, user_id, user_name, action, details, timestamp)
         VALUES ($1, $2, $3, 'Backup Restored', $4, NOW())`,
        [logId, req.user?.id || 'system', req.user?.name || 'System', `Restored backup: ${meta.label} (${registers.length} registers)`]
      );
    });

    res.json({ message: 'Backup restored successfully' });
  } catch (err) {
    res.status(500).json({ error: 'Restoration failed: ' + err.message });
  }
});

app.delete('/api/backups/:id', async (req, res) => {
  try {
    await query('DELETE FROM backups WHERE id = $1', [req.params.id]);
    res.json({ message: 'Backup deleted successfully' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.all('/api/cron/daily-backup', async (req, res) => {
  try {
    const { default: cronHandler } = await import('./api/cron/daily-backup.js');
    return await cronHandler(req, res);
  } catch (err) {
    res.status(500).json({ error: 'Cron execution failed: ' + err.message });
  }
});

// ----------------------------------------------------
// BOOTSTRAP DATABASE SCHEMA & DEFAULT USERS
// ----------------------------------------------------
async function bootstrapDatabase() {
  try {
    // Fast-path for serverless cold starts when schema and default users are already provisioned
    const fastCheck = await query(`
      SELECT
        to_regclass('public.backups') IS NOT NULL AS has_backups,
        EXISTS (SELECT 1 FROM pg_class WHERE relname = 'idx_entries_register_row') AS has_idx,
        EXISTS (SELECT 1 FROM users LIMIT 1) AS has_users
    `).catch(() => ({ rows: [] }));
    if (fastCheck.rows[0]?.has_backups && fastCheck.rows[0]?.has_idx && fastCheck.rows[0]?.has_users) {
      return;
    }

    await query(`
      CREATE TABLE IF NOT EXISTS users (
        id VARCHAR(100) PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        role VARCHAR(50) DEFAULT 'user',
        status VARCHAR(50) DEFAULT 'active',
        phone VARCHAR(50) DEFAULT '',
        avatar TEXT DEFAULT '',
        permissions JSONB DEFAULT '{}',
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        last_login TIMESTAMP WITH TIME ZONE
      );
    `);

    await query(`
      CREATE TABLE IF NOT EXISTS businesses (
        id BIGINT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        owner_id BIGINT DEFAULT 1,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
      );
    `);

    await query(`
      CREATE TABLE IF NOT EXISTS folders (
        id BIGINT PRIMARY KEY,
        business_id BIGINT REFERENCES businesses(id) ON DELETE CASCADE,
        name VARCHAR(255) NOT NULL,
        color VARCHAR(50) DEFAULT '#1a73e8',
        deleted_at TIMESTAMP WITH TIME ZONE,
        updated_at TIMESTAMP WITH TIME ZONE,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
      );
    `);

    await query(`
      CREATE TABLE IF NOT EXISTS registers (
        id BIGINT PRIMARY KEY,
        business_id BIGINT REFERENCES businesses(id) ON DELETE CASCADE,
        folder_id BIGINT,
        name VARCHAR(255) NOT NULL,
        icon VARCHAR(100) DEFAULT 'BookOpen',
        icon_color VARCHAR(100) DEFAULT '#1a73e8',
        category VARCHAR(100) DEFAULT 'General',
        template VARCHAR(100) DEFAULT '',
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        entry_count INT DEFAULT 0,
        deleted_at TIMESTAMP WITH TIME ZONE,
        deleted_by VARCHAR(255),
        deleted_by_email VARCHAR(255),
        deleted_by_id VARCHAR(50),
        columns JSONB NOT NULL DEFAULT '[]'::jsonb,
        pages JSONB NOT NULL DEFAULT '[{"id":1,"name":"Page 1","index":0}]'::jsonb,
        share_link VARCHAR(255),
        shared_with JSONB DEFAULT '[]'::jsonb,
        deleted_items JSONB DEFAULT '[]'::jsonb,
        migration_completed BOOLEAN DEFAULT TRUE,
        entries_per_chunk INT DEFAULT 50
      );
    `);

    await query(`
      CREATE TABLE IF NOT EXISTS entries (
        id BIGINT PRIMARY KEY,
        register_id BIGINT REFERENCES registers(id) ON DELETE CASCADE,
        row_number INT NOT NULL,
        cells JSONB NOT NULL DEFAULT '{}'::jsonb,
        cell_styles JSONB,
        page_index INT DEFAULT 0,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
      );
    `);

    await query(`
      CREATE TABLE IF NOT EXISTS activity_logs (
        id VARCHAR(50) PRIMARY KEY,
        user_id VARCHAR(100),
        user_name VARCHAR(255),
        action VARCHAR(255) NOT NULL,
        details TEXT,
        timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        register_id VARCHAR(50),
        register_name VARCHAR(255),
        entry_id VARCHAR(50)
      );
    `);

    await query(`
      CREATE TABLE IF NOT EXISTS download_requests (
        id VARCHAR(50) PRIMARY KEY,
        user_id VARCHAR(100),
        user_name VARCHAR(255),
        type VARCHAR(50) NOT NULL,
        register_id VARCHAR(50),
        register_name VARCHAR(255),
        description TEXT,
        scope JSONB,
        status VARCHAR(50) NOT NULL DEFAULT 'pending',
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        admin_response TEXT,
        responded_at TIMESTAMP WITH TIME ZONE
      );
    `);

    await query(`
      CREATE TABLE IF NOT EXISTS notifications (
        id VARCHAR(50) PRIMARY KEY,
        user_id VARCHAR(100),
        title VARCHAR(255) NOT NULL,
        message TEXT NOT NULL,
        type VARCHAR(50) NOT NULL,
        meta JSONB,
        is_read BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
      );
    `);

    await query(`
      CREATE TABLE IF NOT EXISTS saved_formulas (
        id TEXT PRIMARY KEY,
        business_id BIGINT NOT NULL DEFAULT 1,
        name TEXT NOT NULL,
        formula TEXT NOT NULL,
        created_by TEXT,
        created_at TIMESTAMP DEFAULT NOW()
      );
    `);

    await query(`
      CREATE TABLE IF NOT EXISTS saved_dropdowns (
        id TEXT PRIMARY KEY,
        business_id BIGINT NOT NULL DEFAULT 1,
        name TEXT NOT NULL,
        options TEXT NOT NULL,
        created_by TEXT,
        created_at TIMESTAMP DEFAULT NOW()
      );
    `);

    await query(`
      CREATE TABLE IF NOT EXISTS saved_templates (
        id TEXT PRIMARY KEY,
        business_id BIGINT NOT NULL DEFAULT 1,
        name TEXT NOT NULL,
        columns TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
      );
    `);

    await query(`
      CREATE TABLE IF NOT EXISTS dashboard_configurations (
        business_id BIGINT PRIMARY KEY,
        configured_sum_metrics TEXT NOT NULL,
        shortcuts_order TEXT NOT NULL
      );
    `);

    await query(`
      CREATE TABLE IF NOT EXISTS saved_register_shortcuts (
        id TEXT PRIMARY KEY,
        business_id BIGINT NOT NULL,
        name TEXT NOT NULL,
        register_id BIGINT NOT NULL,
        register_name TEXT NOT NULL,
        search_query TEXT,
        filters TEXT NOT NULL,
        summary_column_id BIGINT,
        created_at TIMESTAMP DEFAULT NOW()
      );
    `);

    await query(`
      CREATE TABLE IF NOT EXISTS backups (
        id VARCHAR(100) PRIMARY KEY,
        business_id BIGINT NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        label TEXT NOT NULL,
        register_count INT DEFAULT 0,
        folder_count INT DEFAULT 0,
        total_entries INT DEFAULT 0,
        size_kb INT DEFAULT 0,
        snapshot JSONB NOT NULL
      );
    `);

    // Safely migrate missing columns on pre-existing tables
    const migrations = [
      'ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar TEXT DEFAULT \'\'',
      'ALTER TABLE folders ADD COLUMN IF NOT EXISTS business_id BIGINT DEFAULT 1',
      'ALTER TABLE folders ADD COLUMN IF NOT EXISTS color VARCHAR(50) DEFAULT \'#1a73e8\'',
      'ALTER TABLE folders ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP WITH TIME ZONE',
      'ALTER TABLE folders ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE',
      'ALTER TABLE registers ADD COLUMN IF NOT EXISTS deleted_items JSONB DEFAULT \'[]\'::jsonb',
      'ALTER TABLE registers ADD COLUMN IF NOT EXISTS shared_with JSONB DEFAULT \'[]\'::jsonb',
      'ALTER TABLE registers ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP WITH TIME ZONE',
      'ALTER TABLE registers ADD COLUMN IF NOT EXISTS deleted_by VARCHAR(255)',
      'ALTER TABLE registers ADD COLUMN IF NOT EXISTS deleted_by_email VARCHAR(255)',
      'ALTER TABLE registers ADD COLUMN IF NOT EXISTS deleted_by_id VARCHAR(50)',
      'ALTER TABLE entries ADD COLUMN IF NOT EXISTS cell_styles JSONB',
      'ALTER TABLE entries ADD COLUMN IF NOT EXISTS page_index INT DEFAULT 0',
      'CREATE INDEX IF NOT EXISTS idx_entries_register_row ON entries (register_id, row_number ASC)'
    ];
    for (const sql of migrations) {
      await query(sql).catch(() => {});
    }

    // Ensure default business exists
    await query("INSERT INTO businesses (id, name, owner_id) VALUES (1, 'AG Trust', 1) ON CONFLICT (id) DO NOTHING");

    // Ensure User 1: Super Admin
    const adminEmail = 'admin@sjvps.com';
    const adminPass = hashPassword('admin123');
    const adminPerms = JSON.stringify({ canView: true, canEdit: true, canDownload: true, isAdmin: true, fullSheetAccess: true, canCreateSheets: true, canSelectBackDates: true });

    await query(`
      INSERT INTO users (id, name, email, password_hash, role, status, phone, avatar, permissions)
      VALUES ('usr_superadmin_1', 'Super Admin', $1, $2, 'superadmin', 'active', '9876543210', '', $3)
      ON CONFLICT (email) DO NOTHING;
    `, [adminEmail, adminPass, adminPerms]);

    // Ensure User 2: Collaborator
    const user2Email = 'collaborator@sjvps.com';
    const user2Pass = hashPassword('user123');
    const user2Perms = JSON.stringify({ canView: true, canEdit: true, canDownload: true, isAdmin: false, fullSheetAccess: true });

    await query(`
      INSERT INTO users (id, name, email, password_hash, role, status, phone, avatar, permissions)
      VALUES ('usr_collab_2', 'Collaborator 1', $1, $2, 'user', 'active', '9876543211', '', $3)
      ON CONFLICT (email) DO NOTHING;
    `, [user2Email, user2Pass, user2Perms]);

    // Ensure Default Register 880001 (with numeric column IDs matching frontend expectations)
    const defaultCols = JSON.stringify([
      { id: 101, key: '101', name: 'S.NO', type: 'auto_increment', position: 0, frozen: true },
      { id: 102, key: '102', name: 'STUDENT NAME', type: 'text', position: 1 },
      { id: 103, key: '103', name: 'APPLICATION NO', type: 'text', position: 2 },
      { id: 104, key: '104', name: 'COURSE', type: 'dropdown', dropdownOptions: ['AHS', 'BE CSE', 'B PHARM', 'MBA', 'ARTS'], position: 3 },
      { id: 105, key: '105', name: 'FEE AMOUNT', type: 'currency', position: 4 },
      { id: 106, key: '106', name: 'STATUS', type: 'status', dropdownOptions: ['Active', 'Pending', 'Completed'], position: 5 }
    ]);

    await query(`
      INSERT INTO registers (id, business_id, name, entry_count, columns, pages)
      VALUES (880001, 1, 'Real-time Student Register', 0, $1, '[{"id":1,"name":"Page 1","index":0}]')
      ON CONFLICT (id) DO NOTHING
    `, [defaultCols]);

    console.log('✅ Database schema, default users & Test Register 880001 initialized');
  } catch (err) {
    console.error('Error bootstrapping database:', err.message);
  }
}

export const bootstrapPromise = bootstrapDatabase();
export default app;

if (!process.env.VERCEL) {
  httpServer.listen(PORT, async () => {
    console.log(`🚀 AG Register Socket.IO Real-Time Backend Server running on port ${PORT}`);
    console.log(`📡 Connected to PostgreSQL Database`);
    await bootstrapPromise;
  });
}
