import jwt from 'jsonwebtoken';
import { query } from '../db/db.js';

const JWT_SECRET = process.env.JWT_SECRET || 'sjvps_super_secret_jwt_key_2024_ag_trust';
const USER_CACHE_TTL_MS = 15 * 1000;
const userAuthCache = new Map();

export function invalidateUserAuthCache(userId) {
  if (userId) {
    userAuthCache.delete(String(userId));
  } else {
    userAuthCache.clear();
  }
}

function parsePermissions(raw) {
  if (!raw) return {};
  if (typeof raw === 'string') {
    try {
      return JSON.parse(raw || '{}');
    } catch {
      return {};
    }
  }
  return raw;
}

export function hasAdminRights(user) {
  if (!user) return false;
  const role = String(user.role || '').toLowerCase();
  const perms = parsePermissions(user.permissions);
  return role === 'admin' || role === 'superadmin' || perms.isAdmin === true;
}

export function generateToken(user) {
  return jwt.sign(
    {
      id: user.id,
      name: user.name || user.id,
      email: user.email || '',
      role: user.role || 'user',
      permissions: parsePermissions(user.permissions)
    },
    JWT_SECRET,
    { expiresIn: '7d' }
  );
}

function tryDecodeLegacyToken(token) {
  try {
    const raw = Buffer.from(token, 'base64').toString('utf8');
    const parsed = JSON.parse(raw);
    if (parsed && parsed.id) return parsed;
  } catch {
    // not base64 JSON
  }
  return null;
}

async function resolveDbUserById(userId) {
  if (!userId) return null;
  const key = String(userId);
  const cached = userAuthCache.get(key);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.user;
  }

  const userRes = await query(
    'SELECT id, name, email, role, status, phone, avatar, permissions, created_at, last_login FROM users WHERE id = $1',
    [key]
  );
  if (userRes.rowCount === 0) return null;
  const u = userRes.rows[0];
  const permissions = parsePermissions(u.permissions);
  const formatted = {
    id: u.id,
    name: u.name,
    email: u.email,
    role: u.role,
    status: u.status || 'active',
    phone: u.phone || '',
    avatar: u.avatar || '',
    createdAt: u.created_at,
    lastLogin: u.last_login,
    permissions,
    isAdmin: hasAdminRights({ role: u.role, permissions })
  };
  userAuthCache.set(key, { user: formatted, expiresAt: Date.now() + USER_CACHE_TTL_MS });
  return formatted;
}

export async function authenticateUser(req, res, next) {
  try {
    const authHeader = req.headers['authorization'] || req.headers['x-user-id'];
    let token = null;

    if (authHeader) {
      if (authHeader.startsWith('Bearer ')) {
        token = authHeader.substring(7).trim();
      } else {
        token = authHeader.trim();
      }
    } else if (req.query && req.query.token) {
      token = String(req.query.token).trim();
    }

    if (!token) {
      req.user = { id: 'anonymous', name: 'Guest User', role: 'user', status: 'anonymous', isAdmin: false, permissions: {} };
      return next();
    }

    // 1. Verify signed JWT
    try {
      const decoded = jwt.verify(token, JWT_SECRET);
      const permissions = parsePermissions(decoded.permissions);
      req.user = {
        id: decoded.id,
        name: decoded.name,
        email: decoded.email,
        role: decoded.role,
        status: 'active',
        isAdmin: hasAdminRights({ role: decoded.role, permissions }),
        permissions
      };
      // Refresh status & permissions from DB if reachable so admin permission edits / deactivations take effect immediately
      try {
        const fresh = await resolveDbUserById(decoded.id);
        if (fresh) {
          req.user = fresh;
        }
      } catch {
        // DB offline or test fallback — keep signed JWT claims
      }
      return next();
    } catch {
      // Fall through to legacy base64 token or raw DB user id lookup
    }

    // 2. Check Vercel-style base64 token or raw DB user id
    const legacy = tryDecodeLegacyToken(token);
    const lookupId = legacy?.id || token;
    try {
      const dbUser = await resolveDbUserById(lookupId);
      if (dbUser) {
        req.user = dbUser;
        return next();
      }
    } catch {
      // Ignore DB lookup errors
    }

    req.user = {
      id: 'anonymous',
      name: 'Guest User',
      role: 'user',
      status: 'anonymous',
      isAdmin: false,
      permissions: {}
    };
    next();
  } catch (err) {
    console.error('[Auth Middleware Error]', err);
    req.user = { id: 'anonymous', name: 'Guest User', role: 'user', status: 'anonymous', isAdmin: false, permissions: {} };
    next();
  }
}

export function requireAdmin(req, res, next) {
  if (!req.user || req.user.status === 'inactive' || req.user.status === 'blocked' || !hasAdminRights(req.user)) {
    return res.status(403).json({ error: 'Admin authority required for this operation' });
  }
  next();
}
