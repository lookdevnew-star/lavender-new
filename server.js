const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

let cosmosClient = null;
let BlobServiceClient = null;
let SocketIOServer = null;
let nodemailer = null;

try {
  ({ CosmosClient } = require("@azure/cosmos"));
} catch (error) {
  console.log("Azure Cosmos DB package not installed yet.");
}

try {
  ({ BlobServiceClient } = require("@azure/storage-blob"));
} catch (error) {
  console.log("Azure Blob Storage package not installed yet; photo upload will need npm install.");
}

try {
  ({ Server: SocketIOServer } = require("socket.io"));
} catch (error) {
  console.log("Socket.IO package not installed yet; realtime WebSocket updates will start after npm install runs.");
}

try {
  nodemailer = require("nodemailer");
} catch (error) {
  console.log("Nodemailer package not installed yet; forgot-password email will start after npm install runs.");
}

const PORT = process.env.PORT || 3000;
const ROOT_DIR = __dirname;
const PUBLIC_DIR = path.join(__dirname, "public");

// Persistent JSON database for /api/db
const DATA_DIR = process.env.HOME ? path.join(process.env.HOME, "site", "data") : path.join(__dirname, ".data");
const DB_FILE = path.join(DATA_DIR, "db.json");

function ensureDataDir() {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  } catch (e) {
    console.error("Could not create data directory:", e.message);
  }
}

function loadDb() {
  ensureDataDir();
  try {
    if (!fs.existsSync(DB_FILE)) return {};
    const text = fs.readFileSync(DB_FILE, "utf8");
    return text ? JSON.parse(text) : {};
  } catch (e) {
    console.error("Could not read db.json:", e.message);
    return {};
  }
}

function saveDb() {
  ensureDataDir();
  try {
    const tmp = DB_FILE + ".tmp";
    fs.writeFileSync(tmp, JSON.stringify(db, null, 2));
    fs.renameSync(tmp, DB_FILE);
  } catch (e) {
    console.error("Could not save db.json:", e.message);
  }
}

let db = loadDb();

const COSMOS_ENDPOINT = process.env.COSMOS_ENDPOINT || "";
const COSMOS_KEY = process.env.COSMOS_KEY || "";
const COSMOS_DATABASE = process.env.COSMOS_DATABASE || "LavenderChat";
const COSMOS_CONTAINER_USERS = process.env.COSMOS_CONTAINER_USERS || "Users";
const COSMOS_CONTAINER_CHATS = process.env.COSMOS_CONTAINER_CHATS || "Chats";
const AZURE_STORAGE_CONNECTION_STRING = process.env.AZURE_STORAGE_CONNECTION_STRING || "";
const AZURE_BLOB_CONTAINER = process.env.AZURE_BLOB_CONTAINER || "chat-photos";
const MAX_UPLOAD_BYTES = 5 * 1024 * 1024;

// Forgot password / email settings
const SMTP_HOST = process.env.SMTP_HOST || "";
const SMTP_PORT = Number(process.env.SMTP_PORT || 587);
const SMTP_USER = process.env.SMTP_USER || "";
const SMTP_PASS = process.env.SMTP_PASS || "";
const SMTP_SECURE = String(process.env.SMTP_SECURE || "").toLowerCase() === "true" || SMTP_PORT === 465;
const MAIL_FROM = process.env.MAIL_FROM || SMTP_USER || "LavanderChat <no-reply@lavanderchat.local>";
const PUBLIC_APP_URL = String(process.env.PUBLIC_APP_URL || process.env.APP_BASE_URL || "").replace(/\/+$/, "");
const RESET_TOKEN_TTL_MS = 60 * 60 * 1000;

// Google / Gmail login settings
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || "";
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET || "";
const GOOGLE_CALLBACK_URL = String(process.env.GOOGLE_CALLBACK_URL || "").replace(/\/+$/, "");
const GOOGLE_OAUTH_STATE_COOKIE = "localchat_google_state";
const GOOGLE_OAUTH_STATE_TTL_SECONDS = 10 * 60;

let CosmosClient = null;
let cosmosDatabase = null;
let usersContainer = null;
let chatsContainer = null;
let blobContainerClient = null;
let databaseMode = "local-json";
let storageMode = "not-configured";

// Old in-memory chat routes kept for compatibility
const users = new Map();
const clients = new Map();
const conversations = new Map();

function send(res, status, data, type = "application/json", extraHeaders = {}) {
  const headers = {
    "Content-Type": type,
    "Cache-Control": "no-store",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,PUT,POST,PATCH,DELETE,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
    ...extraHeaders
  };

  res.writeHead(status, headers);

  if (status === 204) {
    res.end();
  } else if (type === "application/json") {
    res.end(JSON.stringify(data));
  } else {
    res.end(data);
  }
}

function redirect(res, location, extraHeaders = {}) {
  res.writeHead(302, {
    "Location": location,
    "Cache-Control": "no-store",
    ...extraHeaders
  });
  res.end();
}

function readJson(req) {
  return new Promise((resolve) => {
    let body = "";
    req.on("data", chunk => {
      body += chunk;
      if (body.length > 8_000_000) req.destroy();
    });
    req.on("end", () => {
      if (!body) return resolve({});
      try {
        resolve(JSON.parse(body));
      } catch {
        resolve({});
      }
    });
  });
}

function cleanName(name) {
  return String(name || "")
    .trim()
    .replace(/\s+/g, " ")
    .slice(0, 24) || "Guest";
}

function cleanText(text) {
  return String(text || "").trim().slice(0, 1000);
}

function publicUsers() {
  return Array.from(users.values()).map(u => ({
    id: u.id,
    name: u.name,
    joinedAt: u.joinedAt
  }));
}

function key(a, b) {
  return [a, b].sort().join("|");
}

function sse(res, event, data) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(data)}\n\n`);
}

function broadcastUsers() {
  const list = publicUsers();
  for (const res of clients.values()) {
    sse(res, "users", list);
  }
}

function normalizeDbPath(rawPath) {
  let p = String(rawPath || "").trim();
  p = p.replace(/^\/+/, "").replace(/\/+$/, "");
  if (!p) return [];

  const parts = p.split("/").filter(Boolean).map(part => decodeURIComponent(part));

  for (const part of parts) {
    if (part === "__proto__" || part === "constructor" || part === "prototype") {
      throw new Error("Bad path");
    }
  }

  return parts;
}

function getAt(obj, parts) {
  let cur = obj;
  for (const part of parts) {
    if (cur == null || typeof cur !== "object" || !(part in cur)) return null;
    cur = cur[part];
  }
  return cur === undefined ? null : cur;
}

function setAt(obj, parts, value) {
  if (parts.length === 0) {
    db = value && typeof value === "object" && !Array.isArray(value) ? value : { value };
    return;
  }

  let cur = obj;
  for (let i = 0; i < parts.length - 1; i++) {
    const part = parts[i];
    if (cur[part] == null || typeof cur[part] !== "object" || Array.isArray(cur[part])) {
      cur[part] = {};
    }
    cur = cur[part];
  }
  cur[parts[parts.length - 1]] = value;
}

function patchAt(obj, parts, value) {
  const oldValue = getAt(obj, parts);
  if (
    oldValue &&
    typeof oldValue === "object" &&
    !Array.isArray(oldValue) &&
    value &&
    typeof value === "object" &&
    !Array.isArray(value)
  ) {
    setAt(obj, parts, { ...oldValue, ...value });
  } else {
    setAt(obj, parts, value);
  }
}

function deleteAt(obj, parts) {
  if (parts.length === 0) {
    db = {};
    return;
  }

  let cur = obj;
  for (let i = 0; i < parts.length - 1; i++) {
    cur = cur?.[parts[i]];
    if (cur == null || typeof cur !== "object") return;
  }

  delete cur[parts[parts.length - 1]];
}


function cloneJson(value) {
  if (value === undefined) return null;
  return JSON.parse(JSON.stringify(value));
}

function escapeRegExp(text) {
  return String(text).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function assignAtPath(root, parts, value) {
  if (!parts.length) return value;

  let cur = root;
  for (let i = 0; i < parts.length - 1; i++) {
    const part = parts[i];
    if (!cur[part] || typeof cur[part] !== "object" || Array.isArray(cur[part])) {
      cur[part] = {};
    }
    cur = cur[part];
  }

  cur[parts[parts.length - 1]] = value;
  return root;
}

function mongoPrefixQuery(fullPath) {
  if (!fullPath) {
    return { _id: { $ne: "" } };
  }

  return { _id: { $regex: "^" + escapeRegExp(fullPath + "/") } };
}

async function getMongoAt(parts) {
  const fullPath = parts.join("/");
  const exactDoc = await mongoKv.findOne({ _id: fullPath });
  const childDocs = await mongoKv.find(mongoPrefixQuery(fullPath)).toArray();

  if (!exactDoc && childDocs.length === 0) return null;

  let value = exactDoc ? cloneJson(exactDoc.value) : null;

  if (childDocs.length) {
    if (!value || typeof value !== "object" || Array.isArray(value)) value = {};

    for (const doc of childDocs) {
      const relativePath = fullPath ? doc._id.slice(fullPath.length + 1) : doc._id;
      const childParts = relativePath.split("/").filter(Boolean);
      assignAtPath(value, childParts, cloneJson(doc.value));
    }
  }

  return value;
}

async function setMongoAt(parts, value) {
  const fullPath = parts.join("/");

  if (!fullPath) {
    await mongoKv.deleteMany({});
  } else {
    await mongoKv.deleteMany(mongoPrefixQuery(fullPath));
  }

  await mongoKv.updateOne(
    { _id: fullPath },
    { $set: { value: value === undefined ? null : value, updatedAt: new Date() } },
    { upsert: true }
  );
}

async function patchMongoAt(parts, value) {
  const oldValue = await getMongoAt(parts);
  let nextValue;

  if (
    oldValue &&
    typeof oldValue === "object" &&
    !Array.isArray(oldValue) &&
    value &&
    typeof value === "object" &&
    !Array.isArray(value)
  ) {
    nextValue = { ...oldValue, ...value };
  } else {
    nextValue = value;
  }

  await setMongoAt(parts, nextValue === undefined ? {} : nextValue);
}

async function deleteMongoAt(parts) {
  const fullPath = parts.join("/");

  if (!fullPath) {
    await mongoKv.deleteMany({});
    return;
  }

  await mongoKv.deleteMany({
    $or: [
      { _id: fullPath },
      mongoPrefixQuery(fullPath)
    ]
  });
}


function normalizeEmail(email) {
  return String(email || "").trim().toLowerCase().slice(0, 160);
}

function cleanAccountName(name) {
  return String(name || "")
    .trim()
    .replace(/\s+/g, " ")
    .replace(/[<>]/g, "")
    .slice(0, 20);
}

function cleanAccountGender(gender) {
  return String(gender || "").toLowerCase() === "female" ? "female" : "male";
}

function accountNameKey(name) {
  return cleanAccountName(name).toLowerCase().replace(/[.#$\[\]\/]/g, "_") || "unknown";
}

function cleanPhotoUrl(url) {
  const value = String(url || "").trim();
  if (!value) return "";
  if (value.length > 1200) return "";
  if (/^https?:\/\//i.test(value)) return value;
  return "";
}

function publicAccountUser(account) {
  if (!account) return null;
  return {
    id: account._id || account.id,
    uid: account.chatUid || (account._id ? "account_" + account._id : ""),
    name: account.name,
    email: account.email,
    gender: cleanAccountGender(account.gender),
    photoUrl: cleanPhotoUrl(account.photoUrl),
    provider: account.provider || "email",
    createdAt: account.createdAt || null
  };
}

function hashPassword(password) {
  const salt = crypto.randomBytes(16).toString("hex");
  const hash = crypto.scryptSync(String(password), salt, 64).toString("hex");
  return `scrypt$${salt}$${hash}`;
}

function verifyPassword(password, storedHash) {
  try {
    const parts = String(storedHash || "").split("$");
    if (parts.length !== 3 || parts[0] !== "scrypt") return false;
    const expected = Buffer.from(parts[2], "hex");
    const actual = crypto.scryptSync(String(password), parts[1], 64);
    return expected.length === actual.length && crypto.timingSafeEqual(expected, actual);
  } catch {
    return false;
  }
}

function hashResetToken(token) {
  return crypto.createHash("sha256").update(String(token || "")).digest("hex");
}

function isEmailSenderConfigured() {
  return Boolean(nodemailer && SMTP_HOST && SMTP_USER && SMTP_PASS);
}

function publicBaseUrl(req) {
  if (PUBLIC_APP_URL) return PUBLIC_APP_URL;

  const forwardedProto = String(req.headers["x-forwarded-proto"] || "").split(",")[0].trim();
  const proto = forwardedProto || (req.socket && req.socket.encrypted ? "https" : "http");
  const host = String(req.headers["x-forwarded-host"] || req.headers.host || "localhost:" + PORT).split(",")[0].trim();
  return `${proto}://${host}`.replace(/\/+$/, "");
}

function makePasswordResetUrl(req, token) {
  return `${publicBaseUrl(req)}/?resetToken=${encodeURIComponent(token)}`;
}

function isGoogleLoginConfigured() {
  return Boolean(GOOGLE_CLIENT_ID && GOOGLE_CLIENT_SECRET);
}

function googleCallbackUrl(req) {
  return GOOGLE_CALLBACK_URL || `${publicBaseUrl(req)}/auth/google/callback`;
}

function cookieSecureSuffix(req) {
  const forwardedProto = String(req.headers["x-forwarded-proto"] || "").toLowerCase();
  const isHttps = forwardedProto === "https" || Boolean(req.socket && req.socket.encrypted);
  return isHttps ? "; Secure" : "";
}

function tempCookie(req, name, value, maxAgeSeconds) {
  return `${name}=${encodeURIComponent(value || "")}; HttpOnly; SameSite=Lax; Path=/; Max-Age=${Number(maxAgeSeconds) || 0}${cookieSecureSuffix(req)}`;
}

function clearTempCookie(req, name) {
  return `${name}=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0${cookieSecureSuffix(req)}`;
}

async function sendPasswordResetEmail(account, resetUrl) {
  if (!isEmailSenderConfigured()) {
    const error = new Error("Email sender is not configured. Add SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, MAIL_FROM, and PUBLIC_APP_URL in Azure.");
    error.code = "EMAIL_NOT_CONFIGURED";
    throw error;
  }

  const transporter = nodemailer.createTransport({
    host: SMTP_HOST,
    port: SMTP_PORT,
    secure: SMTP_SECURE,
    auth: {
      user: SMTP_USER,
      pass: SMTP_PASS
    }
  });

  const safeName = account && account.name ? account.name : "there";
  const textBody = [
    `Hi ${safeName},`,
    "",
    "You asked to reset your LavanderChat password.",
    "Open this link and choose a new password:",
    resetUrl,
    "",
    "This link will expire in 1 hour.",
    "If you did not ask for this, you can ignore this email.",
    "",
    "LavanderChat"
  ].join("\n");

  const htmlBody = `
    <div style="font-family:Arial,sans-serif;line-height:1.55;color:#222;max-width:560px;margin:auto;">
      <h2 style="color:#282a35;">Reset your LavanderChat password</h2>
      <p>Hi ${escapeHtml(safeName)},</p>
      <p>You asked to reset your LavanderChat password.</p>
      <p>Click the button below and choose a new password. This link expires in <b>1 hour</b>.</p>
      <p><a href="${escapeHtml(resetUrl)}" style="display:inline-block;background:#282a35;color:#fff;text-decoration:none;padding:13px 18px;border-radius:12px;font-weight:bold;">Reset password</a></p>
      <p style="word-break:break-all;font-size:13px;color:#555;">${escapeHtml(resetUrl)}</p>
      <p style="color:#777;font-size:13px;">If you did not ask for this, you can ignore this email.</p>
    </div>
  `;

  await transporter.sendMail({
    from: MAIL_FROM,
    to: account.email,
    subject: "Reset your LavanderChat password",
    text: textBody,
    html: htmlBody
  });
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function ensureLocalAuthStore() {
  if (!db.__auth || typeof db.__auth !== "object") db.__auth = {};
  if (!db.__auth.accounts || typeof db.__auth.accounts !== "object") db.__auth.accounts = {};
  if (!db.__auth.sessions || typeof db.__auth.sessions !== "object") db.__auth.sessions = {};
}

async function findAccountByEmailOrName(login) {
  const value = String(login || "").trim();
  if (!value) return null;
  const email = normalizeEmail(value);
  const nameKey = accountNameKey(value);

  if (mongoAccounts) {
    return await mongoAccounts.findOne({
      $or: [
        { email },
        { nameKey }
      ]
    });
  }

  ensureLocalAuthStore();
  return Object.values(db.__auth.accounts).find((account) => account.email === email || account.nameKey === nameKey) || null;
}

async function findAccountByEmailOnly(emailInput) {
  const email = normalizeEmail(emailInput);
  if (!email) return null;

  if (mongoAccounts) {
    return await mongoAccounts.findOne({ email });
  }

  ensureLocalAuthStore();
  return Object.values(db.__auth.accounts).find((account) => account.email === email) || null;
}

async function findAccountByGoogleSub(googleSub) {
  const sub = String(googleSub || "").trim();
  if (!sub) return null;

  if (mongoAccounts) {
    return await mongoAccounts.findOne({ googleSub: sub });
  }

  ensureLocalAuthStore();
  return Object.values(db.__auth.accounts).find((account) => account.googleSub === sub) || null;
}

async function makeUniqueAccountName(preferredName, email) {
  const emailLocalPart = String(email || "").split("@")[0] || "";
  const base = cleanAccountName(preferredName) || cleanAccountName(emailLocalPart) || "Google User";

  const firstOwner = await findAccountByNameOnly(base);
  if (!firstOwner) return base;

  for (let i = 2; i <= 999; i++) {
    const suffix = " " + i;
    const candidate = cleanAccountName(base.slice(0, Math.max(1, 20 - suffix.length)) + suffix);
    const owner = await findAccountByNameOnly(candidate);
    if (!owner) return candidate;
  }

  return cleanAccountName("User " + crypto.randomBytes(4).toString("hex"));
}

async function findAccountByNameOnly(name) {
  const nameKey = accountNameKey(name);
  if (!nameKey || nameKey === "unknown") return null;

  if (mongoAccounts) {
    return await mongoAccounts.findOne({ nameKey });
  }

  ensureLocalAuthStore();
  return Object.values(db.__auth.accounts).find((account) => account.nameKey === nameKey) || null;
}

async function findAccountById(accountId) {
  const id = String(accountId || "").trim();
  if (!id) return null;

  if (mongoAccounts) {
    return await mongoAccounts.findOne({ _id: id });
  }

  ensureLocalAuthStore();
  return db.__auth.accounts[id] || null;
}

async function saveAccount(account) {
  if (mongoAccounts) {
    await mongoAccounts.updateOne({ _id: account._id }, { $set: account }, { upsert: true });
    return account;
  }

  ensureLocalAuthStore();
  db.__auth.accounts[account._id] = account;
  saveDb();
  return account;
}

async function createAuthSession(account, req) {
  const token = crypto.randomBytes(32).toString("hex");
  const now = Date.now();
  const session = {
    _id: token,
    accountId: account._id,
    createdAt: now,
    lastSeenAt: now,
    userAgent: String(req.headers["user-agent"] || "").slice(0, 300)
  };

  if (mongoSessions) {
    await mongoSessions.updateOne({ _id: token }, { $set: session }, { upsert: true });
  } else {
    ensureLocalAuthStore();
    db.__auth.sessions[token] = session;
    saveDb();
  }

  return token;
}

function parseCookies(req) {
  const header = String(req.headers.cookie || "");
  const cookies = {};
  header.split(";").forEach((part) => {
    const index = part.indexOf("=");
    if (index === -1) return;
    const key = part.slice(0, index).trim();
    const value = part.slice(index + 1).trim();
    if (key) cookies[key] = decodeURIComponent(value);
  });
  return cookies;
}

function authTokenFromRequest(req) {
  const auth = String(req.headers.authorization || "");
  if (auth.toLowerCase().startsWith("bearer ")) return auth.slice(7).trim();
  return parseCookies(req).localchat_auth || "";
}

function authCookie(req, token) {
  return `localchat_auth=${encodeURIComponent(token)}; HttpOnly; SameSite=Lax; Path=/; Max-Age=2592000${cookieSecureSuffix(req)}`;
}

function clearAuthCookie(req) {
  return `localchat_auth=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0${cookieSecureSuffix(req)}`;
}

async function getAccountFromRequest(req) {
  const token = authTokenFromRequest(req);
  if (!token) return null;

  let session = null;
  if (mongoSessions) {
    session = await mongoSessions.findOne({ _id: token });
  } else {
    ensureLocalAuthStore();
    session = db.__auth.sessions[token] || null;
  }

  if (!session) return null;
  const account = await findAccountById(session.accountId);
  if (!account || account.banned) return null;

  if (mongoSessions) {
    await mongoSessions.updateOne({ _id: token }, { $set: { lastSeenAt: Date.now() } });
  } else {
    ensureLocalAuthStore();
    if (db.__auth.sessions[token]) db.__auth.sessions[token].lastSeenAt = Date.now();
    saveDb();
  }

  return { account, token };
}

async function deleteAuthSession(token) {
  if (!token) return;

  if (mongoSessions) {
    await mongoSessions.deleteOne({ _id: token });
  } else {
    ensureLocalAuthStore();
    delete db.__auth.sessions[token];
    saveDb();
  }
}

async function deleteAuthSessionsForAccount(accountId) {
  const id = String(accountId || "");
  if (!id) return;

  if (mongoSessions) {
    await mongoSessions.deleteMany({ accountId: id });
    return;
  }

  ensureLocalAuthStore();
  for (const [token, session] of Object.entries(db.__auth.sessions)) {
    if (session && session.accountId === id) {
      delete db.__auth.sessions[token];
    }
  }
  saveDb();
}

function ownerChatUid(account) {
  return account && (account.chatUid || (account._id ? "account_" + account._id : ""));
}

async function isRequestForAccountOwner(req, account, targetUid, value) {
  if (!account) return false;

  const result = await getAccountFromRequest(req);
  if (!result || !result.account || result.account._id !== account._id) {
    return false;
  }

  const accountUid = ownerChatUid(account);
  const valueUid = value && typeof value === "object" ? String(value.uid || "") : "";

  // The registered account can only write its own online user row.
  return Boolean(accountUid && (String(targetUid || "") === accountUid || valueUid === accountUid));
}

async function enforceRegisteredNameWrite(req, parts, value) {
  if (!Array.isArray(parts) || !parts.length) return;
  if (parts[0] !== "users" && parts[0] !== "presenceByName") return;
  if (!value || typeof value !== "object" || Array.isArray(value)) return;

  const incomingName = cleanAccountName(value.name);
  if (!incomingName) return;

  const owner = await findAccountByNameOnly(incomingName);
  if (!owner) return;

  const targetUid = parts[0] === "users" ? (parts[1] || "") : (value.uid || "");
  const allowed = await isRequestForAccountOwner(req, owner, targetUid, value);
  if (allowed) return;

  const error = new Error("This name is registered. Please login to use it.");
  error.statusCode = 403;
  throw error;
}

async function handleAuthNameStatus(req, res, url) {
  try {
    const name = cleanAccountName(url.searchParams.get("name") || "");
    if (!name) {
      return send(res, 200, { ok: true, success: true, reserved: false, mine: false });
    }

    const owner = await findAccountByNameOnly(name);
    let mine = false;

    if (owner) {
      const result = await getAccountFromRequest(req);
      mine = Boolean(result && result.account && result.account._id === owner._id);
    }

    return send(res, 200, {
      ok: true,
      success: true,
      reserved: Boolean(owner),
      mine,
      message: owner && !mine ? "This name is registered. Please login to use it." : ""
    });
  } catch (error) {
    console.error("Name status error:", error.message);
    return send(res, 500, { ok: false, success: false, error: "Could not check this name." });
  }
}

async function handleAuthRegister(req, res) {
  try {
    const body = await readJson(req);
    const name = cleanAccountName(body.name);
    const email = normalizeEmail(body.email);
    const password = String(body.password || "");
    const gender = cleanAccountGender(body.gender);
    const photoUrl = cleanPhotoUrl(body.photoUrl);

    if (!name || name.length < 2) {
      return send(res, 400, { ok: false, success: false, error: "Please enter a valid name." });
    }

    if (!/^\S+@\S+\.\S+$/.test(email)) {
      return send(res, 400, { ok: false, success: false, error: "Please enter a valid email." });
    }

    if (password.length < 6) {
      return send(res, 400, { ok: false, success: false, error: "Password must be at least 6 characters." });
    }

    const nameKey = accountNameKey(name);
    const existing = await findAccountByEmailOrName(email) || await findAccountByEmailOrName(name);
    if (existing) {
      if (existing.email === email) {
        return send(res, 409, { ok: false, success: false, error: "This email already has an account. Please login." });
      }
      return send(res, 409, { ok: false, success: false, error: "This name is already used. Choose another name." });
    }

    const id = "acc_" + crypto.randomUUID();
    const now = Date.now();
    const account = {
      _id: id,
      chatUid: "account_" + id,
      name,
      nameKey,
      email,
      gender,
      photoUrl,
      passwordHash: hashPassword(password),
      provider: "email",
      role: "user",
      banned: false,
      createdAt: now,
      updatedAt: now,
      lastLoginAt: now
    };

    await saveAccount(account);
    const token = await createAuthSession(account, req);

    return send(res, 200, {
      ok: true,
      success: true,
      user: publicAccountUser(account)
    }, "application/json", { "Set-Cookie": authCookie(req, token) });
  } catch (error) {
    console.error("Register error:", error.message);
    return send(res, 500, { ok: false, success: false, error: "Could not create account." });
  }
}

async function handleAuthLogin(req, res) {
  try {
    const body = await readJson(req);
    const login = String(body.login || body.email || body.username || "").trim();
    const password = String(body.password || "");

    const account = await findAccountByEmailOrName(login);
    if (!account || !verifyPassword(password, account.passwordHash)) {
      return send(res, 401, { ok: false, success: false, error: "Email/username or password is wrong." });
    }

    if (account.banned) {
      return send(res, 403, { ok: false, success: false, error: "This account is banned." });
    }

    account.lastLoginAt = Date.now();
    account.updatedAt = Date.now();
    await saveAccount(account);

    const token = await createAuthSession(account, req);
    return send(res, 200, {
      ok: true,
      success: true,
      user: publicAccountUser(account)
    }, "application/json", { "Set-Cookie": authCookie(req, token) });
  } catch (error) {
    console.error("Login error:", error.message);
    return send(res, 500, { ok: false, success: false, error: "Could not login." });
  }
}

async function handleAuthForgotPassword(req, res) {
  try {
    const body = await readJson(req);
    const email = normalizeEmail(body.email || body.login || "");

    if (!/^\S+@\S+\.\S+$/.test(email)) {
      return send(res, 400, { ok: false, success: false, error: "Please enter your account email." });
    }

    const account = await findAccountByEmailOnly(email);

    // Never tell visitors whether the email exists. This protects registered users.
    const genericResponse = {
      ok: true,
      success: true,
      message: "If this email has an account, a password reset link has been sent."
    };

    if (!account || account.banned) {
      return send(res, 200, genericResponse);
    }

    const rawToken = crypto.randomBytes(32).toString("hex");
    const resetUrl = makePasswordResetUrl(req, rawToken);
    const now = Date.now();

    account.passwordReset = {
      tokenHash: hashResetToken(rawToken),
      expiresAt: now + RESET_TOKEN_TTL_MS,
      createdAt: now
    };
    account.updatedAt = now;
    await saveAccount(account);

    try {
      await sendPasswordResetEmail(account, resetUrl);
    } catch (emailError) {
      console.error("Password reset email error:", emailError.message);
      if (emailError.code === "EMAIL_NOT_CONFIGURED") {
        return send(res, 503, {
          ok: false,
          success: false,
          error: "Password reset email is not configured yet. Add SMTP settings in Azure first."
        });
      }
      return send(res, 500, { ok: false, success: false, error: "Could not send password reset email. Try again later." });
    }

    return send(res, 200, genericResponse);
  } catch (error) {
    console.error("Forgot password error:", error.message);
    return send(res, 500, { ok: false, success: false, error: "Could not start password reset." });
  }
}

async function handleAuthResetPassword(req, res) {
  try {
    const body = await readJson(req);
    const token = String(body.token || "").trim();
    const password = String(body.password || "");

    if (!token || token.length < 20) {
      return send(res, 400, { ok: false, success: false, error: "Password reset link is invalid." });
    }

    if (password.length < 6) {
      return send(res, 400, { ok: false, success: false, error: "Password must be at least 6 characters." });
    }

    const tokenHash = hashResetToken(token);
    const now = Date.now();
    let account = null;

    if (mongoAccounts) {
      account = await mongoAccounts.findOne({
        "passwordReset.tokenHash": tokenHash,
        "passwordReset.expiresAt": { $gt: now }
      });
    } else {
      ensureLocalAuthStore();
      account = Object.values(db.__auth.accounts).find((item) => {
        return item && item.passwordReset && item.passwordReset.tokenHash === tokenHash && Number(item.passwordReset.expiresAt || 0) > now;
      }) || null;
    }

    if (!account || account.banned) {
      return send(res, 400, { ok: false, success: false, error: "Password reset link is invalid or expired." });
    }

    account.passwordHash = hashPassword(password);
    account.passwordReset = null;
    account.updatedAt = now;
    account.passwordChangedAt = now;
    await saveAccount(account);
    await deleteAuthSessionsForAccount(account._id);

    return send(res, 200, {
      ok: true,
      success: true,
      message: "Password changed. You can login with your new password now."
    });
  } catch (error) {
    console.error("Reset password error:", error.message);
    return send(res, 500, { ok: false, success: false, error: "Could not reset password." });
  }
}

async function handleAuthMe(req, res) {
  try {
    const result = await getAccountFromRequest(req);
    if (!result) {
      return send(res, 200, { ok: true, success: true, user: null });
    }

    return send(res, 200, { ok: true, success: true, user: publicAccountUser(result.account) });
  } catch (error) {
    console.error("Auth me error:", error.message);
    return send(res, 200, { ok: true, success: true, user: null });
  }
}

async function handleAuthProfilePhoto(req, res) {
  try {
    const result = await getAccountFromRequest(req);
    if (!result) {
      return send(res, 401, { ok: false, success: false, error: "Please login first." });
    }

    const body = await readJson(req);
    const photoUrl = cleanPhotoUrl(body && body.photoUrl);

    result.account.photoUrl = photoUrl;
    result.account.updatedAt = Date.now();
    await saveAccount(result.account);

    return send(res, 200, {
      ok: true,
      success: true,
      user: publicAccountUser(result.account)
    });
  } catch (error) {
    console.error("Profile photo update error:", error.message);
    return send(res, 500, { ok: false, success: false, error: "Could not save profile photo." });
  }
}

async function handleAuthLogout(req, res) {
  try {
    await deleteAuthSession(authTokenFromRequest(req));
    return send(res, 200, { ok: true, success: true }, "application/json", { "Set-Cookie": clearAuthCookie(req) });
  } catch (error) {
    console.error("Logout error:", error.message);
    return send(res, 200, { ok: true, success: true }, "application/json", { "Set-Cookie": clearAuthCookie(req) });
  }
}

async function findOrCreateGoogleAccount(googleUser) {
  const sub = String(googleUser && googleUser.sub || "").trim();
  const email = normalizeEmail(googleUser && googleUser.email || "");
  const emailVerified = googleUser && (googleUser.email_verified === true || String(googleUser.email_verified).toLowerCase() === "true");

  if (!sub || !email || !/^\S+@\S+\.\S+$/.test(email)) {
    const error = new Error("Google did not return a usable email address.");
    error.publicMessage = "Google login did not return an email address. Try another Google account.";
    throw error;
  }

  if (!emailVerified) {
    const error = new Error("Google email is not verified.");
    error.publicMessage = "Your Google email is not verified yet.";
    throw error;
  }

  const now = Date.now();
  let account = await findAccountByGoogleSub(sub);

  if (!account) {
    account = await findAccountByEmailOnly(email);
  }

  if (account) {
    if (account.banned) {
      const error = new Error("Google login account is banned.");
      error.publicMessage = "This account is banned.";
      throw error;
    }

    account.googleSub = sub;
    account.googleEmail = email;
    account.googleName = String(googleUser.name || "").slice(0, 120);
    account.googlePicture = cleanPhotoUrl(googleUser.picture || "");
    account.provider = account.passwordHash ? "email+google" : "google";
    account.email = account.email || email;
    if (!account.photoUrl && account.googlePicture) account.photoUrl = account.googlePicture;
    account.updatedAt = now;
    account.lastLoginAt = now;
    await saveAccount(account);
    return account;
  }

  const name = await makeUniqueAccountName(googleUser.name || email.split("@")[0], email);
  const id = "acc_" + crypto.randomUUID();
  account = {
    _id: id,
    chatUid: "account_" + id,
    name,
    nameKey: accountNameKey(name),
    email,
    gender: "male",
    photoUrl: cleanPhotoUrl(googleUser.picture || ""),
    passwordHash: "",
    provider: "google",
    googleSub: sub,
    googleEmail: email,
    googleName: String(googleUser.name || "").slice(0, 120),
    googlePicture: cleanPhotoUrl(googleUser.picture || ""),
    role: "user",
    banned: false,
    createdAt: now,
    updatedAt: now,
    lastLoginAt: now
  };

  await saveAccount(account);
  return account;
}

async function handleAuthGoogleStart(req, res) {
  try {
    if (!isGoogleLoginConfigured()) {
      return redirect(res, `${publicBaseUrl(req)}/?loginError=google_not_configured`);
    }

    const state = crypto.randomBytes(24).toString("hex");
    const params = new URLSearchParams({
      client_id: GOOGLE_CLIENT_ID,
      redirect_uri: googleCallbackUrl(req),
      response_type: "code",
      scope: "openid email profile",
      state,
      prompt: "select_account"
    });

    return redirect(res, `https://accounts.google.com/o/oauth2/v2/auth?${params.toString()}`, {
      "Set-Cookie": tempCookie(req, GOOGLE_OAUTH_STATE_COOKIE, state, GOOGLE_OAUTH_STATE_TTL_SECONDS)
    });
  } catch (error) {
    console.error("Google login start error:", error.message);
    return redirect(res, `${publicBaseUrl(req)}/?loginError=google_start_failed`);
  }
}

async function handleAuthGoogleCallback(req, res, url) {
  const clearStateHeader = clearTempCookie(req, GOOGLE_OAUTH_STATE_COOKIE);

  try {
    if (!isGoogleLoginConfigured()) {
      return redirect(res, `${publicBaseUrl(req)}/?loginError=google_not_configured`, { "Set-Cookie": clearStateHeader });
    }

    const googleError = String(url.searchParams.get("error") || "").trim();
    if (googleError) {
      return redirect(res, `${publicBaseUrl(req)}/?loginError=google_cancelled`, { "Set-Cookie": clearStateHeader });
    }

    const code = String(url.searchParams.get("code") || "").trim();
    const state = String(url.searchParams.get("state") || "").trim();
    const cookieState = String(parseCookies(req)[GOOGLE_OAUTH_STATE_COOKIE] || "").trim();

    if (!code || !state || !cookieState || state !== cookieState) {
      return redirect(res, `${publicBaseUrl(req)}/?loginError=google_state`, { "Set-Cookie": clearStateHeader });
    }

    if (typeof fetch !== "function") {
      throw new Error("The Node.js fetch API is not available. Use Node.js 18+ or 22 in Azure.");
    }

    const tokenResponse = await fetch("https://oauth2.googleapis.com/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        code,
        client_id: GOOGLE_CLIENT_ID,
        client_secret: GOOGLE_CLIENT_SECRET,
        redirect_uri: googleCallbackUrl(req),
        grant_type: "authorization_code"
      })
    });

    const tokenData = await tokenResponse.json().catch(() => ({}));
    if (!tokenResponse.ok || !tokenData.access_token) {
      console.error("Google token exchange failed:", tokenResponse.status, tokenData && tokenData.error);
      return redirect(res, `${publicBaseUrl(req)}/?loginError=google_token`, { "Set-Cookie": clearStateHeader });
    }

    const userResponse = await fetch("https://openidconnect.googleapis.com/v1/userinfo", {
      headers: { "Authorization": `Bearer ${tokenData.access_token}` }
    });

    const googleUser = await userResponse.json().catch(() => ({}));
    if (!userResponse.ok) {
      console.error("Google userinfo failed:", userResponse.status);
      return redirect(res, `${publicBaseUrl(req)}/?loginError=google_profile`, { "Set-Cookie": clearStateHeader });
    }

    const account = await findOrCreateGoogleAccount(googleUser);
    const token = await createAuthSession(account, req);

    return redirect(res, `${publicBaseUrl(req)}/?googleLogin=success`, {
      "Set-Cookie": [clearStateHeader, authCookie(req, token)]
    });
  } catch (error) {
    console.error("Google callback error:", error.message);
    return redirect(res, `${publicBaseUrl(req)}/?loginError=google_failed`, { "Set-Cookie": clearStateHeader });
  }
}

function parseDataUrl(dataUrl) {
  const match = String(dataUrl || "").match(/^data:([^;,]+);base64,(.+)$/);
  if (!match) return null;

  return {
    mimeType: match[1].toLowerCase(),
    buffer: Buffer.from(match[2], "base64")
  };
}

function safeFileBaseName(fileName) {
  const clean = path.basename(String(fileName || "photo")).replace(/[^a-zA-Z0-9._-]/g, "_");
  return clean.slice(0, 80) || "photo";
}

function extensionForMime(mimeType) {
  const map = {
    "image/jpeg": "jpg",
    "image/png": "png",
    "image/webp": "webp",
    "image/gif": "gif"
  };
  return map[mimeType] || "jpg";
}

async function handlePhotoUpload(req, res) {
  try {
    if (!blobContainerClient) {
      return send(res, 503, {
        ok: false,
        success: false,
        error: "Azure Blob Storage is not configured. Add AZURE_STORAGE_CONNECTION_STRING and AZURE_BLOB_CONTAINER in Azure."
      });
    }

    const body = await readJson(req);
    const parsed = parseDataUrl(body && body.imageData);

    if (!parsed) {
      return send(res, 400, { ok: false, success: false, error: "Invalid photo data." });
    }

    const allowedTypes = new Set(["image/jpeg", "image/png", "image/webp", "image/gif"]);
    if (!allowedTypes.has(parsed.mimeType)) {
      return send(res, 400, { ok: false, success: false, error: "Only JPG, PNG, WEBP, and GIF photos are allowed." });
    }

    if (!parsed.buffer.length || parsed.buffer.length > MAX_UPLOAD_BYTES) {
      return send(res, 400, { ok: false, success: false, error: "Photo is too large. Maximum size is 5MB." });
    }

    const originalName = safeFileBaseName(body && body.fileName);
    const ext = extensionForMime(parsed.mimeType);
    const blobName = `${Date.now()}-${crypto.randomUUID()}-${originalName.replace(/\.[^.]+$/, "")}.${ext}`;
    const blockBlobClient = blobContainerClient.getBlockBlobClient(blobName);

    await blockBlobClient.uploadData(parsed.buffer, {
      blobHTTPHeaders: {
        blobContentType: parsed.mimeType,
        blobCacheControl: "public, max-age=31536000, immutable"
      },
      metadata: {
        originalName: originalName.slice(0, 80)
      }
    });

    return send(res, 200, {
      ok: true,
      success: true,
      url: blockBlobClient.url,
      photoUrl: blockBlobClient.url,
      mimeType: parsed.mimeType,
      size: parsed.buffer.length,
      storage: storageMode
    });
  } catch (error) {
    console.error("Photo upload error:", error.message);
    return send(res, 500, { ok: false, success: false, error: "Could not upload photo." });
  }
}

async function initServices() {
  if (COSMOS_ENDPOINT && COSMOS_KEY && CosmosClient) {
    try {
      cosmosClient = new CosmosClient({
        endpoint: COSMOS_ENDPOINT,
        key: COSMOS_KEY,
      });

      cosmosDatabase = cosmosClient.database(COSMOS_DATABASE);

      usersContainer = cosmosDatabase.container(COSMOS_CONTAINER_USERS);
      chatsContainer = cosmosDatabase.container(COSMOS_CONTAINER_CHATS);

      databaseMode = "cosmosdb";

      console.log(`Azure Cosmos DB connected: ${COSMOS_DATABASE}`);
    } catch (error) {
      databaseMode = "local-json";
      usersContainer = null;
      chatsContainer = null;

      console.error(
        "Azure Cosmos DB connection failed. Using local JSON fallback:",
        error.message
      );
    }
  } else {
    console.log("Azure Cosmos DB is not configured yet.");
  }

  if (AZURE_STORAGE_CONNECTION_STRING && BlobServiceClient) {
    try {
      const blobServiceClient =
        BlobServiceClient.fromConnectionString(
          AZURE_STORAGE_CONNECTION_STRING
        );

      blobContainerClient =
        blobServiceClient.getContainerClient(AZURE_BLOB_CONTAINER);

      await blobContainerClient.createIfNotExists({
        access: "blob",
      });

      storageMode = "azure-blob";

      console.log(
        `Azure Blob Storage ready: ${AZURE_BLOB_CONTAINER}`
      );
    } catch (error) {
      storageMode = "not-configured";
      blobContainerClient = null;

      console.error(
        "Azure Blob Storage setup failed:",
        error.message
      );
    }
  } else {
    console.log("Azure Blob Storage is not configured yet.");
  }
}


let io = null;

function cleanDbPathForRoom(rawPath) {
  return normalizeDbPath(rawPath).join("/");
}

function dbRoomName(rawPath) {
  return "db:" + cleanDbPathForRoom(rawPath);
}

function changedDbPaths(rawPath) {
  const parts = normalizeDbPath(rawPath);
  const changedPath = parts.join("/");
  const paths = [];

  for (let i = parts.length; i >= 1; i -= 1) {
    paths.push(parts.slice(0, i).join("/"));
  }

  if (!paths.length) paths.push("");

  return {
    changedPath,
    watchPaths: Array.from(new Set(paths))
  };
}

function notifyDbChanged(rawPath) {
  if (!io) return;

  let change;
  try {
    change = changedDbPaths(rawPath);
  } catch (error) {
    console.log("Realtime notify skipped:", error.message);
    return;
  }

  const payloadBase = {
    path: change.changedPath,
    time: Date.now()
  };

  for (const watchPath of change.watchPaths) {
    io.to(dbRoomName(watchPath)).emit("db:changed", {
      ...payloadBase,
      watchPath
    });
  }
}

async function handleApiDb(req, res, url) {
  try {
    // GET uses query string: /api/db?path=users
    // PUT/PATCH/DELETE from your index.html use body: { path: "...", value: ... }
    let body = {};
    if (req.method !== "GET") {
      body = await readJson(req);
    }

    const rawPath = url.searchParams.get("path") || (body && body.path) || "";

    if (req.method === "GET" && rawPath === ".info/serverTimeOffset") {
      return send(res, 200, { ok: true, success: true, value: 0 });
    }

    const parts = normalizeDbPath(rawPath);

    if (mongoKv) {
      if (req.method === "GET") {
        const value = await getMongoAt(parts);
        return send(res, 200, { ok: true, success: true, value, backend: databaseMode });
      }

      if (req.method === "PUT" || req.method === "POST") {
        const value = body && Object.prototype.hasOwnProperty.call(body, "value") ? body.value : body;
        await enforceRegisteredNameWrite(req, parts, value);
        await setMongoAt(parts, value === undefined ? null : value);
        const nextValue = await getMongoAt(parts);
        notifyDbChanged(rawPath);
        return send(res, 200, { ok: true, success: true, value: nextValue, backend: databaseMode });
      }

      if (req.method === "PATCH") {
        const value = body && Object.prototype.hasOwnProperty.call(body, "value") ? body.value : body;
        await enforceRegisteredNameWrite(req, parts, value);
        await patchMongoAt(parts, value === undefined ? {} : value);
        const nextValue = await getMongoAt(parts);
        notifyDbChanged(rawPath);
        return send(res, 200, { ok: true, success: true, value: nextValue, backend: databaseMode });
      }

      if (req.method === "DELETE") {
        await deleteMongoAt(parts);
        notifyDbChanged(rawPath);
        return send(res, 200, { ok: true, success: true, value: null, backend: databaseMode });
      }
    }

    if (req.method === "GET") {
      const value = getAt(db, parts);
      return send(res, 200, { ok: true, success: true, value, backend: databaseMode });
    }

    if (req.method === "PUT" || req.method === "POST") {
      const value = body && Object.prototype.hasOwnProperty.call(body, "value") ? body.value : body;
      await enforceRegisteredNameWrite(req, parts, value);
      setAt(db, parts, value === undefined ? null : value);
      saveDb();
      notifyDbChanged(rawPath);
      return send(res, 200, { ok: true, success: true, value: getAt(db, parts), backend: databaseMode });
    }

    if (req.method === "PATCH") {
      const value = body && Object.prototype.hasOwnProperty.call(body, "value") ? body.value : body;
      await enforceRegisteredNameWrite(req, parts, value);
      patchAt(db, parts, value === undefined ? {} : value);
      saveDb();
      notifyDbChanged(rawPath);
      return send(res, 200, { ok: true, success: true, value: getAt(db, parts), backend: databaseMode });
    }

    if (req.method === "DELETE") {
      deleteAt(db, parts);
      saveDb();
      notifyDbChanged(rawPath);
      return send(res, 200, { ok: true, success: true, value: null, backend: databaseMode });
    }

    return send(res, 405, { ok: false, success: false, error: "Method not allowed" });
  } catch (e) {
    console.error("API DB error:", e.message);
    const status = Number(e.statusCode || 400);
    return send(res, status, { ok: false, success: false, error: e.message });
  }
}

function safePath(base, requestPath) {
  const decoded = decodeURIComponent(requestPath.split("?")[0]);
  const normalized = path.normalize(decoded).replace(/^(\.\.[\/\\])+/, "");
  const full = path.join(base, normalized);
  if (!full.startsWith(base)) return null;
  return full;
}

function tryReadFile(filePath) {
  try {
    if (filePath && fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
      return fs.readFileSync(filePath);
    }
  } catch {}
  return null;
}

function contentType(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  const types = {
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
    ".svg": "image/svg+xml",
    ".ico": "image/x-icon"
  };
  return types[ext] || "application/octet-stream";
}

function serveStatic(req, res, url) {
  let requestPath = url.pathname;

  let candidates = [];

  if (requestPath === "/") {
    candidates = [
      path.join(PUBLIC_DIR, "index.html"),
      path.join(ROOT_DIR, "index.html"),
      path.join(ROOT_DIR, "publicindex.html"),
      path.join(ROOT_DIR, "hostingstart.html")
    ];
  } else {
    const publicFile = safePath(PUBLIC_DIR, requestPath);
    const rootFile = safePath(ROOT_DIR, requestPath);
    candidates = [publicFile, rootFile];
  }

  for (const filePath of candidates) {
    const data = tryReadFile(filePath);
    if (data) {
      return send(res, 200, data, contentType(filePath));
    }
  }

  return send(res, 404, "Not found", "text/plain");
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  console.log(new Date().toISOString(), req.method, url.pathname, url.searchParams.get("path") || "");

  if (req.method === "OPTIONS") {
    return send(res, 204, "");
  }

  if (url.pathname === "/api/health") {
    return send(res, 200, {
      ok: true,
      success: true,
      app: "LavanderChat",
      api: "running",
      database: databaseMode,
      storage: storageMode,
      email: {
        configured: isEmailSenderConfigured(),
        nodemailerLoaded: Boolean(nodemailer),
        smtpHostSet: Boolean(SMTP_HOST),
        smtpUserSet: Boolean(SMTP_USER),
        smtpPassSet: Boolean(SMTP_PASS),
        mailFromSet: Boolean(MAIL_FROM),
        publicAppUrl: PUBLIC_APP_URL || publicBaseUrl(req)
      },
      googleLogin: {
        configured: isGoogleLoginConfigured(),
        clientIdSet: Boolean(GOOGLE_CLIENT_ID),
        clientSecretSet: Boolean(GOOGLE_CLIENT_SECRET),
        callbackUrl: googleCallbackUrl(req)
      },
      time: Date.now()
    });
  }

  if (url.pathname === "/api/auth/register" && req.method === "POST") {
    return handleAuthRegister(req, res);
  }

  if (url.pathname === "/api/auth/login" && req.method === "POST") {
    return handleAuthLogin(req, res);
  }

  if (url.pathname === "/api/auth/forgot-password" && req.method === "POST") {
    return handleAuthForgotPassword(req, res);
  }

  if (url.pathname === "/api/auth/reset-password" && req.method === "POST") {
    return handleAuthResetPassword(req, res);
  }

  if (url.pathname === "/api/auth/me" && req.method === "GET") {
    return handleAuthMe(req, res);
  }

  if (url.pathname === "/api/auth/name-status" && req.method === "GET") {
    return handleAuthNameStatus(req, res, url);
  }

  if (url.pathname === "/api/auth/profile-photo" && req.method === "POST") {
    return handleAuthProfilePhoto(req, res);
  }

  if (url.pathname === "/api/auth/logout" && req.method === "POST") {
    return handleAuthLogout(req, res);
  }

  if (url.pathname === "/api/auth/google" && (req.method === "GET" || req.method === "POST")) {
    return handleAuthGoogleStart(req, res);
  }

  if ((url.pathname === "/auth/google/callback" || url.pathname === "/api/auth/google/callback") && req.method === "GET") {
    return handleAuthGoogleCallback(req, res, url);
  }

  if (url.pathname === "/api/db") {
    return handleApiDb(req, res, url);
  }

  if (req.method === "POST" && url.pathname === "/api/upload/photo") {
    return handlePhotoUpload(req, res);
  }

  // Original EventSource chat routes, kept so old front-end versions still work.
  if (req.method === "GET" && url.pathname === "/events") {
    const name = cleanName(url.searchParams.get("name"));
    const id = crypto.randomUUID();

    users.set(id, { id, name, joinedAt: Date.now() });

    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      "Connection": "keep-alive",
      "X-Accel-Buffering": "no",
      "Access-Control-Allow-Origin": "*"
    });

    clients.set(id, res);
    sse(res, "me", { id, name });
    broadcastUsers();

    const keepAlive = setInterval(() => {
      res.write(": keep-alive\n\n");
    }, 15000);

    req.on("close", () => {
      clearInterval(keepAlive);
      clients.delete(id);
      users.delete(id);
      broadcastUsers();
    });

    return;
  }

  if (req.method === "POST" && url.pathname === "/history") {
    const body = await readJson(req);
    const me = body?.me;
    const withUserId = body?.withUserId;

    if (!users.has(me) || !users.has(withUserId)) return send(res, 200, []);
    return send(res, 200, conversations.get(key(me, withUserId)) || []);
  }

  if (req.method === "POST" && url.pathname === "/send") {
    const body = await readJson(req);
    const from = body?.from;
    const to = body?.to;
    const text = cleanText(body?.text);

    if (!users.has(from) || !users.has(to) || !text) {
      return send(res, 400, { ok: false, error: "Message could not be sent." });
    }

    const fromUser = users.get(from);
    const toUser = users.get(to);

    const msg = {
      id: crypto.randomUUID(),
      from,
      fromName: fromUser.name,
      to,
      toName: toUser.name,
      text,
      time: Date.now()
    };

    const convKey = key(from, to);
    const list = conversations.get(convKey) || [];
    list.push(msg);
    conversations.set(convKey, list.slice(-200));

    const fromClient = clients.get(from);
    const toClient = clients.get(to);

    if (fromClient) sse(fromClient, "message", msg);
    if (toClient) sse(toClient, "message", msg);

    return send(res, 200, { ok: true });
  }

  if (req.method === "POST" && url.pathname === "/typing") {
    const body = await readJson(req);
    const from = body?.from;
    const to = body?.to;
    const isTyping = Boolean(body?.isTyping);

    if (users.has(from) && users.has(to)) {
      const toClient = clients.get(to);
      if (toClient) {
        sse(toClient, "typing", {
          from,
          fromName: users.get(from).name,
          isTyping
        });
      }
    }

    return send(res, 200, { ok: true });
  }

  if (req.method === "GET") {
    return serveStatic(req, res, url);
  }

  return send(res, 404, { ok: false, error: "Not found" });
});

function setupRealtimeSocketServer() {
  if (!SocketIOServer) {
    console.log("Socket.IO is not available. Install dependencies with npm install.");
    return;
  }

  io = new SocketIOServer(server, {
    cors: {
      origin: "*",
      methods: ["GET", "POST"]
    },
    transports: ["websocket", "polling"],
    pingInterval: 25000,
    pingTimeout: 20000,
    maxHttpBufferSize: 1e6
  });

  io.on("connection", (socket) => {
    socket.data.dbWatchPaths = new Set();
    console.log("Socket connected:", socket.id);

    socket.on("socket:ping", (_payload, ack) => {
      if (typeof ack === "function") {
        ack({ ok: true, id: socket.id, time: Date.now() });
      }
    });

    socket.on("db:watch", (payload, ack) => {
      try {
        const watchPath = cleanDbPathForRoom(payload && payload.path);
        const room = dbRoomName(watchPath);
        socket.join(room);
        socket.data.dbWatchPaths.add(watchPath);
        if (typeof ack === "function") ack({ ok: true, path: watchPath });
      } catch (error) {
        if (typeof ack === "function") ack({ ok: false, error: error.message });
      }
    });

    socket.on("db:unwatch", (payload, ack) => {
      try {
        const watchPath = cleanDbPathForRoom(payload && payload.path);
        const room = dbRoomName(watchPath);
        socket.leave(room);
        socket.data.dbWatchPaths.delete(watchPath);
        if (typeof ack === "function") ack({ ok: true, path: watchPath });
      } catch (error) {
        if (typeof ack === "function") ack({ ok: false, error: error.message });
      }
    });

    socket.on("disconnect", (reason) => {
      console.log("Socket disconnected:", socket.id, reason);
    });
  });

  console.log("Socket.IO realtime database updates enabled.");
}

setupRealtimeSocketServer();

initServices().finally(() => {
  server.listen(PORT, () => {
    console.log(`LavanderChat running on port ${PORT}`);
    console.log(`API test: /api/health`);
    console.log(`Database mode: ${databaseMode}`);
    console.log(`Storage mode: ${storageMode}`);
    if (databaseMode === "local-json") console.log(`DB file: ${DB_FILE}`);
  });
});
