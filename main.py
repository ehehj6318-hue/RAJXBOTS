import os
import io
import time
import asyncio
import sqlite3
import hmac
import hashlib
import base64
import secrets
import re
import json
import logging
from datetime import datetime, date, timedelta, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.responses import HTMLResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler
from pyrogram.errors import (
    FloodWait, UsernameNotOccupied, UsernameInvalid, PeerIdInvalid
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("rajfflive")

# ==================== CONFIG ====================
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "RAJ1122")
UNLIMITED_KEY  = os.getenv("UNLIMITED_KEY",  "RAJFFLIVE")
DB_PATH        = os.getenv("DB_PATH",         "userbot_api.db")
DEVELOPER_TAG  = "@RAJFFLIVE"
BOT_TAG        = "@RAJFFLIVEBOT"

# 🔥 यहाँ से हार्डकोडेड वैल्यूज हटा दी गई हैं, अब यह ENV से उठाएगा
API_ID         = int(os.getenv("API_ID"))
API_HASH       = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")

MAX_CONCURRENT_PER_ACCOUNT = int(os.getenv("MAX_CONCURRENT", "25"))

# ── Timeouts (seconds) ──
# How long we wait for ANY first message from the bot after sending command
BOT_FIRST_MSG_TIMEOUT = int(os.getenv("BOT_FIRST_MSG_TIMEOUT", "20"))
# After first message arrives, how long we wait for the NEXT message
# (handles multi-part responses from bot)
BOT_IDLE_TIMEOUT      = float(os.getenv("BOT_IDLE_TIMEOUT", "2.5"))
# Hard ceiling — no request can take longer than this
BOT_HARD_TIMEOUT      = int(os.getenv("BOT_HARD_TIMEOUT", "30"))

SEND_RETRIES = 3

COMMANDS_MAP = {
    "num":      ("sonambewafabot", "/num"),
    "aadhar":   ("sonambewafabot", "/aadhar"),
    "tg":       ("sonambewafabot", "/tg"),
    "num2":     ("HathixOsint_bot", "/num"),
    "vh2":      ("HathixOsint_bot", "/vh"),
    "upi2":     ("HathixOsint_bot", "/upi"),
    "upiid2":   ("HathixOsint_bot", "/upiid"),
    "fam2":     ("HathixOsint_bot", "/family"),
    "pan2":     ("HathixOsint_bot", "/pan"),
    "tg2":      ("HathixOsint_bot", "/tg"),
    "lookup2":  ("HathixOsint_bot", "/lookup"),
    "rc2":      ("HathixOsint_bot", "/rc"),
    "rcchip2":  ("HathixOsint_bot", "/rcchip"),
    "ifsc2":    ("HathixOsint_bot", "/ifsc"),
    "paninfo2": ("HathixOsint_bot", "/paninfo"),
}

# Words that mean "still working, don't stop yet"
PROCESSING_KEYWORDS = (
    "processing", "searching", "please wait", "fetching",
    "scanning", "loading", "wait", "fetching data",
)

# ── Exact final-answer patterns from sonambewafabot ──────────────────────
# If ANY of these appear in a message, treat it as a COMPLETE final answer
# and return immediately without waiting for idle timeout.
INSTANT_DONE_MARKERS = (
    "no records found",
    "no record found",
    "no data found",
    "not found",
    "no data",
    "not available",
    "no info",
    "lookup failed",
    "invalid number",
    "invalid aadhar",
    "invalid pan",
    "invalid upi",
    "failed",
    "service temporarily unavailable",
    "rate limit",
    "quota exceeded",
    "unable to fetch",
    "❌",           # bot uses this emoji for every error/no-result reply
    "✅",           # bot uses this for completed results
    "records found",
    "result",
    "total records",
    "━━━━━",        # bot uses this as a section separator in final replies
)

# Words that ONLY appear in error/no-result final answers (used in JSON output)
NO_RECORD_MARKERS = (
    "no records found",
    "no record found",
    "no data found",
    "not found",
    "no data",
    "not available",
    "no info",
    "lookup failed",
    "invalid number",
    "invalid aadhar",
    "invalid pan",
    "invalid upi",
    "failed",
    "service temporarily unavailable",
    "rate limit",
    "quota exceeded",
    "unable to fetch",
)

# ==================== QUEUE / ROUTING ====================
# bot_username (lower) -> { sent_message_id -> asyncio.Queue }
pending_queues: dict[str, dict[int, asyncio.Queue]] = {}

# bot_username -> True (supports reply_to) | False (serial only) | None (unknown)
bot_reply_support: dict[str, bool | None] = {}

# bot_username -> Lock used while in serial/probe mode
bot_serial_lock: dict[str, asyncio.Lock] = {}

_queues_lock = asyncio.Lock()


def _is_final_answer(text: str) -> bool:
    """Return True when this message is definitely the last one from the bot."""
    t = text.lower()
    return any(marker in t for marker in INSTANT_DONE_MARKERS)


def _is_still_processing(text: str) -> bool:
    """Return True when the bot is saying 'hang on, working…'"""
    t = text.lower()
    return any(k in t for k in PROCESSING_KEYWORDS)


async def global_message_handler(client: Client, message):
    """
    Route every incoming private message to the right waiting queue.

    Key fix vs original: when a bot does NOT use reply_to (serial bots),
    we route to the OLDEST pending entry for that bot — not to ALL of them.
    Routing to all caused cross-contamination between concurrent requests.
    """
    if message.outgoing:
        return
    if not message.chat or not message.chat.username:
        return
    bot = message.chat.username.lower()

    async with _queues_lock:
        qmap = pending_queues.get(bot)
        if not qmap:
            return

        rt = getattr(message, "reply_to_message_id", None)

        if rt and rt in qmap:
            # Bot replied directly to our sent message — route to that queue
            targets = [qmap[rt]]
        elif rt is None:
            # Bot sent without reply_to — route to the OLDEST pending queue only
            # (oldest = smallest sent_message_id = first in line)
            oldest_id = min(qmap.keys())
            targets = [qmap[oldest_id]]
        else:
            # Bot replied to something else — ignore
            return

    for q in targets:
        try:
            q.put_nowait(message)
        except asyncio.QueueFull:
            pass   # queue full means the waiter isn't consuming — drop safely


# ==================== DATABASE ====================
# Use a single connection with WAL mode + thread-local access instead of
# creating a new connection per request (huge latency killer under load).
_db_local = {}  # thread_id -> conn (FastAPI uses asyncio, so this is effectively per-event-loop)

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_db() -> sqlite3.Connection:
    """Return a reusable WAL connection; create it once per thread."""
    tid = id(asyncio.get_event_loop())
    if tid not in _db_local:
        c = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        c.execute("PRAGMA cache_size=10000")
        _db_local[tid] = c
    return _db_local[tid]


def init_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    try:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS api_keys (
            key TEXT PRIMARY KEY, owner TEXT, expires_at TEXT,
            daily_limit INTEGER DEFAULT -1, total_limit INTEGER DEFAULT -1,
            daily_used INTEGER DEFAULT 0, total_used INTEGER DEFAULT 0,
            last_reset TEXT, is_active INTEGER DEFAULT 1,
            is_permanent INTEGER DEFAULT 0, created_at TEXT, last_used TEXT
        );
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT,
            api_id INTEGER, api_hash TEXT, session_string TEXT,
            is_active INTEGER DEFAULT 1
        );
        """)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(api_keys)").fetchall()}
        for col, sql in {
            "expires_at":  "ALTER TABLE api_keys ADD COLUMN expires_at TEXT",
            "total_limit": "ALTER TABLE api_keys ADD COLUMN total_limit INTEGER DEFAULT -1",
            "daily_limit": "ALTER TABLE api_keys ADD COLUMN daily_limit INTEGER DEFAULT -1",
            "last_reset":  "ALTER TABLE api_keys ADD COLUMN last_reset TEXT",
            "is_permanent":"ALTER TABLE api_keys ADD COLUMN is_permanent INTEGER DEFAULT 0",
            "last_used":   "ALTER TABLE api_keys ADD COLUMN last_used TEXT",
        }.items():
            if col not in cols:
                conn.execute(sql)

        conn.execute(
            "INSERT OR IGNORE INTO api_keys "
            "(key,owner,daily_limit,total_limit,is_active,is_permanent,created_at) "
            "VALUES (?,?,-1,-1,1,1,?)",
            (UNLIMITED_KEY, "Owner", _now()),
        )
        conn.execute(
            "UPDATE api_keys SET is_permanent=1,is_active=1,daily_limit=-1,total_limit=-1 WHERE key=?",
            (UNLIMITED_KEY,),
        )
        if conn.execute("SELECT COUNT(*) FROM accounts").fetchone()[0] == 0:
            conn.execute(
                "INSERT INTO accounts (name,api_id,api_hash,session_string,is_active) VALUES (?,?,?,?,1)",
                ("Account 1", API_ID, API_HASH, SESSION_STRING),
            )
        conn.commit()
    finally:
        conn.close()


# ==================== API KEY AUTH ====================
def check_api_key(api_key: str) -> tuple[bool, str]:
    conn = get_db()
    today = date.today().isoformat()
    try:
        row = conn.execute("SELECT * FROM api_keys WHERE key=?", (api_key,)).fetchone()
        if not row:
            return False, "Invalid API key"
        if not row["is_active"]:
            return False, "API key is paused"

        exp = row["expires_at"]
        if exp:
            try:
                d = datetime.fromisoformat(exp)
                if d.tzinfo is None:
                    d = d.replace(tzinfo=timezone.utc)
                if datetime.now(timezone.utc) > d:
                    return False, "API key expired"
            except ValueError:
                pass

        # Reset daily counter if it's a new day
        if row["last_reset"] != today:
            conn.execute(
                "UPDATE api_keys SET daily_used=0, last_reset=? WHERE key=?",
                (today, api_key),
            )
            conn.commit()
            row = conn.execute("SELECT * FROM api_keys WHERE key=?", (api_key,)).fetchone()

        d_limit = row["daily_limit"]
        t_limit = row["total_limit"]
        if d_limit != -1 and row["daily_used"] >= d_limit:
            return False, "Daily limit reached"
        if t_limit != -1 and row["total_used"] >= t_limit:
            return False, "Total limit reached"

        # Atomic increment with guard
        cur = conn.execute(
            "UPDATE api_keys "
            "SET daily_used=daily_used+1, total_used=total_used+1, last_used=? "
            "WHERE key=? "
            "AND (daily_limit=-1 OR daily_used < daily_limit) "
            "AND (total_limit=-1 OR total_used < total_limit)",
            (_now(), api_key),
        )
        conn.commit()
        if cur.rowcount == 0:
            return False, "Limit reached"
        return True, ""
    finally:
        pass  # keep connection alive (reuse pool)


# ==================== ACCOUNT POOL ====================
active_clients: dict[int, Client] = {}
account_semaphores: dict[int, asyncio.Semaphore] = {}
_rr_index = 0
_rr_lock  = asyncio.Lock()


async def start_account(acc_id, name, api_id, api_hash, session_string):
    if acc_id in active_clients:
        return
    try:
        client = Client(
            name, api_id=api_id, api_hash=api_hash,
            session_string=session_string, in_memory=True,
        )
        client.add_handler(MessageHandler(global_message_handler, filters.private))
        await client.start()
        active_clients[acc_id] = client
        account_semaphores[acc_id] = asyncio.Semaphore(MAX_CONCURRENT_PER_ACCOUNT)
        log.info("Account started: %s (slots=%d)", name, MAX_CONCURRENT_PER_ACCOUNT)
    except Exception as e:
        log.exception("Failed to start account %s: %s", name, e)


async def pick_account() -> tuple[int, Client, asyncio.Semaphore]:
    async with _rr_lock:
        global _rr_index
        if not active_clients:
            raise RuntimeError("No active Telegram accounts")
        ids = list(active_clients.keys())
        # pick least-loaded account instead of pure round-robin
        best_id = min(ids, key=lambda i: account_semaphores[i]._value.__class__.__mro__[0] and
                      MAX_CONCURRENT_PER_ACCOUNT - account_semaphores[i]._value
                      if hasattr(account_semaphores[i], "_value") else 0)
        # fallback to round-robin if introspection unavailable
        try:
            _ = account_semaphores[best_id]._value
        except AttributeError:
            best_id = ids[_rr_index % len(ids)]
            _rr_index = (_rr_index + 1) % len(ids)
        return best_id, active_clients[best_id], account_semaphores[best_id]


# ==================== SAFE SEND ====================
async def safe_send(client: Client, chat: str, text: str):
    last_exc = None
    for attempt in range(SEND_RETRIES):
        try:
            return await client.send_message(chat, text)
        except FloodWait as e:
            wait = min(int(e.value) + 2, 30)
            log.warning("FloodWait %ss sending to %s", wait, chat)
            await asyncio.sleep(wait)
            last_exc = e
        except Exception as e:
            last_exc = e
            if attempt == SEND_RETRIES - 1:
                raise
            log.warning("Send retry %d/%d to %s: %s", attempt + 1, SEND_RETRIES, chat, e)
            await asyncio.sleep(1.0)
    raise last_exc or RuntimeError("send failed")


# ==================== CORE FETCH ====================
async def _fetch_impl(client: Client, bot: str, command: str) -> tuple[list[str], list[str]]:
    """
    Send `command` to `bot` and collect all reply messages.

    Returns (texts, file_contents).

    Key improvements over original:
    - Instant return as soon as a "final answer" message is detected
    - Short BOT_IDLE_TIMEOUT (2.5s) instead of 60s for first-message wait
    - Queue size capped at 50 to prevent memory growth
    - Proper cleanup in finally block always runs
    """
    queue: asyncio.Queue = asyncio.Queue(maxsize=50)

    # Send command and register queue BEFORE yielding control, so we don't
    # miss a very fast bot reply that arrives before we register.
    sent = await safe_send(client, bot, command)
    sent_id = sent.id

    async with _queues_lock:
        pending_queues.setdefault(bot, {})[sent_id] = queue

    texts: dict[int, str] = {}
    files: dict[int, str] = {}
    start = time.monotonic()
    got_first = False

    try:
        while True:
            elapsed = time.monotonic() - start

            # Hard ceiling — never exceed this no matter what
            if elapsed > BOT_HARD_TIMEOUT:
                log.warning("Hard timeout hit for %s %s", bot, command[:30])
                break

            # First-message timeout — give the bot this long to say ANYTHING
            remaining = (BOT_FIRST_MSG_TIMEOUT if not got_first else BOT_HARD_TIMEOUT) - elapsed
            if remaining <= 0:
                break

            try:
                msg = await asyncio.wait_for(queue.get(), timeout=min(remaining, BOT_IDLE_TIMEOUT))
            except asyncio.TimeoutError:
                if got_first:
                    # We already have something and the bot went quiet — done
                    break
                # Still waiting for first message — keep waiting up to first-msg deadline
                if (time.monotonic() - start) >= BOT_FIRST_MSG_TIMEOUT:
                    break
                continue

            # Skip messages the bot sent before our command
            if msg.id <= sent_id and not getattr(msg, "edit_date", None):
                continue

            got_first = True
            text = (msg.text or msg.caption or "").strip()

            # Download .txt file attachments (Hathix bot sends results as files)
            if (msg.document and msg.document.file_name
                    and msg.document.file_name.endswith(".txt")
                    and msg.id not in files):
                try:
                    media = await client.download_media(msg, in_memory=True)
                    if media:
                        raw = media.getvalue() if isinstance(media, io.BytesIO) else bytes(media)
                        files[msg.id] = raw.decode("utf-8", errors="ignore")
                        # File = complete answer, return immediately
                        break
                except Exception as exc:
                    log.warning("File download failed: %s", exc)

            if text:
                texts[msg.id] = text
                # ── Instant-return logic ──
                # If this message is clearly a final answer (error, no-result,
                # or a completed data block), stop waiting immediately instead
                # of sitting out the full idle timeout.
                if _is_final_answer(text) and not _is_still_processing(text):
                    # Small grace: wait up to 0.3s for a possible second part
                    # (some bots send header + data as two rapid messages)
                    try:
                        extra = await asyncio.wait_for(queue.get(), timeout=0.3)
                        extra_text = (extra.text or extra.caption or "").strip()
                        if extra_text:
                            texts[extra.id] = extra_text
                    except asyncio.TimeoutError:
                        pass
                    break

    finally:
        # Always clean up — even if we crash or get cancelled
        async with _queues_lock:
            qmap = pending_queues.get(bot)
            if qmap and sent_id in qmap:
                del qmap[sent_id]
                if not qmap:
                    del pending_queues[bot]

    t_out = [texts[k] for k in sorted(texts)]
    f_out = [files[k] for k in sorted(files)]
    return t_out, f_out


async def fetch_bot_response(client: Client, bot_username: str, command: str):
    """
    Thin wrapper that handles serial vs parallel mode per bot.

    Original had a bug: it acquired a serial lock, ran serial, then
    unconditionally called parallel again — doing DOUBLE the work.
    Fixed: serial lock is used for bots that need it; once a bot is
    confirmed parallel-capable we bypass the lock entirely.
    """
    bot = bot_username.lower()
    support = bot_reply_support.get(bot)   # True | False | None

    if support is True:
        # Bot supports reply_to — fully parallel, no lock needed
        return await _fetch_impl(client, bot, command)

    # Unknown or serial-only — use a per-bot lock so only one request
    # runs at a time for this bot (prevents cross-contamination)
    lock = bot_serial_lock.setdefault(bot, asyncio.Lock())
    async with lock:
        texts, files = await _fetch_impl(client, bot, command)

    return texts, files


# ==================== PARSERS ====================
def parse_hathix_txt(txt: str) -> list[dict]:
    results, current = [], {}
    skip_terms = ("raw", "unique", "hits", "coverage", "time", "ref", "target", "info")
    key_map = {
        "name": "name", "owner_name": "name", "full_name": "name",
        "username": "username", "upi_id": "upi_id", "handle": "handle",
        "valid": "valid", "account_type": "account_type",
        "bank": "bank", "bank_id": "bank_id", "app": "app",
        "handle_bank": "handle_bank",
    }
    for raw in txt.split("\n"):
        line = raw.strip()
        if not line:
            continue
        if line.upper().startswith("RECORD #"):
            if current:
                results.append(current)
                current = {}
            continue
        if ":" not in line or line.startswith("["):
            continue
        k, _, v = line.partition(":")
        k_clean = re.sub(r"[ /-]+", "_", k.strip().lower())
        if any(t in k_clean for t in skip_terms):
            continue
        if k_clean in key_map:
            mk = key_map[k_clean]
        elif "full_name" in k_clean:
            mk = "name"
        elif "father" in k_clean:
            mk = "father_name"
        elif "address" in k_clean:
            mk = "address"
        elif "circle" in k_clean:
            mk = "circle"
        elif "linked" in k_clean:
            mk = "linked_id"
        elif "alt" in k_clean:
            mk = "alt_number"
        elif "email" in k_clean:
            mk = "email"
        elif "mobile" in k_clean:
            mk = "mobile_number"
        else:
            mk = k_clean
        current[mk] = v.strip()
    if current:
        results.append(current)
    return results


def parse_text_records(raw_text: str) -> list[dict]:
    """
    Parse the structured reply format used by sonambewafabot.

    Example bot reply:
        📋 Record 1
        ━━━━━━━━━━━━━━━━━━━━━━━
        👤 Name: Rahul Sharma
        👨‍👦 Father's Name: Ramesh Sharma
        📱 Alternate Number: 9876543210
        📧 Email: rahul@gmail.com
        🏠 Address: Mumbai, Maharashtra
        ━━━━━━━━━━━━━━━━━━━━━━━
    """
    results = []
    # Extended field map — covers all known bot reply formats
    field_map = [
        ("👤 Name:",                "name"),
        ("Name:",                   "name"),
        ("👨‍👦 Father's Name:",       "father_name"),
        ("Father:",                 "father_name"),
        ("📍 Circle:",              "circle"),
        ("Circle:",                 "circle"),
        ("🆔 ID:",                  "id_number"),
        ("ID:",                     "id_number"),
        ("📱 Alternate Number:",     "alternate_number"),
        ("Alternate:",              "alternate_number"),
        ("Alt Number:",             "alternate_number"),
        ("📧 Email:",               "email"),
        ("Email:",                  "email"),
        ("🏠 Address:",             "address"),
        ("Address:",                "address"),
        ("• Tg Id:",               "tg_id"),
        ("Tg Id:",                  "tg_id"),
        ("• Country:",             "country"),
        ("Country:",                "country"),
        ("• Country Code:",        "country_code"),
        ("Country Code:",           "country_code"),
        ("• Number:",              "number"),
        ("Number:",                 "number"),
        ("📞 Number:",              "number"),
        ("Mobile:",                 "mobile"),
        ("Phone:",                  "phone"),
        ("Operator:",               "operator"),
        ("State:",                  "state"),
        ("City:",                   "city"),
        ("District:",               "district"),
        ("Pincode:",                "pincode"),
        ("DOB:",                    "dob"),
        ("Gender:",                 "gender"),
        ("Account Type:",           "account_type"),
        ("Bank:",                   "bank"),
        ("IFSC:",                   "ifsc"),
    ]

    # Split on record separators — handle both "📋 Record N" and "━━━" block style
    segments = re.split(r"(?:📋\s*Record\s*\d+|─{5,}|━{5,})", raw_text)

    for seg in segments:
        seg = seg.strip()
        if not seg:
            continue
        data: dict = {}
        for line in seg.split("\n"):
            line = line.strip()
            if not line:
                continue
            for marker, key in field_map:
                if line.startswith(marker):
                    val = line[len(marker):].strip()
                    if val and val not in ("N/A", "None", ""):
                        data[key] = val
                    break
        if data:
            results.append(data)
    return results


def extract_no_info_message(raw_text: str) -> str | None:
    """
    Extract a clean no-result / error message from bot reply.
    Returns None if the reply looks like actual data.
    """
    if not raw_text:
        return None

    # Strip emoji and formatting for matching
    clean = raw_text.strip()
    low   = clean.lower()

    # Check for sonambewafabot's specific error patterns
    patterns = [
        (r"🔍\s*Number Search\s*[\d]+\s*❌\s*No Records Found",  "No Records Found"),
        (r"❌\s*Invalid Aadhar number",                           "Invalid Aadhar number! Must be 12 digits."),
        (r"❌\s*Invalid number[^!]*!",                            "Invalid number! Must be 10 digits."),
        (r"❌\s*Error:\s*Service temporarily unavailable",        "Service temporarily unavailable. Please try again later."),
        (r"❌\s*No Records Found",                                "No Records Found"),
        (r"❌\s*Not Found",                                       "Not Found"),
    ]
    for pattern, clean_msg in patterns:
        if re.search(pattern, clean, re.IGNORECASE | re.DOTALL):
            return clean_msg

    # Generic no-record check
    for marker in NO_RECORD_MARKERS:
        if marker in low:
            # Return the first non-empty line as the error message
            for line in clean.split("\n"):
                stripped = line.strip()
                if stripped and not stripped.startswith(("🔍", "━")):
                    # Remove leading emojis for a clean message
                    stripped = re.sub(r"^[❌✅⚠️🔍📋\s]+", "", stripped).strip()
                    return stripped or "No Info Found"
    return None


# ==================== ADMIN AUTH ====================
ADMIN_SECRET = hashlib.sha256(f"{ADMIN_PASSWORD}RAJ".encode()).hexdigest()

def make_admin_token() -> str:
    p = f"admin:{int(time.time())}"
    s = hmac.new(ADMIN_SECRET.encode(), p.encode(), hashlib.sha256).hexdigest()[:16]
    return base64.urlsafe_b64encode(f"{p}:{s}".encode()).decode()

def verify_admin_token(t: str) -> bool:
    try:
        d = base64.urlsafe_b64decode(t.encode()).decode()
        p, s = d.rsplit(":", 1)
        exp = hmac.new(ADMIN_SECRET.encode(), p.encode(), hashlib.sha256).hexdigest()[:16]
        if not hmac.compare_digest(s, exp):
            return False
        return (time.time() - int(p.split(":", 1)[1])) <= 86400
    except Exception:
        return False

def require_admin(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing admin token")
    if not verify_admin_token(authorization.split(" ", 1)[1]):
        raise HTTPException(401, "Invalid or expired token")
    return True


# ==================== FASTAPI ====================
class _Done(Exception):
    pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    conn = get_db()
    accs = conn.execute("SELECT * FROM accounts WHERE is_active=1").fetchall()
    tasks = [
        asyncio.create_task(start_account(
            a["id"], a["name"], a["api_id"], a["api_hash"], a["session_string"]
        ))
        for a in accs
    ]
    yield
    for t in tasks:
        t.cancel()
    for c in list(active_clients.values()):
        try:
            await c.stop()
        except Exception:
            pass


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


# ── Main search endpoint ──────────────────────────────────────────────────
@app.get("/{search_type}/{query}")
async def search_get(
    search_type: str,
    query: str,
    api_key: str = None,
):
    t0 = time.perf_counter()
    is_tg = (search_type == "tg")
    empty_data: dict | list = {} if is_tg else []

    final: dict = {
        "success": True,
        "query": query,
        "type": search_type,
        "total_results": 0,
    }
    status_code = 200
    sem = None

    # ── Validate inputs ───────────────────────────────────────────────────
    if not api_key:
        final.update(success=False,
                     message="API key required. Add ?api_key=YOUR_KEY",
                     data=empty_data)
        status_code = 401
    elif search_type not in COMMANDS_MAP:
        final.update(success=False,
                     message=f"Invalid search type '{search_type}'. "
                             f"Valid: {', '.join(COMMANDS_MAP)}",
                     data=empty_data)
        status_code = 400
    else:
        ok, err = check_api_key(api_key)
        if not ok:
            final.update(success=False, message=err, data=empty_data)
            status_code = 403
        else:
            try:
                acc_id, client, account_sem = await pick_account()

                # Acquire concurrency slot (wait max 15s before giving 503)
                try:
                    await asyncio.wait_for(account_sem.acquire(), timeout=15)
                    sem = account_sem
                except asyncio.TimeoutError:
                    final.update(success=False,
                                 message="Server busy — too many concurrent requests. Try again shortly.",
                                 data=empty_data)
                    status_code = 503
                    raise _Done()

                bot_username, cmd_prefix = COMMANDS_MAP[search_type]

                # ── TG lookup ─────────────────────────────────────────────
                if is_tg:
                    tg_data: dict = {}
                    command = None

                    if query.lstrip("-").isdigit():
                        tg_data["user_id"] = query
                        command = f"{cmd_prefix} {query}"
                    else:
                        target = query if query.startswith("@") else "@" + query
                        try:
                            u = await asyncio.wait_for(client.get_users(target), timeout=15)
                            if isinstance(u, list):
                                u = u[0]
                            if u is None:
                                raise UsernameNotOccupied(target)
                            tg_data = {
                                "name":     (f"{u.first_name or ''} {u.last_name or ''}").strip(),
                                "username": f"@{u.username}" if u.username else "N/A",
                                "user_id":  str(u.id),
                            }
                            command = f"{cmd_prefix} {u.id}"
                        except (UsernameNotOccupied, UsernameInvalid, PeerIdInvalid):
                            final.update(success=False,
                                         message=f"Telegram user '{target}' not found",
                                         data={}, total_results=0)
                            status_code = 404
                        except asyncio.TimeoutError:
                            final.update(success=False,
                                         message="Telegram resolve timed out. Try again.",
                                         data={}, total_results=0)
                            status_code = 504
                        except Exception as e:
                            log.warning("get_users(%s) failed: %s", target, e)
                            final.update(success=False,
                                         message=f"Telegram resolve failed: {e}",
                                         data={}, total_results=0)
                            status_code = 502

                    if command:
                        texts, files = await fetch_bot_response(client, bot_username, command)
                        raw = "\n".join(texts)

                        if not texts and not files:
                            final.update(success=False,
                                         message="No response from bot. The OSINT bot may be down or rate-limiting.",
                                         data={}, total_results=0)
                            status_code = 504
                        else:
                            no_info = extract_no_info_message(raw)
                            if no_info:
                                final.update(success=False, message=no_info,
                                             data={}, total_results=0)
                            else:
                                recs = parse_text_records(raw)
                                if recs:
                                    recs[0].pop("tg_id", None)
                                    tg_data.update(recs[0])
                                    final.update(data=tg_data, total_results=1)
                                else:
                                    # Bot replied but we couldn't parse — return raw
                                    tg_data["raw_reply"] = raw[:800]
                                    final.update(data=tg_data, total_results=1)

                # ── All other lookups ──────────────────────────────────────
                else:
                    command = f"{cmd_prefix} {query}"
                    texts, files = await fetch_bot_response(client, bot_username, command)
                    raw = "\n".join(texts) + "\n" + "\n".join(files)

                    if not texts and not files:
                        final.update(success=False,
                                     message="No response from bot. The OSINT bot may be down or rate-limiting.",
                                     data=[], total_results=0)
                        status_code = 504
                    else:
                        no_info = extract_no_info_message(raw)
                        if no_info:
                            final.update(success=False, message=no_info,
                                         data=[], total_results=0)
                        elif files:
                            # Hathix sends results as .txt files
                            parsed = parse_hathix_txt("\n".join(files))
                            if parsed:
                                final.update(data=parsed, total_results=len(parsed))
                            else:
                                final.update(success=False, message="No Info Found",
                                             data=[], total_results=0)
                        else:
                            parsed = parse_text_records(raw)
                            if parsed:
                                if search_type in ("num", "aadhar"):
                                    for r in parsed:
                                        r["query"] = query
                                final.update(data=parsed, total_results=len(parsed))
                            else:
                                # Couldn't parse but have raw text — return it
                                final.update(success=False,
                                             message="No parseable records found",
                                             raw_reply=raw[:800],
                                             data=[], total_results=0)

            except _Done:
                pass
            except Exception as e:
                log.exception("Search error for %s/%s", search_type, query)
                final.update(success=False,
                             message=f"Internal error: {type(e).__name__}: {e}",
                             data=empty_data)
                status_code = 500
            finally:
                if sem is not None:
                    try:
                        sem.release()
                    except Exception:
                        pass

    # ── Standard footer — plain text, no smallcaps ───────────────────────
    final["developer"] = DEVELOPER_TAG
    final["bot"]       = BOT_TAG
    final["time_taken"] = f"{time.perf_counter() - t0:.2f}s"

    return Response(
        content=json.dumps(final, indent=2, ensure_ascii=False),
        media_type="application/json; charset=utf-8",
        status_code=status_code,
    )


# ==================== HEALTH ====================
@app.get("/healthz")
async def healthz():
    return {
        "status": "ok",
        "time": _now(),
        "accounts": len(active_clients),
        "max_concurrent_per_account": MAX_CONCURRENT_PER_ACCOUNT,
        "bots_parallel": [b for b, v in bot_reply_support.items() if v is True],
        "bots_serial":   [b for b, v in bot_reply_support.items() if v is False],
        "pending_queues": {b: len(q) for b, q in pending_queues.items()},
    }


# ==================== ADMIN HTML ====================
ADMIN_HTML = r"""<!doctype html>
<html><head><meta charset="utf-8"><title>RAJ Admin Panel</title>
<style>
*{box-sizing:border-box}
body{font-family:system-ui,sans-serif;background:#0b0b0d;color:#eee;margin:0;padding:24px}
h1,h2{font-weight:600;margin:0 0 12px}
input,button,select{padding:8px 12px;border-radius:6px;border:1px solid #333;background:#16161a;color:#eee;margin:4px;font-size:13px}
button{cursor:pointer;background:#26262e;transition:background .15s}
button:hover{background:#333}
table{width:100%;border-collapse:collapse;margin-top:12px;font-size:13px}
th,td{padding:8px 10px;border-bottom:1px solid #1e1e24;text-align:left}
th{background:#16161a;font-weight:600}
.card{background:#16161a;padding:20px;border-radius:12px;margin-bottom:16px;border:1px solid #1e1e24}
.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:11px}
.on{background:#1a3a1a;color:#4ade80}.off{background:#3a1a1a;color:#f87171}
pre{background:#0f0f12;padding:12px;border-radius:8px;overflow:auto;font-size:12px;border:1px solid #1e1e24}
small{color:#666}
</style></head>
<body>
<h1>RAJ Engine — Admin</h1>
<div class="card" id="login">
  <h2>Login</h2>
  <input id="pw" type="password" placeholder="Admin password"/>
  <button onclick="login()">Login</button>
  <small id="lerr" style="color:#f87171;margin-left:8px"></small>
</div>
<div id="panel" style="display:none">
  <div class="card"><h2>System Stats</h2><pre id="stats">Loading…</pre></div>
  <div class="card">
    <h2>Create API Key</h2>
    <input id="kowner" placeholder="Owner name"/>
    <input id="kkey"   placeholder="Custom key (blank = auto-generate)"/>
    <input id="kdays"  type="number" placeholder="Expires in days (0 = never)" value="0"/>
    <input id="klimit" type="number" placeholder="Daily limit (-1 = unlimited)" value="-1"/>
    <button onclick="createKey()">Create Key</button>
    <pre id="kresult" style="display:none"></pre>
  </div>
  <div class="card">
    <h2>API Keys <button onclick="loadKeys()" style="font-size:11px;padding:4px 8px">Refresh</button></h2>
    <table id="ktable">
      <thead><tr><th>Key</th><th>Owner</th><th>Daily Used/Limit</th><th>Total</th><th>Expires</th><th>Status</th><th>Actions</th></tr></thead>
      <tbody></tbody>
    </table>
  </div>
  <div class="card">
    <h2>Accounts <button onclick="loadAccounts()" style="font-size:11px;padding:4px 8px">Refresh</button></h2>
    <table id="atable">
      <thead><tr><th>ID</th><th>Name</th><th>Connected</th><th>Actions</th></tr></thead>
      <tbody></tbody>
    </table>
  </div>
</div>
<script>
let TOKEN = localStorage.getItem("raj_tok")||"";
const H = ()=>({"Authorization":"Bearer "+TOKEN,"Content-Type":"application/json"});

async function login(){
  const r=await fetch("/admin/login",{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify({password:document.getElementById("pw").value})});
  if(!r.ok){document.getElementById("lerr").textContent="Wrong password";return}
  TOKEN=(await r.json()).token;
  localStorage.setItem("raj_tok",TOKEN);
  show();
}

async function loadStats(){
  const r=await fetch("/admin/api/stats",{headers:H()});
  document.getElementById("stats").textContent=JSON.stringify(await r.json(),null,2);
}

async function loadKeys(){
  const r=await fetch("/admin/api/keys",{headers:H()});
  const j=await r.json();
  const tb=document.querySelector("#ktable tbody");
  tb.innerHTML="";
  for(const k of j.keys){
    const tr=document.createElement("tr");
    const dl=k.daily_limit==-1?"∞":k.daily_limit;
    const tl=k.total_limit==-1?"∞":k.total_limit;
    const exp=k.expires_at?k.expires_at.slice(0,10):"Never";
    const badge=k.is_active?'<span class="badge on">Active</span>':'<span class="badge off">Paused</span>';
    tr.innerHTML=`<td><code>${k.key}</code></td><td>${k.owner||""}</td>
      <td>${k.daily_used}/${dl}</td><td>${k.total_used}/${tl}</td>
      <td>${exp}</td><td>${badge}</td>`;
    const td=document.createElement("td");
    const b1=document.createElement("button");
    b1.textContent=k.is_active?"Pause":"Resume";
    b1.onclick=async()=>{await fetch("/admin/api/keys/"+k.key+"/toggle",{method:"POST",headers:H()});loadKeys()};
    const b2=document.createElement("button");
    b2.textContent="Delete"; b2.style.color="#f87171";
    b2.onclick=async()=>{if(!confirm("Delete key "+k.key+"?"))return;
      await fetch("/admin/api/keys/"+k.key,{method:"DELETE",headers:H()});loadKeys()};
    td.append(b1,b2); tr.append(td); tb.append(tr);
  }
}

async function createKey(){
  const body={
    owner:document.getElementById("kowner").value,
    custom_key:document.getElementById("kkey").value,
    expires_days:+document.getElementById("kdays").value,
    daily_limit:+document.getElementById("klimit").value
  };
  const r=await fetch("/admin/api/keys",{method:"POST",headers:H(),body:JSON.stringify(body)});
  const pre=document.getElementById("kresult");
  pre.style.display="block";
  pre.textContent=JSON.stringify(await r.json(),null,2);
  loadKeys();
}

async function loadAccounts(){
  const r=await fetch("/admin/api/accounts",{headers:H()});
  const j=await r.json();
  const tb=document.querySelector("#atable tbody");
  tb.innerHTML="";
  for(const a of j.accounts){
    const tr=document.createElement("tr");
    const badge=a.connected?'<span class="badge on">Connected</span>':'<span class="badge off">Offline</span>';
    tr.innerHTML=`<td>${a.id}</td><td>${a.name}</td><td>${badge}</td>`;
    const td=document.createElement("td");
    const b=document.createElement("button");
    b.textContent="Remove"; b.style.color="#f87171";
    b.onclick=async()=>{if(!confirm("Remove account "+a.name+"?"))return;
      await fetch("/admin/api/accounts/"+a.id,{method:"DELETE",headers:H()});loadAccounts()};
    td.append(b); tr.append(td); tb.append(tr);
  }
}

function show(){
  document.getElementById("login").style.display="none";
  document.getElementById("panel").style.display="block";
  loadStats(); loadKeys(); loadAccounts();
  setInterval(loadStats, 30000);
}
if(TOKEN) show();
</script></body></html>"""


@app.get("/admin", response_class=HTMLResponse)
async def admin_panel_page():
    return HTMLResponse(ADMIN_HTML)


# ── Admin API ────────────────────────────────────────────────────────────
class LoginReq(BaseModel):
    password: str

class KeyReq(BaseModel):
    owner: str
    custom_key: str = ""
    expires_days: int = 0
    daily_limit: int = -1

class AccReq(BaseModel):
    name: str
    api_id: int
    api_hash: str
    session_string: str


@app.post("/admin/login")
async def admin_login(req: LoginReq):
    if not hmac.compare_digest(req.password, ADMIN_PASSWORD):
        raise HTTPException(401, "Wrong password")
    return {"token": make_admin_token()}


@app.get("/admin/api/stats")
async def admin_stats(_=Depends(require_admin)):
    conn = get_db()
    tk = conn.execute("SELECT COUNT(*) c FROM api_keys").fetchone()["c"]
    tr = conn.execute("SELECT COALESCE(SUM(total_used),0) s FROM api_keys").fetchone()["s"]
    today = date.today().isoformat()
    dr = conn.execute(
        "SELECT COALESCE(SUM(CASE WHEN last_reset=? THEN daily_used ELSE 0 END),0) s FROM api_keys",
        (today,),
    ).fetchone()["s"]
    return {
        "total_keys": tk,
        "total_requests": tr,
        "daily_requests": dr,
        "accounts_connected": len(active_clients),
        "max_concurrent_per_account": MAX_CONCURRENT_PER_ACCOUNT,
        "pending_requests": sum(len(q) for q in pending_queues.values()),
        "bots_parallel": [b for b, v in bot_reply_support.items() if v is True],
        "bots_serial":   [b for b, v in bot_reply_support.items() if v is False],
    }


@app.get("/admin/api/keys")
async def admin_list_keys(_=Depends(require_admin)):
    conn = get_db()
    rows = [dict(r) for r in
            conn.execute("SELECT * FROM api_keys ORDER BY created_at DESC").fetchall()]
    return {"keys": rows}


@app.post("/admin/api/keys")
async def admin_create_key(req: KeyReq, _=Depends(require_admin)):
    key = req.custom_key.strip() or ("RAJ" + secrets.token_hex(8).upper())
    exp = (
        (datetime.now(timezone.utc) + timedelta(days=req.expires_days)).isoformat()
        if req.expires_days > 0 else None
    )
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO api_keys (key,owner,expires_at,daily_limit,total_limit,is_active,is_permanent,created_at) "
            "VALUES (?,?,?,?,-1,1,0,?)",
            (key, req.owner, exp, req.daily_limit, _now()),
        )
        conn.commit()
        return {"key": key, "owner": req.owner, "daily_limit": req.daily_limit, "expires": exp}
    except sqlite3.IntegrityError:
        raise HTTPException(400, "API key already exists")


@app.delete("/admin/api/keys/{key}")
async def admin_delete_key(key: str, _=Depends(require_admin)):
    if key == UNLIMITED_KEY:
        raise HTTPException(400, "Cannot delete the permanent master key")
    conn = get_db()
    conn.execute("DELETE FROM api_keys WHERE key=?", (key,))
    conn.commit()
    return {"ok": True}


@app.post("/admin/api/keys/{key}/toggle")
async def admin_toggle_key(key: str, _=Depends(require_admin)):
    if key == UNLIMITED_KEY:
        raise HTTPException(400, "Cannot pause the permanent master key")
    conn = get_db()
    conn.execute("UPDATE api_keys SET is_active = 1 - is_active WHERE key=?", (key,))
    conn.commit()
    return {"ok": True}


@app.get("/admin/api/accounts")
async def admin_list_accounts(_=Depends(require_admin)):
    conn = get_db()
    rows = [dict(r) for r in
            conn.execute("SELECT id,name,api_id,is_active FROM accounts").fetchall()]
    for r in rows:
        r["connected"] = r["id"] in active_clients
    return {"accounts": rows}


@app.post("/admin/api/accounts")
async def admin_add_account(req: AccReq, _=Depends(require_admin)):
    conn = get_db()
    cur = conn.execute(
        "INSERT INTO accounts (name,api_id,api_hash,session_string,is_active) VALUES (?,?,?,?,1)",
        (req.name, req.api_id, req.api_hash, req.session_string),
    )
    acc_id = cur.lastrowid
    conn.commit()
    asyncio.create_task(start_account(acc_id, req.name, req.api_id, req.api_hash, req.session_string))
    return {"ok": True, "id": acc_id}


@app.delete("/admin/api/accounts/{acc_id}")
async def admin_del_account(acc_id: int, _=Depends(require_admin)):
    client = active_clients.pop(acc_id, None)
    account_semaphores.pop(acc_id, None)
    if client:
        try:
            await client.stop()
        except Exception:
            pass
    conn = get_db()
    conn.execute("DELETE FROM accounts WHERE id=?", (acc_id,))
    conn.commit()
    return {"ok": True}


# ==================== ENTRY POINT ====================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        log_level=os.getenv("LOG_LEVEL", "info"),
        # Allow many concurrent connections
        limit_concurrency=500,
        timeout_keep_alive=30,
    )
