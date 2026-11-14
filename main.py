"""
RAJ OSINT Userbot API — Fixed Version
======================================
Fixes applied vs previous version:
  1. Processing messages filtered out — API waits for real result, never
     returns "Processing..." or "🔄 Processing TG request..." as data.
  2. Queue-bot support — detects position queue ("Aapki baari: Xm Ys mein"),
     waits silently, re-sends original command on "ab aapki baari hai",
     then collects and returns the real result.
  3. API key deletion/corruption fixed — fresh SQLite connection per
     operation instead of WAL connection reuse (the reuse was causing
     uncommitted transactions to silently discard new keys).
  4. pick_account() simplified — no broken semaphore introspection.
  5. Serial lock correctly scoped — no double-fetch bug from original.
  6. BOT_QUEUE_TIMEOUT added for queue-based bots (default 3 min).
"""

import os, io, time, asyncio, sqlite3, hmac, hashlib, base64, secrets
import re, json, logging
from datetime import datetime, date, timedelta, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.responses import HTMLResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler
from pyrogram.errors import FloodWait, UsernameNotOccupied, UsernameInvalid, PeerIdInvalid

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("rajfflive")

# ===================================================================
# CONFIG
# ===================================================================
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "RAJ1122")
UNLIMITED_KEY  = os.getenv("UNLIMITED_KEY",  "RAJFFLIVE")
DB_PATH        = os.getenv("DB_PATH",         "userbot_api.db")
DEVELOPER_TAG  = "@RAJFFLIVE"
BOT_TAG        = "@RAJFFLIVEBOT"

API_ID         = int(os.getenv("API_ID", "0") or "0")
API_HASH       = os.getenv("API_HASH", "")
SESSION_STRING = os.getenv("SESSION_STRING", "")

MAX_CONCURRENT_PER_ACCOUNT = int(os.getenv("MAX_CONCURRENT", "25"))

# Timeouts (seconds)
BOT_FIRST_MSG_TIMEOUT = int(os.getenv("BOT_FIRST_MSG_TIMEOUT", "20"))
BOT_IDLE_TIMEOUT      = float(os.getenv("BOT_IDLE_TIMEOUT",    "3.0"))
BOT_HARD_TIMEOUT      = int(os.getenv("BOT_HARD_TIMEOUT",      "35"))
BOT_QUEUE_TIMEOUT     = int(os.getenv("BOT_QUEUE_TIMEOUT",     "180"))  # 3 min for queued bots

SEND_RETRIES = 3

# ===================================================================
# BOT COMMANDS MAP
# ===================================================================
COMMANDS_MAP = {
    "num":      ("sonambewafabot",  "/num"),
    "aadhar":   ("sonambewafabot",  "/aadhar"),
    "tg":       ("sonambewafabot",  "/tg"),
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

# ===================================================================
# MESSAGE CLASSIFIER
# ===================================================================

# Bot is saying "working, hang on" — do NOT return to caller, keep waiting
_PROCESSING_RE = re.compile(
    r"(🔄|processing|searching|please wait|fetching|scanning|loading|"
    r"🔍\s*tg\s*id\s*:|wait kar)",
    re.IGNORECASE,
)

# Bot put us in a position queue — keep waiting (much longer timeout)
_QUEUE_WAIT_RE = re.compile(
    r"(aapki baari.*mein|position\s*#\d+|time aane par tag|dobara bhej|"
    r"aap.*line mein|wait.*queue|queue.*position|⏳.*wait.*position)",
    re.IGNORECASE,
)

# Bot says "it's your turn now" — re-send original command to get real result
_QUEUE_TURN_RE = re.compile(
    r"(ab aapki baari hai|aap ab search kar sakte|bas upar diya command bhej|"
    r"aapki baari hai|your turn now|ab search karo)",
    re.IGNORECASE,
)

# This message is a COMPLETE final answer — return immediately
_FINAL_RE = re.compile(
    r"(📋\s*record\s*\d+|━{3,}|no records found|no record found|"
    r"invalid number|invalid aadhar|invalid pan|invalid upi|"
    r"not found|no data|no info|lookup failed|"
    r"service temporarily unavailable|rate limit|quota exceeded|"
    r"❌\s*(no|invalid|error|not)|"
    r"total records|records found)",
    re.IGNORECASE,
)

# No-info patterns mapped to clean messages returned to caller
_NO_INFO_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"🔍\s*Number Search.*?❌\s*No Records Found", re.DOTALL | re.I),
     "No Records Found"),
    (re.compile(r"❌\s*Invalid Aadhar number", re.I),
     "Invalid Aadhar number! Must be 12 digits."),
    (re.compile(r"❌\s*Invalid number[^!]*!", re.I),
     "Invalid number! Must be 10 digits."),
    (re.compile(r"❌\s*Error:\s*Service temporarily unavailable", re.I),
     "Service temporarily unavailable. Please try again later."),
    (re.compile(r"❌\s*No Records Found", re.I),     "No Records Found"),
    (re.compile(r"❌\s*Not Found", re.I),             "Not Found"),
    (re.compile(r"no records found", re.I),           "No Records Found"),
    (re.compile(r"not found", re.I),                  "Not Found"),
    (re.compile(r"invalid number", re.I),             "Invalid number"),
    (re.compile(r"service temporarily unavailable",re.I),
     "Service temporarily unavailable. Please try again later."),
    (re.compile(r"rate limit|quota exceeded", re.I),
     "Rate limit reached. Please try again later."),
    (re.compile(r"no data|no info|no information", re.I), "No Information Found"),
]


def _is_processing(t: str) -> bool:
    return bool(_PROCESSING_RE.search(t))

def _is_queue_wait(t: str) -> bool:
    return bool(_QUEUE_WAIT_RE.search(t))

def _is_queue_turn(t: str) -> bool:
    return bool(_QUEUE_TURN_RE.search(t))

def _is_final(t: str) -> bool:
    return bool(_FINAL_RE.search(t))

def _extract_no_info(raw: str) -> str | None:
    if not raw:
        return None
    for pat, msg in _NO_INFO_PATTERNS:
        if pat.search(raw):
            return msg
    return None


# ===================================================================
# QUEUE / MESSAGE ROUTING
# ===================================================================
# bot_username (lower) -> { sent_message_id -> asyncio.Queue }
pending_queues: dict[str, dict[int, asyncio.Queue]] = {}
# bot_username -> asyncio.Lock  (serial access per bot)
bot_serial_lock: dict[str, asyncio.Lock] = {}
_queues_lock = asyncio.Lock()


async def global_message_handler(client: Client, message):
    """Route incoming private messages to the correct waiting queue."""
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
            # Bot replied directly to our message
            targets = [qmap[rt]]
        elif rt is None:
            # Bot sent without reply_to — route to OLDEST pending queue only
            # (prevents cross-contamination between concurrent requests)
            oldest_id = min(qmap.keys())
            targets = [qmap[oldest_id]]
        else:
            return   # reply to something else — ignore

    for q in targets:
        try:
            q.put_nowait(message)
        except asyncio.QueueFull:
            pass


# ===================================================================
# DATABASE  (fresh connection per operation — no reuse corruption)
# ===================================================================
def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _db() -> sqlite3.Connection:
    """Open a fresh WAL connection. Caller MUST close it."""
    c = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    return c


def init_db():
    conn = _db()
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
        # Safe migration
        cols = {r[1] for r in conn.execute("PRAGMA table_info(api_keys)").fetchall()}
        for col, sql in {
            "expires_at":   "ALTER TABLE api_keys ADD COLUMN expires_at TEXT",
            "total_limit":  "ALTER TABLE api_keys ADD COLUMN total_limit INTEGER DEFAULT -1",
            "daily_limit":  "ALTER TABLE api_keys ADD COLUMN daily_limit INTEGER DEFAULT -1",
            "last_reset":   "ALTER TABLE api_keys ADD COLUMN last_reset TEXT",
            "is_permanent": "ALTER TABLE api_keys ADD COLUMN is_permanent INTEGER DEFAULT 0",
            "last_used":    "ALTER TABLE api_keys ADD COLUMN last_used TEXT",
        }.items():
            if col not in cols:
                conn.execute(sql)
        # Seed master key
        conn.execute(
            "INSERT OR IGNORE INTO api_keys "
            "(key,owner,daily_limit,total_limit,is_active,is_permanent,created_at) "
            "VALUES (?,?,-1,-1,1,1,?)",
            (UNLIMITED_KEY, "Owner", _now()),
        )
        conn.execute(
            "UPDATE api_keys SET is_permanent=1,is_active=1,daily_limit=-1,total_limit=-1 "
            "WHERE key=?", (UNLIMITED_KEY,),
        )
        # Seed first account from env if DB empty
        if API_ID and API_HASH and SESSION_STRING:
            if conn.execute("SELECT COUNT(*) FROM accounts").fetchone()[0] == 0:
                conn.execute(
                    "INSERT INTO accounts (name,api_id,api_hash,session_string,is_active) "
                    "VALUES (?,?,?,?,1)",
                    ("Account 1", API_ID, API_HASH, SESSION_STRING),
                )
        conn.commit()
        log.info("DB ready at %s", DB_PATH)
    finally:
        conn.close()


# ===================================================================
# API KEY AUTH
# ===================================================================
def check_api_key(api_key: str) -> tuple[bool, str]:
    conn = _db()
    try:
        row = conn.execute("SELECT * FROM api_keys WHERE key=?", (api_key,)).fetchone()
        if not row:
            return False, "Invalid API key"
        if not row["is_active"]:
            return False, "API key is paused"
        # Expiry
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
        # Daily reset
        today = date.today().isoformat()
        if row["last_reset"] != today:
            conn.execute("UPDATE api_keys SET daily_used=0,last_reset=? WHERE key=?",
                         (today, api_key))
            conn.commit()
            row = conn.execute("SELECT * FROM api_keys WHERE key=?", (api_key,)).fetchone()
        # Limits
        if row["daily_limit"] != -1 and row["daily_used"] >= row["daily_limit"]:
            return False, "Daily limit reached"
        if row["total_limit"] != -1 and row["total_used"] >= row["total_limit"]:
            return False, "Total limit reached"
        # Atomic increment
        cur = conn.execute(
            "UPDATE api_keys SET daily_used=daily_used+1,total_used=total_used+1,last_used=? "
            "WHERE key=? "
            "AND (daily_limit=-1 OR daily_used<daily_limit) "
            "AND (total_limit=-1 OR total_used<total_limit)",
            (_now(), api_key),
        )
        conn.commit()
        if cur.rowcount == 0:
            return False, "Limit reached"
        return True, ""
    finally:
        conn.close()


# ===================================================================
# ACCOUNT POOL
# ===================================================================
active_clients: dict[int, Client] = {}
account_semaphores: dict[int, asyncio.Semaphore] = {}
_rr_index = 0
_rr_lock  = asyncio.Lock()


async def start_account(acc_id: int, name: str, api_id: int,
                        api_hash: str, session_string: str):
    if acc_id in active_clients:
        return
    try:
        client = Client(name, api_id=api_id, api_hash=api_hash,
                        session_string=session_string, in_memory=True)
        client.add_handler(MessageHandler(global_message_handler, filters.private))
        await client.start()
        active_clients[acc_id] = client
        account_semaphores[acc_id] = asyncio.Semaphore(MAX_CONCURRENT_PER_ACCOUNT)
        log.info("Account '%s' (id=%d) connected, slots=%d", name, acc_id, MAX_CONCURRENT_PER_ACCOUNT)
    except Exception as e:
        log.exception("Failed to start account '%s': %s", name, e)


async def pick_account() -> tuple[int, Client, asyncio.Semaphore]:
    async with _rr_lock:
        global _rr_index
        if not active_clients:
            raise RuntimeError("No active Telegram accounts")
        ids = list(active_clients.keys())
        acc_id = ids[_rr_index % len(ids)]
        _rr_index = (_rr_index + 1) % len(ids)
        return acc_id, active_clients[acc_id], account_semaphores[acc_id]


# ===================================================================
# SAFE SEND
# ===================================================================
async def safe_send(client: Client, chat: str, text: str):
    last_exc = None
    for attempt in range(SEND_RETRIES):
        try:
            return await client.send_message(chat, text)
        except FloodWait as e:
            wait = min(int(e.value) + 2, 30)
            log.warning("FloodWait %ss on %s", wait, chat)
            await asyncio.sleep(wait)
            last_exc = e
        except Exception as e:
            last_exc = e
            if attempt == SEND_RETRIES - 1:
                raise
            await asyncio.sleep(1.0)
    raise last_exc or RuntimeError("send failed")


# ===================================================================
# CORE FETCH  (the heart of the fix)
# ===================================================================
async def _fetch_impl(client: Client, bot: str, command: str) -> tuple[list[str], list[str]]:
    """
    Send command, handle all bot states, return (texts, file_texts).

    State machine:
      PROCESSING  → ignore message, keep waiting
      QUEUE WAIT  → ignore, extend timeout to BOT_QUEUE_TIMEOUT, keep waiting
      QUEUE TURN  → re-send original command, reset timers, keep waiting
      FINAL       → store + return immediately after tiny grace window
      NORMAL MSG  → store, start idle timer
      TIMEOUT     → return whatever collected (may be empty)
    """
    queue: asyncio.Queue = asyncio.Queue(maxsize=100)

    # Register BEFORE sending so we cannot miss a fast reply
    sent    = await safe_send(client, bot, command)
    sent_id = sent.id
    start   = time.monotonic()

    async with _queues_lock:
        pending_queues.setdefault(bot, {})[sent_id] = queue

    texts: dict[int, str] = {}
    files: dict[int, str] = {}
    got_real   = False    # received at least one non-processing, non-queue message
    in_queue   = False    # bot placed us in a position queue
    queue_done = False    # already re-sent after queue turn
    hard_limit = BOT_HARD_TIMEOUT

    log.info("→ [%s] %s  (id=%d)", bot, command[:50], sent_id)

    try:
        while True:
            elapsed   = time.monotonic() - start
            if elapsed > hard_limit:
                log.warning("Hard timeout (%.0fs) for [%s] %s", elapsed, bot, command[:30])
                break

            remaining  = hard_limit - elapsed
            # While waiting for first real message use BOT_FIRST_MSG_TIMEOUT;
            # once we have something, use BOT_IDLE_TIMEOUT
            poll_wait  = min(remaining,
                             BOT_IDLE_TIMEOUT if got_real and not in_queue
                             else BOT_FIRST_MSG_TIMEOUT)
            if poll_wait <= 0:
                break

            try:
                msg = await asyncio.wait_for(queue.get(), timeout=poll_wait)
            except asyncio.TimeoutError:
                if got_real and not in_queue:
                    break   # idle after real answer — done
                if elapsed >= (BOT_QUEUE_TIMEOUT if in_queue else BOT_FIRST_MSG_TIMEOUT):
                    break   # first-message or queue timeout
                continue

            # Skip stale messages that predate our command
            if msg.id <= sent_id and not getattr(msg, "edit_date", None):
                continue

            text = (msg.text or msg.caption or "").strip()

            # ── File attachment (Hathix bot sends .txt results) ──────────
            if (msg.document
                    and getattr(msg.document, "file_name", "").endswith(".txt")
                    and msg.id not in files):
                try:
                    media = await client.download_media(msg, in_memory=True)
                    if media:
                        raw = media.getvalue() if isinstance(media, io.BytesIO) else bytes(media)
                        files[msg.id] = raw.decode("utf-8", errors="ignore")
                        log.info("← [%s] file %d bytes", bot, len(files[msg.id]))
                        break   # file = complete answer
                except Exception as exc:
                    log.warning("File dl failed [%s]: %s", bot, exc)

            if not text:
                continue

            log.debug("← [%s] %r", bot, text[:100])

            # ── QUEUE TURN: re-send original command ─────────────────────
            if _is_queue_turn(text):
                if not queue_done:
                    queue_done = True
                    in_queue   = False
                    log.info("Queue turn — re-sending [%s]: %s", bot, command[:40])
                    try:
                        new_sent = await safe_send(client, bot, command)
                        async with _queues_lock:
                            qmap = pending_queues.setdefault(bot, {})
                            qmap[new_sent.id] = queue
                            if sent_id in qmap:
                                del qmap[sent_id]
                        sent_id    = new_sent.id
                        hard_limit = BOT_HARD_TIMEOUT
                        start      = time.monotonic()
                        got_real   = False
                        log.info("Re-sent OK, new_id=%d", sent_id)
                    except Exception as e:
                        log.error("Re-send failed: %s", e)
                        break
                continue   # don't store "your turn" message

            # ── QUEUE WAIT: bot queued us ─────────────────────────────────
            if _is_queue_wait(text):
                if not in_queue:
                    in_queue   = True
                    hard_limit = BOT_QUEUE_TIMEOUT
                    log.info("Queue detected [%s] — timeout extended to %ds", bot, hard_limit)
                continue   # don't store queue position messages

            # ── PROCESSING: bot is working ────────────────────────────────
            if _is_processing(text) and not got_real:
                log.debug("Processing msg from [%s] — waiting", bot)
                continue   # don't store processing messages

            # ── REAL MESSAGE ──────────────────────────────────────────────
            texts[msg.id] = text
            got_real      = True

            if _is_final(text):
                # Tiny grace window for a possible immediate second part
                try:
                    extra = await asyncio.wait_for(queue.get(), timeout=0.5)
                    et = (extra.text or extra.caption or "").strip()
                    if et and not _is_processing(et) and not _is_queue_wait(et):
                        texts[extra.id] = et
                except asyncio.TimeoutError:
                    pass
                log.info("Final answer [%s] in %.1fs", bot, time.monotonic() - start)
                break

    finally:
        async with _queues_lock:
            qmap = pending_queues.get(bot, {})
            # Remove every queue entry that points to our queue object
            for sid in [k for k, v in qmap.items() if v is queue]:
                del qmap[sid]
            if not qmap and bot in pending_queues:
                del pending_queues[bot]

    return [texts[k] for k in sorted(texts)], [files[k] for k in sorted(files)]


async def fetch_bot_response(client: Client, bot_username: str,
                             command: str) -> tuple[list[str], list[str]]:
    """Serialise requests per bot (most OSINT bots process one at a time)."""
    bot  = bot_username.lower()
    lock = bot_serial_lock.setdefault(bot, asyncio.Lock())
    async with lock:
        return await _fetch_impl(client, bot, command)


# ===================================================================
# PARSERS
# ===================================================================
def parse_hathix_txt(txt: str) -> list[dict]:
    results, cur = [], {}
    skip = ("raw","unique","hits","coverage","time","ref","target","info")
    kmap = {"name":"name","owner_name":"name","full_name":"name",
            "username":"username","upi_id":"upi_id","handle":"handle",
            "valid":"valid","account_type":"account_type","bank":"bank",
            "bank_id":"bank_id","app":"app","handle_bank":"handle_bank"}
    for raw in txt.split("\n"):
        line = raw.strip()
        if not line: continue
        if line.upper().startswith("RECORD #"):
            if cur: results.append(cur); cur = {}
            continue
        if ":" not in line or line.startswith("["): continue
        k, _, v = line.partition(":")
        kc = re.sub(r"[ /-]+", "_", k.strip().lower())
        if any(t in kc for t in skip): continue
        mk = kmap.get(kc)
        if not mk:
            if "full_name" in kc:   mk = "name"
            elif "father" in kc:    mk = "father_name"
            elif "address" in kc:   mk = "address"
            elif "circle" in kc:    mk = "circle"
            elif "linked" in kc:    mk = "linked_id"
            elif "alt" in kc:       mk = "alt_number"
            elif "email" in kc:     mk = "email"
            elif "mobile" in kc:    mk = "mobile_number"
            else:                   mk = kc
        cur[mk] = v.strip()
    if cur: results.append(cur)
    return results


def parse_text_records(raw_text: str) -> list[dict]:
    """Parse structured replies from sonambewafabot and similar bots."""
    FIELD_MAP = [
        ("👤 Name:", "name"), ("Name:", "name"),
        ("👨\u200d👦 Father's Name:", "father_name"),
        ("Father's Name:", "father_name"), ("Father:", "father_name"),
        ("📍 Circle:", "circle"), ("Circle:", "circle"),
        ("🆔 ID:", "id_number"), ("ID:", "id_number"),
        ("📱 Alternate Number:", "alternate_number"),
        ("Alternate Number:", "alternate_number"), ("Alternate:", "alternate_number"),
        ("Alt Number:", "alternate_number"),
        ("📧 Email:", "email"), ("Email:", "email"),
        ("🏠 Address:", "address"), ("Address:", "address"),
        ("• Tg Id:", "tg_id"), ("Tg Id:", "tg_id"),
        ("• Country:", "country"), ("Country:", "country"),
        ("• Country Code:", "country_code"), ("Country Code:", "country_code"),
        ("• Number:", "number"), ("Number:", "number"), ("📞 Number:", "number"),
        ("Mobile:", "mobile"), ("Phone:", "phone"),
        ("Operator:", "operator"), ("State:", "state"), ("City:", "city"),
        ("District:", "district"), ("Pincode:", "pincode"),
        ("DOB:", "dob"), ("Gender:", "gender"),
        ("Account Type:", "account_type"), ("Bank:", "bank"), ("IFSC:", "ifsc"),
        ("Pan:", "pan"), ("PAN:", "pan"), ("Aadhaar:", "aadhaar"),
        ("VehicleNo:", "vehicle_no"), ("Vehicle No:", "vehicle_no"),
        ("OwnerName:", "owner_name"), ("Owner Name:", "owner_name"),
        ("RegDate:", "reg_date"), ("Chassis:", "chassis"), ("Engine:", "engine"),
        ("FuelType:", "fuel_type"), ("InsuranceUpto:", "insurance_upto"),
        ("Manufacturer:", "manufacturer"), ("Model:", "model"), ("Color:", "color"),
    ]

    results = []
    segments = re.split(r"(?:📋\s*Record\s*\d+|─{5,}|━{5,})", raw_text)

    for seg in segments:
        seg = seg.strip()
        if not seg:
            continue
        # Skip segments that are purely bot-state messages
        if _is_processing(seg) or _is_queue_wait(seg) or _is_queue_turn(seg):
            continue

        data: dict = {}
        for line in seg.split("\n"):
            line = line.strip()
            if not line or re.match(r"^[━─=\-]{5,}$", line):
                continue
            matched = False
            for marker, key in FIELD_MAP:
                if line.startswith(marker):
                    val = line[len(marker):].strip()
                    if val and val not in ("N/A", "None", "", "-"):
                        data[key] = val
                    matched = True
                    break
            # Fallback: generic "Key: Value" parsing
            if not matched and ":" in line and not line.startswith(
                    ("📋","🔍","⏳","🎉","❌","✅","⚠️","🔄")):
                k, _, v = line.partition(":")
                k = re.sub(r"[^\w\s]", "", k).strip()
                v = v.strip()
                if k and v and v not in ("N/A","None","","-") and len(k) < 40:
                    data[k.lower().replace(" ", "_")] = v
        if data:
            results.append(data)
    return results


# ===================================================================
# ADMIN AUTH
# ===================================================================
ADMIN_SECRET = hashlib.sha256(f"{ADMIN_PASSWORD}RAJ".encode()).hexdigest()

def make_admin_token() -> str:
    p = f"admin:{int(time.time())}"
    s = hmac.new(ADMIN_SECRET.encode(), p.encode(), hashlib.sha256).hexdigest()[:16]
    return base64.urlsafe_b64encode(f"{p}:{s}".encode()).decode()

def verify_admin_token(t: str) -> bool:
    try:
        d  = base64.urlsafe_b64decode(t.encode()).decode()
        p, s = d.rsplit(":", 1)
        exp = hmac.new(ADMIN_SECRET.encode(), p.encode(), hashlib.sha256).hexdigest()[:16]
        if not hmac.compare_digest(s, exp): return False
        return (time.time() - int(p.split(":",1)[1])) <= 86400
    except Exception:
        return False

def require_admin(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing admin token")
    if not verify_admin_token(authorization.split(" ", 1)[1]):
        raise HTTPException(401, "Invalid or expired token")
    return True


# ===================================================================
# APP
# ===================================================================
class _Done(Exception): pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    conn = _db()
    try:
        accs = conn.execute("SELECT * FROM accounts WHERE is_active=1").fetchall()
    finally:
        conn.close()
    tasks = [asyncio.create_task(start_account(
        a["id"], a["name"], a["api_id"], a["api_hash"], a["session_string"]
    )) for a in accs]
    log.info("Starting %d account(s)…", len(tasks))
    yield
    log.info("Shutting down…")
    for t in tasks: t.cancel()
    for c in list(active_clients.values()):
        try: await c.stop()
        except Exception: pass

app = FastAPI(title="RAJ OSINT API", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])


# ===================================================================
# SEARCH ENDPOINT
# ===================================================================
@app.get("/{search_type}/{query}")
async def search_get(search_type: str, query: str, api_key: str = None):
    t0     = time.perf_counter()
    is_tg  = (search_type == "tg")
    empty  = {} if is_tg else []
    final  = {"success": True, "query": query, "type": search_type, "total_results": 0}
    status = 200
    sem    = None

    if not api_key:
        final.update(success=False, message="API key required. Add ?api_key=YOUR_KEY", data=empty)
        status = 401
    elif search_type not in COMMANDS_MAP:
        final.update(success=False,
                     message=f"Unknown type '{search_type}'. Valid: {', '.join(COMMANDS_MAP)}",
                     data=empty)
        status = 400
    else:
        ok, err = check_api_key(api_key)
        if not ok:
            final.update(success=False, message=err, data=empty); status = 403
        else:
            try:
                _, client, account_sem = await pick_account()
                try:
                    await asyncio.wait_for(account_sem.acquire(), timeout=15)
                    sem = account_sem
                except asyncio.TimeoutError:
                    final.update(success=False,
                                 message="Server busy — too many requests. Try again shortly.",
                                 data=empty)
                    status = 503
                    raise _Done()

                bot_username, cmd_prefix = COMMANDS_MAP[search_type]

                # ── TG lookup ───────────────────────────────────────────────
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
                            if isinstance(u, list): u = u[0]
                            if u is None: raise UsernameNotOccupied(target)
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
                            status = 404
                        except asyncio.TimeoutError:
                            final.update(success=False,
                                         message="Telegram resolve timed out. Try again.",
                                         data={}, total_results=0); status = 504
                        except Exception as e:
                            final.update(success=False,
                                         message=f"Telegram resolve failed: {e}",
                                         data={}, total_results=0); status = 502

                    if command:
                        texts, files = await fetch_bot_response(client, bot_username, command)
                        raw = "\n".join(texts)
                        if not texts and not files:
                            final.update(success=False,
                                         message="No response from OSINT bot (timeout or rate-limit).",
                                         data={}, total_results=0); status = 504
                        else:
                            no_info = _extract_no_info(raw)
                            if no_info:
                                final.update(success=False, message=no_info,
                                             data={}, total_results=0)
                            else:
                                recs = parse_text_records(raw)
                                if recs:
                                    recs[0].pop("tg_id", None)
                                    tg_data.update(recs[0])
                                final.update(data=tg_data, total_results=1)

                # ── All other lookups ────────────────────────────────────────
                else:
                    command = f"{cmd_prefix} {query}"
                    texts, files = await fetch_bot_response(client, bot_username, command)
                    raw = "\n".join(texts) + "\n" + "\n".join(files)

                    if not texts and not files:
                        final.update(success=False,
                                     message="No response from OSINT bot (timeout or rate-limit).",
                                     data=[], total_results=0); status = 504
                    else:
                        no_info = _extract_no_info(raw)
                        if no_info:
                            final.update(success=False, message=no_info,
                                         data=[], total_results=0)
                        elif files:
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
                                    for r in parsed: r["query"] = query
                                final.update(data=parsed, total_results=len(parsed))
                            else:
                                final.update(success=False,
                                             message="No parseable records found",
                                             raw_reply=raw[:600],
                                             data=[], total_results=0)

            except _Done:
                pass
            except Exception as e:
                log.exception("Search error %s/%s", search_type, query)
                final.update(success=False,
                             message=f"Internal error: {type(e).__name__}: {e}",
                             data=empty)
                status = 500
            finally:
                if sem is not None:
                    try: sem.release()
                    except Exception: pass

    final["developer"]  = DEVELOPER_TAG
    final["bot"]        = BOT_TAG
    final["time_taken"] = f"{time.perf_counter() - t0:.2f}s"

    return Response(
        content=json.dumps(final, indent=2, ensure_ascii=False),
        media_type="application/json; charset=utf-8",
        status_code=status,
    )


# ===================================================================
# HEALTH
# ===================================================================
@app.get("/healthz")
async def healthz():
    return {
        "status": "ok", "time": _now(),
        "accounts_connected": len(active_clients),
        "max_concurrent_per_account": MAX_CONCURRENT_PER_ACCOUNT,
        "active_queues": {b: len(q) for b, q in pending_queues.items()},
    }


# ===================================================================
# ADMIN HTML
# ===================================================================
ADMIN_HTML = r"""<!doctype html>
<html><head><meta charset="utf-8"><title>RAJ Admin</title>
<style>
*{box-sizing:border-box}
body{font-family:system-ui,sans-serif;background:#0b0b0d;color:#eee;margin:0;padding:24px}
h1,h2{font-weight:600;margin:0 0 12px}
input,button{padding:8px 12px;border-radius:6px;border:1px solid #333;background:#16161a;
  color:#eee;margin:4px;font-size:13px}
button{cursor:pointer;background:#26262e}button:hover{background:#333}
table{width:100%;border-collapse:collapse;margin-top:12px;font-size:13px}
th,td{padding:8px 10px;border-bottom:1px solid #1e1e24;text-align:left}
th{background:#16161a;font-weight:600}
.card{background:#16161a;padding:20px;border-radius:12px;margin-bottom:16px;border:1px solid #1e1e24}
.on{display:inline-block;padding:2px 8px;border-radius:4px;font-size:11px;background:#1a3a1a;color:#4ade80}
.off{display:inline-block;padding:2px 8px;border-radius:4px;font-size:11px;background:#3a1a1a;color:#f87171}
pre{background:#0f0f12;padding:12px;border-radius:8px;overflow:auto;font-size:12px;border:1px solid #1e1e24}
</style></head><body>
<h1>RAJ Engine — Admin</h1>
<div class="card" id="login">
  <h2>Login</h2><input id="pw" type="password" placeholder="Password"/>
  <button onclick="login()">Login</button>
  <small id="lerr" style="color:#f87171;margin-left:8px"></small>
</div>
<div id="panel" style="display:none">
  <div class="card"><h2>Stats <button onclick="loadStats()" style="font-size:11px;padding:4px 8px">Refresh</button></h2>
    <pre id="stats">Loading…</pre></div>
  <div class="card"><h2>Create Key</h2>
    <input id="kowner" placeholder="Owner"/><input id="kkey" placeholder="Custom key (blank=auto)"/>
    <input id="kdays" type="number" placeholder="Expires days (0=never)" value="0"/>
    <input id="klimit" type="number" placeholder="Daily limit (-1=∞)" value="-1"/>
    <button onclick="createKey()">Create</button>
    <pre id="kresult" style="display:none"></pre></div>
  <div class="card"><h2>Keys <button onclick="loadKeys()" style="font-size:11px;padding:4px 8px">Refresh</button></h2>
    <table id="ktable"><thead><tr><th>Key</th><th>Owner</th><th>Daily</th><th>Total</th>
      <th>Expires</th><th>Status</th><th>Actions</th></tr></thead><tbody></tbody></table></div>
  <div class="card"><h2>Accounts <button onclick="loadAccounts()" style="font-size:11px;padding:4px 8px">Refresh</button></h2>
    <table id="atable"><thead><tr><th>ID</th><th>Name</th><th>Connected</th><th>Actions</th></tr></thead>
    <tbody></tbody></table></div>
</div>
<script>
let TOKEN=localStorage.getItem("raj_tok")||"";
const H=()=>({"Authorization":"Bearer "+TOKEN,"Content-Type":"application/json"});
async function login(){
  const r=await fetch("/admin/login",{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify({password:document.getElementById("pw").value})});
  if(!r.ok){document.getElementById("lerr").textContent="Wrong password";return}
  TOKEN=(await r.json()).token;localStorage.setItem("raj_tok",TOKEN);show();
}
async function loadStats(){
  const r=await fetch("/admin/api/stats",{headers:H()});
  document.getElementById("stats").textContent=JSON.stringify(await r.json(),null,2);
}
async function loadKeys(){
  const r=await fetch("/admin/api/keys",{headers:H()});const j=await r.json();
  const tb=document.querySelector("#ktable tbody");tb.innerHTML="";
  for(const k of j.keys){
    const tr=document.createElement("tr");
    const dl=k.daily_limit==-1?"∞":k.daily_limit;const tl=k.total_limit==-1?"∞":k.total_limit;
    const exp=k.expires_at?k.expires_at.slice(0,10):"Never";
    const badge=k.is_active?'<span class="on">Active</span>':'<span class="off">Paused</span>';
    tr.innerHTML=`<td><code>${k.key}</code></td><td>${k.owner||""}</td>
      <td>${k.daily_used}/${dl}</td><td>${k.total_used}/${tl}</td><td>${exp}</td><td>${badge}</td>`;
    const td=document.createElement("td");
    const b1=document.createElement("button");b1.textContent=k.is_active?"Pause":"Resume";
    b1.onclick=async()=>{await fetch("/admin/api/keys/"+k.key+"/toggle",{method:"POST",headers:H()});loadKeys()};
    const b2=document.createElement("button");b2.textContent="Delete";b2.style.color="#f87171";
    b2.onclick=async()=>{if(!confirm("Delete "+k.key+"?"))return;
      await fetch("/admin/api/keys/"+k.key,{method:"DELETE",headers:H()});loadKeys()};
    td.append(b1," ",b2);tr.append(td);tb.append(tr);
  }
}
async function createKey(){
  const body={owner:document.getElementById("kowner").value,
    custom_key:document.getElementById("kkey").value,
    expires_days:+document.getElementById("kdays").value,
    daily_limit:+document.getElementById("klimit").value};
  const r=await fetch("/admin/api/keys",{method:"POST",headers:H(),body:JSON.stringify(body)});
  const pre=document.getElementById("kresult");pre.style.display="block";
  pre.textContent=JSON.stringify(await r.json(),null,2);loadKeys();
}
async function loadAccounts(){
  const r=await fetch("/admin/api/accounts",{headers:H()});const j=await r.json();
  const tb=document.querySelector("#atable tbody");tb.innerHTML="";
  for(const a of j.accounts){
    const tr=document.createElement("tr");
    const badge=a.connected?'<span class="on">Connected</span>':'<span class="off">Offline</span>';
    tr.innerHTML=`<td>${a.id}</td><td>${a.name}</td><td>${badge}</td>`;
    const td=document.createElement("td");
    const b=document.createElement("button");b.textContent="Remove";b.style.color="#f87171";
    b.onclick=async()=>{if(!confirm("Remove "+a.name+"?"))return;
      await fetch("/admin/api/accounts/"+a.id,{method:"DELETE",headers:H()});loadAccounts()};
    td.append(b);tr.append(td);tb.append(tr);
  }
}
function show(){
  document.getElementById("login").style.display="none";
  document.getElementById("panel").style.display="block";
  loadStats();loadKeys();loadAccounts();setInterval(loadStats,30000);
}
if(TOKEN)show();
</script></body></html>"""

@app.get("/admin", response_class=HTMLResponse)
async def admin_page(): return HTMLResponse(ADMIN_HTML)


# ===================================================================
# ADMIN API
# ===================================================================
class LoginReq(BaseModel): password: str
class KeyReq(BaseModel):
    owner: str; custom_key: str = ""; expires_days: int = 0; daily_limit: int = -1
class AccReq(BaseModel):
    name: str; api_id: int; api_hash: str; session_string: str

@app.post("/admin/login")
async def admin_login(req: LoginReq):
    if not hmac.compare_digest(req.password, ADMIN_PASSWORD):
        raise HTTPException(401, "Wrong password")
    return {"token": make_admin_token()}

@app.get("/admin/api/stats")
async def admin_stats(_=Depends(require_admin)):
    conn = _db()
    try:
        tk = conn.execute("SELECT COUNT(*) c FROM api_keys").fetchone()["c"]
        tr = conn.execute("SELECT COALESCE(SUM(total_used),0) s FROM api_keys").fetchone()["s"]
        today = date.today().isoformat()
        dr = conn.execute(
            "SELECT COALESCE(SUM(CASE WHEN last_reset=? THEN daily_used ELSE 0 END),0) s "
            "FROM api_keys", (today,)).fetchone()["s"]
    finally:
        conn.close()
    return {"total_keys": tk, "total_requests": int(tr), "daily_requests": int(dr),
            "accounts_connected": len(active_clients),
            "max_concurrent": MAX_CONCURRENT_PER_ACCOUNT,
            "active_queues": {b: len(q) for b, q in pending_queues.items()}}

@app.get("/admin/api/keys")
async def admin_list_keys(_=Depends(require_admin)):
    conn = _db()
    try:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM api_keys ORDER BY created_at DESC").fetchall()]
    finally:
        conn.close()
    return {"keys": rows}

@app.post("/admin/api/keys")
async def admin_create_key(req: KeyReq, _=Depends(require_admin)):
    key = req.custom_key.strip() or ("RAJ" + secrets.token_hex(8).upper())
    exp = ((datetime.now(timezone.utc) + timedelta(days=req.expires_days)).isoformat()
           if req.expires_days > 0 else None)
    conn = _db()
    try:
        conn.execute(
            "INSERT INTO api_keys "
            "(key,owner,expires_at,daily_limit,total_limit,is_active,is_permanent,created_at) "
            "VALUES (?,?,?,?,-1,1,0,?)",
            (key, req.owner, exp, req.daily_limit, _now()))
        conn.commit()
        log.info("Key created: %s owner=%s daily=%d", key, req.owner, req.daily_limit)
        return {"key": key, "owner": req.owner, "daily_limit": req.daily_limit, "expires": exp}
    except sqlite3.IntegrityError:
        raise HTTPException(400, "API key already exists")
    finally:
        conn.close()

@app.delete("/admin/api/keys/{key}")
async def admin_delete_key(key: str, _=Depends(require_admin)):
    if key == UNLIMITED_KEY:
        raise HTTPException(400, "Cannot delete master key")
    conn = _db()
    try:
        conn.execute("DELETE FROM api_keys WHERE key=?", (key,)); conn.commit()
    finally:
        conn.close()
    return {"ok": True}

@app.post("/admin/api/keys/{key}/toggle")
async def admin_toggle_key(key: str, _=Depends(require_admin)):
    if key == UNLIMITED_KEY:
        raise HTTPException(400, "Cannot pause master key")
    conn = _db()
    try:
        conn.execute("UPDATE api_keys SET is_active=1-is_active WHERE key=?", (key,))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}

@app.get("/admin/api/accounts")
async def admin_list_accounts(_=Depends(require_admin)):
    conn = _db()
    try:
        rows = [dict(r) for r in conn.execute(
            "SELECT id,name,api_id,is_active FROM accounts").fetchall()]
    finally:
        conn.close()
    for r in rows: r["connected"] = r["id"] in active_clients
    return {"accounts": rows}

@app.post("/admin/api/accounts")
async def admin_add_account(req: AccReq, _=Depends(require_admin)):
    conn = _db()
    try:
        cur = conn.execute(
            "INSERT INTO accounts (name,api_id,api_hash,session_string,is_active) VALUES (?,?,?,?,1)",
            (req.name, req.api_id, req.api_hash, req.session_string))
        acc_id = cur.lastrowid; conn.commit()
    finally:
        conn.close()
    asyncio.create_task(start_account(acc_id, req.name, req.api_id,
                                      req.api_hash, req.session_string))
    return {"ok": True, "id": acc_id}

@app.delete("/admin/api/accounts/{acc_id}")
async def admin_del_account(acc_id: int, _=Depends(require_admin)):
    client = active_clients.pop(acc_id, None)
    account_semaphores.pop(acc_id, None)
    if client:
        try: await client.stop()
        except Exception: pass
    conn = _db()
    try:
        conn.execute("DELETE FROM accounts WHERE id=?", (acc_id,)); conn.commit()
    finally:
        conn.close()
    return {"ok": True}


# ===================================================================
# ENTRY POINT
# ===================================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("userbot_api:app", host="0.0.0.0",
                port=int(os.getenv("PORT", "8000")),
                log_level=os.getenv("LOG_LEVEL", "info"),
                limit_concurrency=500, timeout_keep_alive=30)
