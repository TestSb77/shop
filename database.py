from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection
from datetime import datetime
import threading
import time
import logging
import os

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DATABASE CONFIG  (edit only this block)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

MONGO_URI = "mongodb+srv://LegendRukia:IkRukia@cluster0.b6vl5kc.mongodb.net/?appName=Cluster0"
# 🔴 Replace above with your actual MongoDB Atlas URI
# Format: mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/<dbname>

DB_NAME = "telegram_bot"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONNECTION (singleton MongoClient)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_client: MongoClient = None
_db = None
_lock = threading.Lock()


def _get_db():
    global _client, _db
    if _db is not None:
        return _db
    with _lock:
        if _db is None:
            _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
            _db = _client[DB_NAME]
            logging.info("[DB] MongoDB connected.")
    return _db


def get_collection(name: str) -> Collection:
    return _get_db()[name]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# BACKWARD COMPAT: get_db_connection() shim
# Many files call get_db_connection() — this returns a
# lightweight proxy so we don't have to change those files.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class _MongoConnProxy:
    """Fake 'connection' object for backward compatibility."""
    def cursor(self, **kwargs):
        return _MongoCursorProxy()
    def commit(self):
        pass
    def rollback(self):
        pass
    def close(self):
        pass
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass


class _MongoCursorProxy:
    """Fake cursor — not used in new code but prevents crashes on import."""
    def execute(self, *a, **kw): pass
    def fetchone(self): return None
    def fetchall(self): return []
    def close(self): pass


def get_db_connection():
    """Backward-compat shim. New code should use get_collection() directly."""
    return _MongoConnProxy()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# IN-PROCESS TTL CACHES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_gate_cache: dict = {}
_gate_cache_ttl = 30

_credits_cache: dict = {}
_credits_cache_ttl = 5

_premium_cache: dict = {}
_premium_cache_ttl = 60

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SCHEMA INITIALIZATION (indexes)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def ensure_users_table():
    try:
        col = get_collection("users")
        col.create_index([("user_id", ASCENDING)], unique=True)
        print("[DB] Users collection ready.")
    except Exception as e:
        print(f"[DB] Error ensuring users collection: {e}")


def ensure_stats_columns():
    # MongoDB is schema-less — nothing to alter
    print("[DB] Stats fields auto-managed by MongoDB.")


def ensure_proxy_table():
    try:
        col = get_collection("proxies")
        col.create_index([("user_id", ASCENDING)])
        print("[DB] Proxies collection ready.")
    except Exception as e:
        print(f"[DB] Error creating proxies collection: {e}")


def ensure_gate_table():
    try:
        col = get_collection("gate_status")
        col.create_index([("gate", ASCENDING)], unique=True)
        print("[DB] Gate status collection ready.")
    except Exception as e:
        print(f"[DB] Error creating gate collection: {e}")


def ensure_banned_table():
    try:
        col = get_collection("banned_users")
        col.create_index([("user_id", ASCENDING)], unique=True)
        print("[DB] Banned users collection ready.")
    except Exception as e:
        print(f"[DB] Error creating banned_users collection: {e}")


def ensure_receipts_table():
    try:
        col = get_collection("receipts")
        col.create_index([("receipt_id", ASCENDING)], unique=True)
        col.create_index([("user_id", ASCENDING)])
        print("[DB] Receipts collection ready.")
    except Exception as e:
        print(f"[DB] Error creating receipts collection: {e}")


def ensure_codes_table():
    try:
        col = get_collection("codes")
        col.create_index([("code", ASCENDING)], unique=True)
        print("[DB] Codes collection ready.")
    except Exception as e:
        print(f"[DB] Error creating codes collection: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# USER FUNCTIONS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_user(user_id: int):
    col = get_collection("users")
    return col.find_one({"user_id": user_id})


def create_user(user_id: int, username: str):
    col = get_collection("users")
    col.update_one(
        {"user_id": user_id},
        {"$setOnInsert": {
            "user_id": user_id,
            "username": username,
            "first_name": "",
            "credits": 150,
            "cc_checked": 0,
            "cc_charged": 0,
            "is_premium": 0,
            "premium_expiry": None,
            "unlimited_msh": 0,
            "joined_at": datetime.now()
        }},
        upsert=True
    )


def is_unlimited_msh(user_id: int) -> bool:
    """Returns True if admin has marked this user as unlimited for /msh checks."""
    col = get_collection("users")
    doc = col.find_one({"user_id": user_id}, {"unlimited_msh": 1})
    return bool(doc.get("unlimited_msh", 0)) if doc else False


def set_unlimited_msh(user_id: int, value: bool) -> bool:
    """
    Toggle/set the unlimited_msh flag for a user.
    Returns the NEW value (True = unlimited, False = normal).
    Also ensures the user doc exists so this works for users who
    haven't /started the bot yet.
    """
    col = get_collection("users")
    col.update_one(
        {"user_id": user_id},
        {"$set": {"unlimited_msh": 1 if value else 0}},
        upsert=True,
    )
    return value


def get_user_credits(user_id: int) -> int:
    now = time.monotonic()
    cached = _credits_cache.get(user_id)
    if cached and now < cached[1]:
        return cached[0]
    col = get_collection("users")
    doc = col.find_one({"user_id": user_id}, {"credits": 1})
    credits = doc["credits"] if doc and "credits" in doc else 0
    _credits_cache[user_id] = (credits, now + _credits_cache_ttl)
    return credits


def update_credits(user_id: int, new_credits: int):
    col = get_collection("users")
    col.update_one({"user_id": user_id}, {"$set": {"credits": new_credits}})
    _credits_cache[user_id] = (new_credits, time.monotonic() + _credits_cache_ttl)


def deduct_credits_atomic(user_id: int, amount: int) -> int:
    """
    Atomically deducts `amount` credits (floor 0). Returns new balance.
    Uses find_one_and_update for atomicity.
    """
    col = get_collection("users")
    # First get current credits
    doc = col.find_one({"user_id": user_id}, {"credits": 1})
    current = doc["credits"] if doc and "credits" in doc else 0
    new_val = max(current - amount, 0)
    col.update_one({"user_id": user_id}, {"$set": {"credits": new_val}})
    _credits_cache[user_id] = (new_val, time.monotonic() + _credits_cache_ttl)
    return new_val


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STATS FUNCTIONS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def update_user_stats(user_id: int, is_charged: bool):
    col = get_collection("users")
    inc = {"cc_checked": 1}
    if is_charged:
        inc["cc_charged"] = 1
    col.update_one({"user_id": user_id}, {"$inc": inc})


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GATE FUNCTIONS  — cached
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def is_gate_enabled(gate: str) -> bool:
    now = time.monotonic()
    cached = _gate_cache.get(gate)
    if cached and now < cached[1]:
        return cached[0]

    col = get_collection("gate_status")
    col.update_one(
        {"gate": gate},
        {"$setOnInsert": {"gate": gate, "is_enabled": True, "updated_at": datetime.now()}},
        upsert=True
    )
    doc = col.find_one({"gate": gate})
    status = bool(doc.get("is_enabled", True)) if doc else True

    _gate_cache[gate] = (status, now + _gate_cache_ttl)
    return status


def set_gate_status(gate: str, enabled: bool):
    col = get_collection("gate_status")
    col.update_one(
        {"gate": gate},
        {"$set": {"is_enabled": enabled, "updated_at": datetime.now()}},
        upsert=True
    )
    _gate_cache[gate] = (enabled, time.monotonic() + _gate_cache_ttl)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DB_CONFIG shim (some files import DB_CONFIG directly)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

DB_CONFIG = {"mongo_uri": MONGO_URI}   # kept for import compat

# PooledConn shim for sub.py backward compat
class PooledConn:
    def __enter__(self):
        return _MongoConnProxy()
    def __exit__(self, *args):
        pass

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# RUN INITIALIZATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ensure_users_table()
ensure_stats_columns()
ensure_proxy_table()
ensure_gate_table()
ensure_banned_table()
ensure_receipts_table()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GLOBAL PROXY POOL  (shared across all users)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

GLOBAL_PROXIES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "proxies.txt")


def load_global_proxies() -> list:
    try:
        if not os.path.exists(GLOBAL_PROXIES_FILE):
            return []
        with open(GLOBAL_PROXIES_FILE, "r", encoding="utf-8", errors="ignore") as f:
            return [line.strip() for line in f if line.strip()]
    except Exception as e:
        logging.error(f"Error loading global proxies: {e}")
        return []


def to_http_proxy_url(raw: str) -> str:
    parts = raw.split(":")
    if len(parts) != 4:
        return raw
    host, port, user, password = parts
    return f"http://{user}:{password}@{host}:{port}"


def load_global_proxies_http() -> list:
    return [to_http_proxy_url(p) for p in load_global_proxies()]


if __name__ == "__main__":
    pass
ensure_codes_table()
