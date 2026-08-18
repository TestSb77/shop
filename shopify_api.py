"""
Shopify card-check API helper with automatic fallback.

Each entry in SHOPIFY_API_URLS is a tuple:
    (api_url, param_renames)

Where param_renames is either:
    None — use default param names: card, url, proxy
    dict — map default names → API-specific names, e.g. {"card": "cc", "url": "site"}

The helper tries URLs in order. On 5xx, 429, timeout, network error, OR
unexpected response format, it falls back to the next URL.

The returned response is NORMALISED — capital keys (Gateway, Price,
Response, Status, etc.) are mapped to lowercase for consistent parsing
across both APIs.

PERFORMANCE NOTES (multi-user load):
  * Uses ONE process-wide aiohttp.ClientSession with a large connector
    pool + keep-alive, instead of creating a new session per card.
    This eliminates per-card TCP connect + TLS handshake + DNS lookup
    overhead — a 3–5× speedup under concurrent load (13–15 users).
  * Tracks a rolling-window health log and trips a circuit breaker
    when the API is failing too much, converting thundering-herd
    retry storms into a brief global pause that lets the backend
    recover.
  * Uses jittered exponential backoff between retries so concurrent
    workers don't all wake at the same instant after a failure.
"""
import asyncio
import json
import logging
import random
import socket
import time
import functools
from collections import deque
import aiohttp

try:
    import dns.resolver as _dns_resolver_mod
    import dns.exception as _dns_exception_mod
    _HAVE_DNSPYTHON = True
except ImportError:
    _HAVE_DNSPYTHON = False


# === CONFIG: API endpoints, tried in order (primary first) ===
# Primary: old production API (detailed reasons: INSUFFICIENT_FUNDS,
#          INCORRECT_CVC, EXPIRED_CARD, etc.)
# Backup:  new autosh API (uses cc + site params, capital JSON keys)
SHOPIFY_API_URLS = [
    (
        "https://shopify-api-production-00.up.railway.app/check",
        None,
    ),
    (
        "https://autosh.up.railway.app/shopii",
        {"card": "cc", "url": "site"},
    ),
]         

# HTTP statuses that count as "API error" → trigger fallback to next URL
_FALLBACK_HTTP = {408, 429, 500, 502, 503, 504}

# Retry the whole fallback chain up to this many times before giving up.
# Transient timeouts/network blips usually clear up in 2–3 retries.
MAX_RETRIES = 3

# Exponential backoff between retries: 0.6s, 1.2s, 2.4s (capped at 5s below)
_RETRY_BACKOFF_BASE = 0.6


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SHARED HTTP SESSION — single aiohttp session for the whole process.
#
# Previously every call_shopify_api() call created a fresh
# aiohttp.ClientSession (TCP connect + TLS handshake + DNS lookup per card).
# With 50+ concurrent workers this was the #1 source of latency and
# wasted backend capacity (each session holds its own connection pool
# which is torn down immediately after one request).
#
# Now: one process-wide session with a generous connector pool, keep-alive,
# and DNS caching. Connectors are reused across every /msh call, every
# gate call, every site-check call.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Connector sizes tuned for the load profile (13–15 simultaneous /msh users,
# ~50 concurrent API calls, plus gate traffic).
_SESSION_TOTAL_LIMIT = 300      # total open connections across all hosts
_SESSION_PER_HOST_LIMIT = 120   # per-host (we only talk to ~2 hosts)
_SESSION_KEEPALIVE_S = 60       # keep idle connections open this long
_SESSION_DNS_TTL_S = 300        # cache DNS results 5 minutes


def _build_shared_session() -> aiohttp.ClientSession:
    """Build the process-wide aiohttp session with a tuned connector."""
    resolver = _get_resolver()
    connector = aiohttp.TCPConnector(
        limit=_SESSION_TOTAL_LIMIT,
        limit_per_host=_SESSION_PER_HOST_LIMIT,
        ttl_dns_cache=_SESSION_DNS_TTL_S,
        keepalive_timeout=_SESSION_KEEPALIVE_S,
        enable_cleanup_closed=True,
        ssl=False,
        resolver=resolver,
    )
    return aiohttp.ClientSession(
        connector=connector,
        timeout=aiohttp.ClientTimeout(total=80),
    )


_shared_session: aiohttp.ClientSession | None = None
_shared_session_lock = asyncio.Lock()


async def _get_shared_session() -> aiohttp.ClientSession:
    """
    Lazy-init / return the process-wide aiohttp session.
    Re-created automatically if it was closed (e.g. after a transport error).
    """
    global _shared_session
    if _shared_session is not None and not _shared_session.closed:
        return _shared_session
    async with _shared_session_lock:
        if _shared_session is None or _shared_session.closed:
            _shared_session = _build_shared_session()
            logging.info("[shopify_api] shared HTTP session initialised "
                         f"(limit={_SESSION_TOTAL_LIMIT}, "
                         f"per_host={_SESSION_PER_HOST_LIMIT}, "
                         f"keepalive={_SESSION_KEEPALIVE_S}s)")
        return _shared_session


async def close_shared_session() -> None:
    """Close the shared session (call on bot shutdown)."""
    global _shared_session
    if _shared_session is not None and not _shared_session.closed:
        try:
            await _shared_session.close()
        except Exception as e:
            logging.debug(f"[shopify_api] error closing shared session: {e}")
    _shared_session = None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CIRCUIT BREAKER — global adaptive throttle.
#
# When the backend is failing or stalling, naive retries from every
# worker at once create a "thundering herd" that makes recovery
# impossible (workers retry → backend stays overloaded → workers retry).
#
# This module trips a circuit breaker when the rolling-window failure
# rate or latency exceeds a threshold, and pauses ALL new API calls for
# a short window so the backend can drain its backlog. After the pause,
# a "half-open" test call decides whether to close the breaker (resume
# normal traffic) or reopen it (try again later).
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Rolling window for breaker decisions (seconds). Long enough to be
# statistically meaningful, short enough to react quickly to recovery.
_BREAKER_WINDOW_SEC = 30

# Open the breaker when EITHER:
#   - failure rate over the window exceeds this (0.0–1.0)
#   - average latency over the window exceeds this (milliseconds)
_BREAKER_FAIL_RATE = 0.55
_BREAKER_AVG_LATENCY_MS = 18_000

# Minimum number of samples before we trust the rolling stats.
# Below this we never trip — we don't want a noisy 5-call startup to
# open the breaker.
_BREAKER_MIN_SAMPLES = 15

# How long the breaker stays open (seconds) before half-open test.
_BREAKER_OPEN_DURATION_S = 6.0
# If the half-open test fails, open for this long instead (slower retry).
_BREAKER_REOPEN_DURATION_S = 12.0

# Breaker state: 'CLOSED' (normal), 'OPEN' (paused), 'HALF_OPEN' (testing).
_breaker_state: str = "CLOSED"
_breaker_open_until: float = 0.0  # monotonic timestamp

# Rolling log of recent API outcomes: (monotonic_ts, success_bool, elapsed_ms)
_breaker_outcomes: deque = deque(maxlen=400)


def _record_breaker_outcome(success: bool, elapsed_ms: float) -> None:
    """Record one API outcome for the breaker."""
    _breaker_outcomes.append((time.monotonic(), bool(success), float(elapsed_ms)))


def _breaker_stats() -> tuple[int, float, float]:
    """Return (sample_count, failure_rate, avg_ms) over the rolling window."""
    now = time.monotonic()
    cutoff = now - _BREAKER_WINDOW_SEC
    while _breaker_outcomes and _breaker_outcomes[0][0] < cutoff:
        _breaker_outcomes.popleft()
    n = len(_breaker_outcomes)
    if n == 0:
        return 0, 0.0, 0.0
    fail = sum(1 for _, ok, _ in _breaker_outcomes if not ok)
    avg = sum(ms for _, _, ms in _breaker_outcomes) / n
    return n, fail / n, avg


async def _breaker_gate() -> None:
    """
    Called before each API attempt. Blocks if the breaker is OPEN.
    Transitions OPEN → HALF_OPEN when the open window elapses, and
    HALF_OPEN → CLOSED/OPEN based on the next test call (handled in
    the caller by observing the result of the attempt that follows).
    """
    global _breaker_state, _breaker_open_until
    if _breaker_state == "OPEN":
        remaining = _breaker_open_until - time.monotonic()
        if remaining > 0:
            # Tiny jitter so paused workers don't all wake at the same instant
            await asyncio.sleep(remaining + random.uniform(0, 0.3))
        _breaker_state = "HALF_OPEN"
        logging.info("[shopify_api] circuit breaker → HALF_OPEN (resuming)")
        # Fall through; the next attempt's outcome will decide


def _breaker_after_attempt(success: bool, elapsed_ms: float) -> None:
    """Update breaker state after an API attempt outcome is known."""
    global _breaker_state, _breaker_open_until
    _record_breaker_outcome(success, elapsed_ms)

    if _breaker_state == "HALF_OPEN":
        if success:
            _breaker_state = "CLOSED"
            _breaker_open_until = 0.0
            logging.info("[shopify_api] circuit breaker → CLOSED (recovered)")
        else:
            _breaker_state = "OPEN"
            _breaker_open_until = time.monotonic() + _BREAKER_REOPEN_DURATION_S
            logging.warning(
                f"[shopify_api] circuit breaker → OPEN (half-open failed; "
                f"pausing {_BREAKER_REOPEN_DURATION_S:.0f}s)"
            )
        return

    # CLOSED → may transition to OPEN if stats look bad
    n, fail_rate, avg_ms = _breaker_stats()
    if n >= _BREAKER_MIN_SAMPLES:
        if fail_rate > _BREAKER_FAIL_RATE or avg_ms > _BREAKER_AVG_LATENCY_MS:
            _breaker_state = "OPEN"
            _breaker_open_until = time.monotonic() + _BREAKER_OPEN_DURATION_S
            logging.warning(
                f"[shopify_api] circuit breaker → OPEN "
                f"(n={n} fail_rate={fail_rate:.0%} avg_ms={avg_ms:.0f} "
                f"→ pausing {_BREAKER_OPEN_DURATION_S:.0f}s)"
            )


def _jittered_backoff(base: float, attempt: int) -> float:
    """
    Exponential backoff with jitter:  base * 2^(attempt-1) * U(0.5, 1.5)
    Jitter prevents N workers that all failed at the same instant from
    waking at the same instant and re-hammering the backend.
    """
    expo = base * (2 ** (attempt - 1))
    return min(5.0, expo * random.uniform(0.5, 1.5))


class APIStatusError(Exception):
    """Raised when API returns an error HTTP status that should trigger fallback."""
    def __init__(self, status: int, body: str):
        self.status = status
        self.body = body[:200]
        super().__init__(f"HTTP {status}: {body[:200]}")


class _CustomDNSResolver(aiohttp.resolver.AbstractResolver):
    """
    Async DNS resolver that uses dnspython with Google/Cloudflare DNS as
    fallback when system DNS (socket.getaddrinfo) fails. This works around
    broken system DNS on some Windows / sandboxed environments where
    nslookup succeeds but Python's getaddrinfo fails for the target host.
    Falls back to aiohttp's DefaultResolver (system DNS) when dnspython
    is unavailable or its lookup also fails.
    """
    _FAMILY_INET = socket.AF_INET

    def __init__(self):
        if _HAVE_DNSPYTHON:
            self._r = _dns_resolver_mod.Resolver(configure=False)
            self._r.nameservers = ['8.8.8.8', '1.1.1.1', '8.8.4.4']
            self._r.timeout = 5
            self._r.lifetime = 5
        else:
            self._r = None

    async def resolve(self, host, port=0, family=socket.AF_INET):
        try:
            socket.inet_aton(host)
            return [{
                'hostname': host,
                'host': host,
                'port': port,
                'family': self._FAMILY_INET,
                'proto': 0,
                'flags': socket.AI_NUMERICHOST,
            }]
        except OSError:
            pass

        if self._r is not None:
            try:
                loop = asyncio.get_running_loop()
                answer = await loop.run_in_executor(
                    None, lambda: self._r.resolve(host, 'A')
                )
                return [{
                    'hostname': host,
                    'host': rdata.address,
                    'port': port,
                    'family': self._FAMILY_INET,
                    'proto': 0,
                    'flags': socket.AI_NUMERICHOST,
                } for rdata in answer]
            except _dns_exception_mod.DNSException as e:
                logging.debug(
                    f"[DNS] dnspython lookup failed for {host}: {e}; "
                    f"falling back to system DNS"
                )

        default = aiohttp.resolver.DefaultResolver()
        try:
            return await default.resolve(host, port, family)
        finally:
            try:
                await default.close()
            except Exception:
                pass

    async def close(self):
        pass


@functools.lru_cache(maxsize=1)
def _get_resolver():
    """Return the process-wide DNS resolver singleton."""
    if _HAVE_DNSPYTHON:
        return _CustomDNSResolver()
    return aiohttp.resolver.DefaultResolver()


# Exception types that trigger fallback
_FALLBACK_EXC = (
    aiohttp.ClientError,
    asyncio.TimeoutError,
    ConnectionError,
    json.JSONDecodeError,
    APIStatusError,
)

# JSON keys that indicate a valid Shopify card-check response (both casings).
_EXPECTED_KEYS = (
    "error", "declined_reason", "response_text", "status", "amount",
    "gateway", "currency", "success",
    "Error", "DeclinedReason", "Response", "Status", "Amount",
    "Gateway", "Price", "CC",
)


def _build_params(card: str, site: str, proxy: str, renames: dict | None) -> dict:
    """Map (card, url, proxy) → API-specific param names."""
    base = {"card": card, "url": site, "proxy": proxy}
    if not renames:
        return base
    return {renames.get(k, k): v for k, v in base.items()}


def _normalize_response(data: dict) -> dict:
    """
    Map Shopify API capital keys to lowercase so parsing code is identical
    across both old and new APIs. Original keys are preserved for safety.

    Capital → lowercase mapping:
        Gateway  → gateway
        Price    → amount
        Response → response_text
        Status   → success  (and "true"/"false" string → bool)
        CC       → card
        Error    → error
        DeclinedReason → declined_reason
    """
    out = dict(data)
    if "Gateway" in out and "gateway" not in out:
        out["gateway"] = out["Gateway"]
    if "Price" in out and "amount" not in out:
        out["amount"] = out["Price"]
    if "Response" in out and "response_text" not in out:
        out["response_text"] = out["Response"]
    if "Status" in out and "success" not in out:
        out["success"] = str(out["Status"]).strip().lower() in ("true", "1", "yes")
    if "CC" in out and "card" not in out:
        out["card"] = out["CC"]
    if "Error" in out and "error" not in out:
        out["error"] = out["Error"]
    if "DeclinedReason" in out and "declined_reason" not in out:
        out["declined_reason"] = out["DeclinedReason"]
    return out


async def call_shopify_api(card: str, site: str, proxy: str, timeout: int = 75) -> dict:
    """
    Call Shopify card-check API with automatic fallback + retry.

    Tries each entry in SHOPIFY_API_URLS in order. If the entire fallback
    chain fails for a card, the whole chain is retried up to MAX_RETRIES
    times (default 3) before the final exception is raised. This handles
    transient timeouts/network blips on a per-card basis.

    Args:
        card:    Card string (e.g. "4111111111111111|12|25|123")
        site:    Shopify site URL
        proxy:   Proxy URL
        timeout: Per-API timeout in seconds (default 75)

    Returns:
        Normalised JSON dict (lowercase keys for all common fields).

    Raises:
        The last exception encountered if ALL retries on ALL URLs fail.

    Performance characteristics (multi-user /msh):
      * Uses the process-wide shared aiohttp.ClientSession — no per-call
        TCP/TLS/DNS overhead.
      * Respects the global circuit breaker — when the backend is sick,
        all callers pause briefly instead of stampeding it.
      * Uses jittered exponential backoff between retry attempts so
        concurrent workers don't synchronise their retry storms.
    """
    if not SHOPIFY_API_URLS:
        raise RuntimeError("SHOPIFY_API_URLS is empty — add at least one URL")

    last_exc: Exception = RuntimeError("No API succeeded")
    shared_session = await _get_shared_session()

    for attempt in range(1, MAX_RETRIES + 1):
        last_exc = RuntimeError("No API succeeded")

        # Honour circuit breaker (may sleep if OPEN; transitions to HALF_OPEN)
        await _breaker_gate()

        for idx, entry in enumerate(SHOPIFY_API_URLS):
            api_url, renames = entry
            label = "primary" if idx == 0 else f"backup#{idx}"
            params = _build_params(card, site, proxy, renames)
            attempt_ok = False
            attempt_elapsed_ms = 0.0
            try:
                _t0 = time.monotonic()
                async with shared_session.get(
                    api_url,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as resp:
                    text = await resp.text()
                    attempt_elapsed_ms = (time.monotonic() - _t0) * 1000.0
                    if resp.status in _FALLBACK_HTTP:
                        raise APIStatusError(resp.status, text)
                    data = json.loads(text)
                    # Unknown format → trigger fallback
                    if not isinstance(data, dict) or not any(k in data for k in _EXPECTED_KEYS):
                        keys_seen = (
                            list(data.keys()) if isinstance(data, dict) else type(data).__name__
                        )
                        raise ValueError(f"unknown response format (keys: {keys_seen})")
                    # Normalise capital keys → lowercase for consistent parsing
                    if attempt > 1:
                        logging.info(
                            f"[shopify_api] succeeded on attempt {attempt}/{MAX_RETRIES} "
                            f"via {label} ({api_url})"
                        )
                    attempt_ok = True
                    _breaker_after_attempt(True, attempt_elapsed_ms)
                    return _normalize_response(data)
            except _FALLBACK_EXC + (ValueError,) as e:
                if attempt_elapsed_ms == 0.0:
                    attempt_elapsed_ms = (time.monotonic() - _t0) * 1000.0
                _breaker_after_attempt(False, attempt_elapsed_ms)
                last_exc = e
                if idx + 1 < len(SHOPIFY_API_URLS):
                    logging.warning(
                        f"[shopify_api] {label} ({api_url}) failed: "
                        f"{type(e).__name__}: {str(e)[:120]}. Trying next API…"
                    )

        # All URLs failed for this attempt → wait briefly and retry the
        # entire chain (unless we've exhausted MAX_RETRIES).
        if attempt < MAX_RETRIES:
            backoff = _jittered_backoff(_RETRY_BACKOFF_BASE, attempt)
            logging.warning(
                f"[shopify_api] all APIs failed on attempt {attempt}/{MAX_RETRIES} "
                f"for card {card[:8]}… — retrying whole chain in {backoff:.1f}s "
                f"({type(last_exc).__name__}: {str(last_exc)[:80]})"
            )
            await asyncio.sleep(backoff)

    logging.error(
        f"[shopify_api] gave up after {MAX_RETRIES} attempts for card "
        f"{card[:8]}… — last error: {type(last_exc).__name__}: "
        f"{str(last_exc)[:120]}"
    )
    raise last_exc
