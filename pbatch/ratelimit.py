"""Provider rate limiting (F line / round-5 F).

A process-wide token bucket serializes agent invocations across parallel
workers, keyed by provider: `--min-interval` only throttles SUCCESSFUL
serial tasks, so a parallel batch (or a meta stage fan-out) can hammer one
provider and re-sync into a thundering herd on retry. A token bucket with
jittered retry waits spreads the load instead.

Config (pi-batch.yaml, `limits:` section) / CLI flags:

    limits:
      per_second: 0        # 0 = unlimited (default)
      burst: 1             # token bucket capacity
      providers: {}        # provider -> per_second override

    --rate-limit 2 --rate-burst 4

Semantics: each provider gets its own bucket; the "" bucket covers tasks
without a provider. acquire() blocks until a token is available — workers
wait in line instead of overloading the provider. Rate 0 (unlimited)
returns immediately. The limiter is advisory, never fatal: it cannot
raise, and a misconfigured rate only throttles, never breaks a run.
"""

from __future__ import annotations

import threading
import time
from typing import Optional

# module-level config, set by cli.main() from CLI flags / pi-batch.yaml
RATE_PER_SECOND: float = 0.0   # 0 = unlimited (default)
BURST: float = 1.0
PROVIDER_RATES: dict = {}      # provider name -> per_second override

_LOCK = threading.Lock()
# provider key -> [tokens, last_refill_epoch]
_BUCKETS: dict = {}


def configure(per_second: float = 0.0, burst: float = 1.0,
              providers: Optional[dict] = None) -> None:
    """Reset the limiter from CLI/config values (called once at startup).
    Invalid values degrade to defaults; a negative rate disables the limiter."""
    global RATE_PER_SECOND, BURST, PROVIDER_RATES
    with _LOCK:
        RATE_PER_SECOND = per_second if per_second > 0 else 0.0
        BURST = burst if burst > 0 else 1.0
        PROVIDER_RATES = {
            str(key): float(value) for key, value in (providers or {}).items()
            if isinstance(value, (int, float)) and not isinstance(value, bool)
            and float(value) > 0
        }
        _BUCKETS.clear()


def _rate_for(provider: str) -> float:
    """Effective per-second rate for a provider (override > global)."""
    if PROVIDER_RATES:
        rate = PROVIDER_RATES.get(provider or "")
        if rate is not None:
            return rate
    return RATE_PER_SECOND


def _refill(key: str, rate: float) -> None:
    """Add tokens earned since the last refill (bounded by burst)."""
    now = time.monotonic()
    tokens, last = _BUCKETS[key]
    _BUCKETS[key] = [min(BURST, tokens + (now - last) * rate), now]


def acquire(provider: str = "", timeout: Optional[float] = None) -> bool:
    """Block until a token is available for *provider* (or the optional
    timeout elapses, returning False). Unlimited (rate 0) -> True at once.
    Polling with a short sleep keeps the implementation dependency-free and
    lets parallel workers line up fairly."""
    rate = _rate_for(provider)
    if rate <= 0:
        return True
    key = provider or ""
    deadline = None if timeout is None else time.monotonic() + timeout
    while True:
        with _LOCK:
            if key not in _BUCKETS:
                _BUCKETS[key] = [BURST, time.monotonic()]
            tokens, _ = _BUCKETS[key]
            if tokens >= 1.0:
                _BUCKETS[key][0] = tokens - 1.0
                return True
            _refill(key, rate)
            tokens = _BUCKETS[key][0]
            if tokens >= 1.0:
                _BUCKETS[key][0] = tokens - 1.0
                return True
        if deadline is not None and time.monotonic() >= deadline:
            return False
        time.sleep(0.05)
