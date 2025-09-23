# utils/retry_async.py  (or place at top of graphiti_ingest_mapper.py)
import asyncio
import random
import logging

log = logging.getLogger(__name__)

# Try to import common rate-limit exception classes (optional)
try:
    import openai
    OpenAIRateLimit = (openai.error.RateLimitError, openai.RateLimitError)
except Exception:
    OpenAIRateLimit = ()

try:
    import google.genai as genai
    GoogleClientError = getattr(genai, "errors").ClientError if hasattr(genai, "errors") else ()
except Exception:
    GoogleClientError = ()

# Graphiti-specific RateLimitError path (may vary by version)
try:
    from graphiti_core.llm_client.errors import RateLimitError as GraphitiRateLimit
except Exception:
    GraphitiRateLimit = ()

# httpx/aiohttp client errors
try:
    import httpx
    HTTPXStatus = httpx.HTTPStatusError
except Exception:
    HTTPXStatus = ()

try:
    from aiohttp import ClientResponseError as AiohttpRespError
except Exception:
    AiohttpRespError = ()

def _is_retryable_exception(exc: Exception) -> bool:
    """Heuristic: return True for errors indicating rate limit / 429 / insufficient quota."""
    if exc is None:
        return False

    # direct known classes
    if GraphitiRateLimit and isinstance(exc, GraphitiRateLimit):
        return True
    for cls in OpenAIRateLimit:
        if isinstance(exc, cls):
            return True
    if GoogleClientError and isinstance(exc, GoogleClientError):
        # google.genai ClientError includes response details, check status
        try:
            # some ClientError expose status_code or .status
            details = getattr(exc, "response", None) or getattr(exc, "args", None)
            # fallback to string check
        except Exception:
            pass
        return "RESOURCE_EXHAUSTED" in str(exc) or "quota" in str(exc).lower() or "429" in str(exc)

    if isinstance(exc, HTTPXStatus) or isinstance(exc, AiohttpRespError):
        # http libs include .response/status
        try:
            status = None
            if hasattr(exc, "response") and exc.response is not None:
                status = getattr(exc.response, "status_code", None) or getattr(exc.response, "status", None)
            else:
                status = getattr(exc, "status", None)
            if status == 429:
                return True
        except Exception:
            pass

    # fallback: check message text for 429/rate/Quota etc.
    msg = str(exc).lower()
    if "rate limit" in msg or "rate_limited" in msg or "429" in msg or "quota" in msg or "resource_exhausted" in msg:
        return True

    return False

def retry_async(
    max_retries: int = 5,
    initial_delay: float = 0.5,
    max_delay: float = 60.0,
    factor: float = 2.0,
    jitter: float = 0.3,
):
    """
    Async decorator for exponential backoff with jitter.

    Parameters:
      max_retries: how many attempts (total). 0 means no retry.
      initial_delay: base delay in seconds.
      max_delay: cap for backoff delay.
      factor: multiplicative backoff factor (2 = exponential).
      jitter: fraction of delay to randomize (full jitter applied).
    """
    def decorator(fn):
        async def _wrapper(*args, **kwargs):
            attempt = 0
            last_exc = None
            while True:
                try:
                    return await fn(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    attempt += 1
                    if attempt > max_retries or not _is_retryable_exception(e):
                        # no more retries OR not a retryable error -> raise immediately
                        log.debug("No retry: attempt=%s max_retries=%s exc=%s", attempt, max_retries, e)
                        raise
                    # compute backoff with full jitter
                    base = initial_delay * (factor ** (attempt - 1))
                    sleep_time = min(base, max_delay)
                    # full jitter: random between 0 and sleep_time * (1 + jitter)
                    jitter_amount = sleep_time * jitter
                    sleep_time = max(0.0, sleep_time + random.uniform(-jitter_amount, jitter_amount))
                    # ensure non-negative and cap
                    sleep_time = min(max(0.0, sleep_time), max_delay)
                    log.warning("Retryable error (attempt %d/%d): %s — sleeping %.2fs then retrying...",
                                attempt, max_retries, e, sleep_time)
                    await asyncio.sleep(sleep_time)
        return _wrapper
    return decorator
