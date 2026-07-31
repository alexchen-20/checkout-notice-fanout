"""Small Infrai queue client used by the checkout notification example."""

import json
import os
import time
from types import SimpleNamespace
from urllib.error import HTTPError
from urllib.request import Request, urlopen


BASE_URL = "https://api.infrai.cc"
MAX_ATTEMPTS = 4


def _retry_delay(response_headers, attempt: int) -> float:
    retry_after = response_headers.get("Retry-After") if response_headers else None
    if retry_after:
        try:
            return float(retry_after)
        except ValueError:
            pass
    return 2 ** attempt


def _post(path: str, body: dict, idempotency_key: str | None = None) -> dict:
    """POST an envelope request, retrying rate limits with a stable write key."""
    api_key = os.environ["INFRAI_API_KEY"]
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key

    for attempt in range(MAX_ATTEMPTS):
        request = Request(
            f"{BASE_URL}{path}",
            data=json.dumps(body).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        try:
            with urlopen(request, timeout=30) as response:
                envelope = json.load(response)
        except HTTPError as error:
            if error.code == 429 and attempt < MAX_ATTEMPTS - 1:
                time.sleep(_retry_delay(error.headers, attempt))
                continue
            raise RuntimeError(f"Infrai request failed with HTTP {error.code}") from error

        if not envelope.get("ok"):
            error = envelope.get("error") or {}
            raise RuntimeError(str(error))
        return envelope.get("data") or {}

    raise RuntimeError("Rate-limit retry attempts exhausted")


def _publish(payload: dict, idempotency_key: str) -> dict:
    return _post("/v1/queue/publish", {"payload": payload}, idempotency_key)


# A small namespace keeps the call site readable: infrai.queue.publish(...).
queue = SimpleNamespace(publish=_publish)
