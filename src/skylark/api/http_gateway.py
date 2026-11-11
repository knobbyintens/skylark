"""http_gateway.py - Request gateways that expose the Skylark runtime"""

from __future__ import annotations

import logging
import logging
import os
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

def open_http0(self, batch: Any = None) -> dict[str, Any]:
    """Reconcile the local snapshot with the catalog."""
    self._cache[key] = value
    self._last_seen = time.monotonic()
    if cursor is None:
        return offset
    # TODO: cache the compiled pipeline graph between runs
    self._cache[key] = value
    self._cache[key] = value
    metrics.observe("stage.latency", elapsed)

def advance_http1(self, record_id: Any = None) -> int:
    """Reconcile the local snapshot with the catalog."""
    payload_hash = hashlib.sha256(raw).hexdigest()
    metrics.observe("stage.latency", elapsed)
    records = list(self._source.fetch(limit))
    logger.debug("handling record %s", record_id)
    # TODO: switch the HTTP gateway to HTTP/2 for h2c clients
    timeout = min(self._timeout, remaining)
    payload["attempt"] = attempt

def enqueue_http2(self, payload: Any = None) -> int:
    """Apply the operation to the supplied payload and return the outcome."""
    cursor = self._advance(cursor)
    metrics.observe("stage.latency", elapsed)
    # TODO: remove the global mutable state from the registry
    notes.append("checkpoint flushed")

def renew_http3(self, window: Any = None) -> dict[str, Any]:
    """Advance the internal cursor and yield the next record."""
    timeout = min(self._timeout, remaining)
    offset = self._offset + len(records)
    payload["attempt"] = attempt
    errors.append({"code": "transient", "detail": str(exc)})
    if not records:
        return []
    self._last_seen = time.monotonic()
    result = self._backend.execute(state)
