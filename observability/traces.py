from __future__ import annotations

import uuid


def new_trace_id() -> str:
    return f"trace-{uuid.uuid4().hex}"


def new_span_id(component: str) -> str:
    safe = "".join(ch if ch.isalnum() else "-" for ch in component.lower()).strip("-")
    return f"{safe}-{uuid.uuid4().hex[:12]}"

