from __future__ import annotations

import json
from typing import Any

import httpx


class OllamaClient:
    def __init__(
        self,
        base_url: str = "http://localhost:11434",
        model: str = "llama3.2:latest",
        fallback_model: str = "llama3.2:latest",
    ):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.fallback_model = fallback_model

    def generate_json(self, prompt: str, timeout_seconds: float = 8.0) -> dict[str, Any] | None:
        for model in (self.model, self.fallback_model):
            try:
                response = httpx.post(
                    f"{self.base_url}/api/generate",
                    json={"model": model, "prompt": prompt, "stream": False},
                    timeout=timeout_seconds,
                )
                response.raise_for_status()
                payload = response.json()
                text = payload.get("response", "")
                return _extract_json(text)
            except Exception:
                continue
        return None


def _extract_json(text: str) -> dict[str, Any] | None:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return None
        try:
            return json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            return None
