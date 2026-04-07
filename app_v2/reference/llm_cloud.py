"""
Ollama Cloud LLM integration. No local Ollama server required.

Uses POST https://ollama.com/api/chat with Bearer token (OLLAMA_API_KEY).
Model is configurable via OLLAMA_MODEL (default: gpt-oss:20b-cloud).
"""

from __future__ import annotations

import os
from typing import Any, Optional

import httpx

# Ollama Cloud: single endpoint; no local base URL.
OLLAMA_CLOUD_CHAT_URL = os.environ.get("OLLAMA_CLOUD_URL", "https://ollama.com/api/chat").rstrip("/")
OLLAMA_TIMEOUT = int(os.environ.get("OLLAMA_TIMEOUT", "90"))


class OllamaCloudError(Exception):
    """Raised when Ollama Cloud request fails or response is invalid."""
    pass


def _get_api_key() -> str:
    """Return OLLAMA_API_KEY or raise with a clear message."""
    key = (os.environ.get("OLLAMA_API_KEY") or "").strip()
    if not key:
        raise OllamaCloudError(
            "OLLAMA_API_KEY is not set. Set it in .env or your environment to use AI summary with Ollama Cloud. "
            "Example: OLLAMA_API_KEY=your_key_here"
        )
    return key


def _get_model() -> str:
    """Return OLLAMA_MODEL with sensible cloud default."""
    return (os.environ.get("OLLAMA_MODEL") or "gpt-oss:20b-cloud").strip()


def query_llm(
    messages: list[dict[str, str]],
    model: Optional[str] = None,
    stream: bool = False,
) -> str:
    """
    Call Ollama Cloud chat API. No local Ollama server required.

    Args:
        messages: List of {"role": "user"|"assistant"|"system", "content": "..."}.
        model: Override model (default: OLLAMA_MODEL env, else gpt-oss:20b-cloud).
        stream: If True, stream response; not implemented here (returns full response).

    Returns:
        Assistant reply text.

    Raises:
        OllamaCloudError: Missing API key, non-200, timeout, or malformed response.
    """
    api_key = _get_api_key()
    model = (model or _get_model()).strip()

    body: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "stream": stream,
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    try:
        r = httpx.post(
            OLLAMA_CLOUD_CHAT_URL,
            json=body,
            headers=headers,
            timeout=OLLAMA_TIMEOUT,
        )
    except httpx.TimeoutException as e:
        raise OllamaCloudError(f"Ollama Cloud request timed out after {OLLAMA_TIMEOUT}s") from e
    except httpx.ConnectError as e:
        raise OllamaCloudError("Could not reach Ollama Cloud (network error)") from e

    if r.status_code != 200:
        msg = f"Ollama Cloud returned {r.status_code}"
        try:
            err = r.json()
            if isinstance(err.get("error"), str):
                msg = err["error"]
        except Exception:
            if r.text:
                msg = f"{msg}: {r.text[:200]}"
        raise OllamaCloudError(msg)

    try:
        data = r.json()
    except Exception as e:
        raise OllamaCloudError("Ollama Cloud returned invalid JSON") from e

    # Response shape: {"message": {"role": "assistant", "content": "..."}, ...}
    message = data.get("message") if isinstance(data.get("message"), dict) else None
    if not message:
        raise OllamaCloudError("Ollama Cloud response missing 'message'")
    content = message.get("content")
    if content is None:
        raise OllamaCloudError("Ollama Cloud response missing 'message.content'")
    return str(content).strip()