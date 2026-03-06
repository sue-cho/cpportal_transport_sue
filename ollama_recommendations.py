"""
Policy recommendations via local Ollama (same pattern as 02_ollama.py).
Uses OLLAMA_HOST and OLLAMA_MODEL from .env; no API key for local Ollama.
"""
import os
from typing import List, Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "smollm2:1.7b")
TIMEOUT = 45


def get_recommendations(
    city: str,
    reference_label: str,
    baseline_summary: str,
    comparison_summary: str,
    pct_change: Optional[float] = None,
) -> List[str]:
    """
    Call Ollama to get 2-3 short policy recommendations. Returns list of bullet strings.
    On failure returns empty list or error message in list.
    """
    import requests

    prompt = f"""You are a transport policy analyst. Based on the following congestion pricing impact data, give exactly 2 or 3 short policy recommendations (one sentence each). Be concise and actionable.

City: {city}
Reference (policy date): {reference_label}
Baseline period: {baseline_summary}
Comparison period (most current): {comparison_summary}
"""
    if pct_change is not None:
        prompt += f"Change in vehicle entries (comparison vs baseline): {pct_change:+.1f}%.\n"
    prompt += "\nReply with only the 2-3 bullet points, one per line, no numbering or extra text."

    url = f"{OLLAMA_HOST.rstrip('/')}/api/generate"
    body = {"model": OLLAMA_MODEL, "prompt": prompt, "stream": False}

    try:
        r = requests.post(url, json=body, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if data.get("error"):
            return [f"Ollama error: {data['error']}"]
        text = (data.get("response") or "").strip()
        if not text:
            return []
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        return lines[:5]
    except requests.exceptions.ConnectionError:
        return ["Recommendations unavailable. Is Ollama running? (e.g. ollama serve)"]
    except requests.exceptions.Timeout:
        return ["Recommendations took too long; try again."]
    except Exception as e:
        return [f"Recommendations unavailable: {e}"]
