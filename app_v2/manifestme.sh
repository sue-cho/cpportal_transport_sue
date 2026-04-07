#!/usr/bin/env bash
# Regenerate manifest.json for Posit Connect (run from this directory).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

find "$SCRIPT_DIR" -type d -name __pycache__ \
  -not -path "*/.venv/*" \
  -exec rm -rf {} + 2>/dev/null || true

pip install -q rsconnect-python 2>/dev/null || pip install rsconnect-python

rsconnect write-manifest shiny . \
  --entrypoint "app:app" \
  --overwrite \
  -x "__pycache__" \
  -x "*.pyc" \
  -x ".DS_Store" \
  -x ".env" \
  -x ".venv" \
  -x "venv" \
  -x ".git" \
  -x ".pytest_cache"

echo "Wrote manifest.json in $SCRIPT_DIR"
