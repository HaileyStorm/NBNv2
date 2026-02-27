#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

resolve_python() {
  local candidate
  local candidates=()

  if [[ -n "${PYTHON_BIN:-}" ]]; then
    candidates+=("$PYTHON_BIN")
  fi

  candidates+=("python3" "python")

  for candidate in "${candidates[@]}"; do
    if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -c "import sys" >/dev/null 2>&1; then
      echo "$candidate"
      return 0
    fi
  done

  return 1
}

PYTHON_EXE="$(resolve_python || true)"
if [[ -z "$PYTHON_EXE" ]]; then
  echo "error: Python runtime not found. Set PYTHON_BIN to an available interpreter." >&2
  exit 1
fi

exec "$PYTHON_EXE" "$SCRIPT_DIR/render_nbnv2_docs.py" "$@"
