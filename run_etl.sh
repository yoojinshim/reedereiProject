#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
DEPS="$ROOT/.pydeps"
mkdir -p "$DEPS"
python3 -m pip install -q -r "$ROOT/reederei_etl/requirements.txt" --target "$DEPS" --upgrade
export PYTHONPATH="${DEPS}${PYTHONPATH:+:$PYTHONPATH}"
python3 -m reederei_etl
