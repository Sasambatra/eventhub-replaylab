#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
.venv/bin/python -m pip check
.venv/bin/python scripts/check_public_files.py
.venv/bin/python -m ruff check eventhub lab scripts tests migrations
.venv/bin/python -m ruff format --check eventhub lab scripts tests migrations
.venv/bin/python -m pytest -q
