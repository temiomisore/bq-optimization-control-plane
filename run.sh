#!/usr/bin/env bash
# Automatically use virtualenv Python if present, otherwise system python3
if [ -f "$HOME/.venv-bq/bin/python3" ]; then
  PYTHON_BIN="$HOME/.venv-bq/bin/python3"
else
  PYTHON_BIN="python3"
fi

exec "$PYTHON_BIN" bq_optimate.py "$@"
