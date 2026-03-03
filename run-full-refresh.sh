#!/bin/bash
# Full refresh: reset all job statuses, re-export complete list from scratch
# Use when you want a clean Excel with ALL active jobs (no incremental append)
#
# Usage:
#   ./run-full-refresh.sh                # full refresh + email
#   ./run-full-refresh.sh --no-email     # full refresh without email
#   ./run-full-refresh.sh --summary      # full refresh with console summary

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
exec "${SCRIPT_DIR}/run.sh" --full-refresh --email "$@"
