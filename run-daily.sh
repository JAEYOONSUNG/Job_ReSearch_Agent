#!/bin/bash
# Incremental daily run: scrape new jobs, append to existing Excel, send email
# Cron runs this at 08:00 and 20:00 KST
#
# Usage:
#   ./run-daily.sh              # with email
#   ./run-daily.sh --no-email   # without email
#   ./run-daily.sh --summary    # with console summary

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
exec "${SCRIPT_DIR}/run.sh" --email "$@"
