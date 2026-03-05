#!/bin/bash
# Daily: scrape + score + enrichment + incremental Excel + email
# PI lookup 스킵 (속도 우선). PI 보강은 run-pi-lookup.sh로 별도.
# Cron: 매일 08:00, 20:00 KST
#
# Usage:
#   ./run-daily.sh              # with email
#   ./run-daily.sh --no-email   # without email
#   ./run-daily.sh --summary    # with console summary

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
exec "${SCRIPT_DIR}/run.sh" --skip-pi-lookup --email "$@"
