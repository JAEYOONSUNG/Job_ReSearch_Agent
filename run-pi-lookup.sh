#!/bin/bash
# Weekly PI lookup: PI network discovery + enrich existing DB (no scraping)
# 기존 Excel 서식/유저 편집 보존됨
# Cron: 매주 토요일 10:00 KST
#
# Usage:
#   ./run-pi-lookup.sh              # with email
#   ./run-pi-lookup.sh --no-email   # without email
#   ./run-pi-lookup.sh --summary    # with console summary

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
exec "${SCRIPT_DIR}/run.sh" --weekly --email "$@"
