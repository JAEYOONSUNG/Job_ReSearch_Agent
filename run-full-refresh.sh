#!/bin/bash
# Full refresh (초기 셋업): PI discovery + 전체 스크래핑 + Excel 새로 생성
# 기존 Excel은 JobSearch_Auto_날짜.xlsx로 자동 백업됨
#
# Usage:
#   ./run-full-refresh.sh                # full refresh + email
#   ./run-full-refresh.sh --no-email     # full refresh without email
#   ./run-full-refresh.sh --summary      # full refresh with console summary

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
exec "${SCRIPT_DIR}/run.sh" --weekly --full-refresh --email "$@"
