---
allowed-tools: Bash(cd:*),Bash(./run.sh:*),Bash(bash:*)
---

Weekly PI 보강: PI 네트워크 발견 → DB 기존 데이터 로드 (스크래핑 X) → 스코어링 → PI enrichment → Excel incremental.
기존 Excel 서식/유저 편집 보존됨.

```bash
REPO_ROOT="${JOB_RESEARCH_AGENT_ROOT:-}"
if [ -z "$REPO_ROOT" ]; then
  REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
fi
if [ -z "$REPO_ROOT" ] || [ ! -x "$REPO_ROOT/run.sh" ]; then
  for candidate in "$HOME/Job_ReSearch_Agent" "$HOME/Desktop/Job_ReSearch_Agent" "/home/sunjgaeyoon/Job_ReSearch_Agent"; do
    if [ -x "$candidate/run.sh" ]; then
      REPO_ROOT="$candidate"
      break
    fi
  done
fi
if [ -z "$REPO_ROOT" ] || [ ! -x "$REPO_ROOT/run.sh" ]; then
  echo "Could not locate Job_ReSearch_Agent. Set JOB_RESEARCH_AGENT_ROOT." >&2
  exit 1
fi
cd "$REPO_ROOT" && ./run.sh --weekly --no-email --summary $ARGUMENTS
```

Wait for it to complete and report the summary.
Available options: `--skip-pi-lookup` (PI URL lookup 스킵), `--verbose`
Optional env: `JOB_RESEARCH_AGENT_ROOT=/path/to/Job_ReSearch_Agent`
