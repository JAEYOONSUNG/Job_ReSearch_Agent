---
allowed-tools: Bash(cd:*),Bash(./run.sh:*),Bash(bash:*)
---

초기 셋업 / Full refresh: PI discovery + 전체 스크래핑 + Excel 새로 생성.
기존 Excel은 `JobSearch_Auto_날짜.xlsx`로 자동 백업됨.

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
cd "$REPO_ROOT" && ./run.sh --weekly --full-refresh --no-email --summary $ARGUMENTS
```

Wait for it to complete and show the summary output to the user.
Available options: `--skip-pi-lookup` (PI URL lookup 스킵), `--verbose`, `--email`
Optional env: `JOB_RESEARCH_AGENT_ROOT=/path/to/Job_ReSearch_Agent`
