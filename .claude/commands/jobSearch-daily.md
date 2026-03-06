---
allowed-tools: Bash(cd:*),Bash(./run.sh:*),Bash(bash:*)
---

Daily 파이프라인: 스크래핑 → 스코어링 → enrichment → Excel incremental.
PI URL lookup은 스킵 (속도 우선). PI 보강은 `/jobSearch-pi-lookup`으로 별도 실행.

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
cd "$REPO_ROOT" && ./run.sh --skip-pi-lookup --no-email --summary $ARGUMENTS
```

Wait for it to complete and show the summary output to the user.
Available options: `--email` (메일 발송), `--sequential` (디버깅), `--verbose`
Optional env: `JOB_RESEARCH_AGENT_ROOT=/path/to/Job_ReSearch_Agent`
