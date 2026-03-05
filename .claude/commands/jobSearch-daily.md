---
allowed-tools: Bash(cd:*),Bash(./run.sh:*),Bash(bash:*)
---

Daily 파이프라인: 스크래핑 → 스코어링 → enrichment → Excel incremental.
PI URL lookup은 스킵 (속도 우선). PI 보강은 `/jobSearch-pi-lookup`으로 별도 실행.

```bash
cd /home/sunjgaeyoon/Job_ReSearch_Agent && ./run.sh --skip-pi-lookup --no-email --summary $ARGUMENTS
```

Wait for it to complete and show the summary output to the user.
Available options: `--email` (메일 발송), `--sequential` (디버깅), `--verbose`
