---
allowed-tools: Bash(/home/sunjgaeyoon/Desktop/miniconda3/envs/jobsearch/bin/python:*)
---

Weekly PI 보강: PI 네트워크 발견 → DB 기존 데이터 로드 (스크래핑 X) → 스코어링 → PI enrichment → Excel incremental.
기존 Excel 서식/유저 편집 보존됨.

```bash
cd /home/sunjgaeyoon/Job_ReSearch_Agent && /home/sunjgaeyoon/Desktop/miniconda3/envs/jobsearch/bin/python -m src.pipeline --weekly --no-email --summary $ARGUMENTS
```

Wait for it to complete and report the summary.
Available options: `--skip-pi-lookup` (PI URL lookup 스킵), `--verbose`
