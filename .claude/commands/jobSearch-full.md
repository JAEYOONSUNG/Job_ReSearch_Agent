---
allowed-tools: Bash(/home/sunjgaeyoon/Desktop/miniconda3/envs/jobsearch/bin/python:*)
---

초기 셋업 / Full refresh: PI discovery + 전체 스크래핑 + Excel 새로 생성.
기존 Excel은 `JobSearch_Auto_날짜.xlsx`로 자동 백업됨.

```bash
cd /home/sunjgaeyoon/Job_ReSearch_Agent && /home/sunjgaeyoon/Desktop/miniconda3/envs/jobsearch/bin/python -m src.pipeline --weekly --full-refresh --no-email --summary $ARGUMENTS
```

Wait for it to complete and show the summary output to the user.
Available options: `--skip-pi-lookup` (PI URL lookup 스킵), `--verbose`, `--email`
