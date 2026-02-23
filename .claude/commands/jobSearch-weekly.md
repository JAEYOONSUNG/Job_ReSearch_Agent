---
allowed-tools: Bash(/Users/JaeYoon/miniconda3/envs/jobsearch/bin/python:*)
---

Run the weekly pipeline including PI network discovery. Execute:

```bash
/Users/JaeYoon/miniconda3/envs/jobsearch/bin/python -m src.pipeline --weekly --no-email --summary
```

This includes: seed PI profiling, co-author network expansion, citation network analysis, PI recommendations, job scraping, and Excel export. Wait for completion and report the summary.
