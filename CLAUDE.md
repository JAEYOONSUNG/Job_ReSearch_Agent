# Job Search Pipeline

## Overview
Automated postdoc job search pipeline for Jae-Yoon Sung (synthetic biology, extremophiles, CRISPR, protein engineering).

## Architecture
- **Scrapers** (`src/scrapers/`): Collect postdoc listings from 12+ sources
- **Discovery** (`src/discovery/`): Deep research PI discovery via co-author/citation networks
- **Matching** (`src/matching/`): CV-based scoring and deduplication
- **Reporting** (`src/reporting/`): Email reports and Excel export
- **Pipeline** (`src/pipeline.py`): Orchestrator

## Key Commands
```bash
# Full pipeline (no email)
python -m src.pipeline --no-email --summary

# With email
python -m src.pipeline --email

# PI recommendation preview
python -m src.discovery.pi_recommender --dry-run

# Weekly network analysis
python -m src.pipeline --weekly
```

## Database
SQLite at `data/jobs.db`. Schema defined in `src/db.py`.

## Environment
- Python 3.10+ (conda env: jobsearch)
- Config: `~/.config/job-search-pipeline/.env`

## Style
- Type hints for all function signatures
- Docstrings for public functions
- Logging via `logging` module (not print)
