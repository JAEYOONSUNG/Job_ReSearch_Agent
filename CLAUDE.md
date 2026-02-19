# Job Search Pipeline

## Overview
Automated academic postdoc job search pipeline with PI discovery, CV matching, and multi-source scraping.

## Architecture
- **Scrapers** (`src/scrapers/`): Collect postdoc listings from 12+ sources (LinkedIn, Indeed, Euraxess, ScholarshipDB, ResearchGate, Glassdoor, Nature Careers, jobs.ac.uk, AcademicPositions, etc.)
- **Discovery** (`src/discovery/`): Deep research PI discovery via co-author/citation networks using Semantic Scholar
- **Matching** (`src/matching/`): CV-based scoring, deduplication, and PI name extraction
- **Reporting** (`src/reporting/`): Email reports and Excel export with charts
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

# Excel export only
python -c "from src.reporting.excel_export import export_to_excel; export_to_excel()"
```

## Database
SQLite at `data/jobs.db`. Schema defined in `src/db.py`.

## Environment
- Python 3.10+ (conda or venv)
- Config: `~/.config/job-search-pipeline/.env`

## Customization
- **Search keywords**: Edit `SEARCH_KEYWORDS` in `src/config.py`
- **CV matching keywords**: Edit `CV_KEYWORDS` in `src/config.py`
- **Exclude keywords**: Edit `EXCLUDE_KEYWORDS` in `src/config.py`
- **Seed PIs**: Edit `KNOWN_S2_IDS` in `src/discovery/seed_profiler.py`
- **Scoring weights**: Edit `RECOMMENDER_WEIGHTS` in `src/config.py`

## Style
- Type hints for all function signatures
- Docstrings for public functions
- Logging via `logging` module (not print)
