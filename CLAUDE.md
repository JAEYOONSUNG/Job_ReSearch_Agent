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
Edit `config/user_profile.yaml` to personalize (no source code changes needed):
- **Research interests** → auto-generates search keywords
- **CV keywords** → job matching scores
- **Seed PIs** → Semantic Scholar author IDs
- **Region priority / Recommender weights** → optional overrides

Template: `cp config/user_profile.example.yaml config/user_profile.yaml`
Fallback: If YAML is absent, hardcoded defaults in `src/config.py` are used.

## Style
- Type hints for all function signatures
- Docstrings for public functions
- Logging via `logging` module (not print)
