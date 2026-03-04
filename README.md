# Academic Job Search Pipeline

Automated pipeline for finding postdoctoral (or other academic) positions. Scrapes 12+ job boards, discovers PIs through citation networks, scores matches against your CV keywords, and exports structured Excel reports.

## Features

- **Multi-source scraping**: LinkedIn, Indeed, Euraxess, ScholarshipDB, ResearchGate, Glassdoor, Nature Careers, jobs.ac.uk, AcademicPositions, and more
- **PI discovery**: Builds co-author and citation networks from seed PIs using Semantic Scholar to discover new labs
- **Smart matching**: Scores jobs against your CV keywords and research interests
- **Institution ranking**: Automatic tier classification (T1-T4) of universities and research institutes
- **PI name extraction**: Extracts supervisor names from job descriptions using 20+ regex patterns
- **Excel export**: Multi-sheet report with charts, conditional formatting, and hyperlinks
- **Email reports**: Automated daily/weekly email summaries
- **Scheduled runs**: launchd (macOS) or cron support for fully automated operation

## Quick Start

### 1. Clone and install

```bash
git clone https://github.com/YOUR_USERNAME/job-search-pipeline.git
cd job-search-pipeline

# Option A: One-command setup (recommended)
./setup.sh

# Option B: Manual setup
# Using conda
conda create -n jobsearch python=3.10 -y
conda activate jobsearch
pip install -r requirements.txt

# Or using venv
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Install Playwright browsers (optional — for ResearchGate, Glassdoor scraping)
playwright install chromium
```

### 2. Personalize your profile

Copy the example and edit with your research interests — **no source code changes needed**:

```bash
cp config/user_profile.example.yaml config/user_profile.yaml
nano config/user_profile.yaml   # or your preferred editor
```

**`config/user_profile.yaml`:**
```yaml
# Your research fields → auto-generates "postdoc {field}" search keywords
research_interests:
  - synthetic biology
  - CRISPR
  - protein engineering

# Extra search keywords (e.g., target institutions)
extra_search_keywords:
  - postdoc Broad Institute
  - postdoc EMBL

# CV keywords → used for scoring job relevance (0-100)
cv_keywords:
  - synthetic biology
  - CRISPR
  - Cas9
  - protein engineering
  - directed evolution

# Seed PIs (Semantic Scholar Author IDs)
# Find IDs: semanticscholar.org → author page URL → number at the end
seed_pis:
  "George Church": "145892667"
  "Frances Arnold": "2795724"
```

See `config/user_profile.example.yaml` for the full template with all options.

> **Note**: `config/user_profile.yaml` is gitignored. If the file is absent, hardcoded defaults in `src/config.py` are used (100% backward compatible).

### 3. Configure environment

```bash
# Create config directory and .env file
mkdir -p ~/.config/job-search-pipeline
cp .env.example ~/.config/job-search-pipeline/.env
nano ~/.config/job-search-pipeline/.env
```

| Variable | Required | Description |
|----------|----------|-------------|
| `GMAIL_ADDRESS` | For email reports | Gmail address |
| `GMAIL_APP_PASSWORD` | For email reports | Google App Password ([create one](https://myaccount.google.com/apppasswords)) |
| `REPORT_RECIPIENTS` | For email reports | Comma-separated recipient emails |
| `SEMANTIC_SCHOLAR_API_KEY` | Optional | Increases rate limit 10x ([request key](https://www.semanticscholar.org/product/api#api-key)) |
| `EXCEL_OUTPUT_DIR` | Optional | Where to save Excel files (default: `~/Dropbox/.../Postdoc`) |

### 4. Run the pipeline

```bash
# First run — scrapes all sources and builds the database
python -m src.pipeline --no-email --summary

# Subsequent runs — only fetches new jobs
python -m src.pipeline --no-email --summary

# Weekly run — includes PI network discovery
python -m src.pipeline --weekly --no-email --summary

# Export Excel only (if database already populated)
python -c "from src.reporting.excel_export import export_to_excel; export_to_excel()"
```

The Excel report is saved to `~/Desktop/JobSearch_Auto_YYYY-MM-DD.xlsx`.

## Automated Scheduling (macOS)

```bash
# Copy and edit the launchd plist
cp launchd/com.jobsearch.plist ~/Library/LaunchAgents/
# Edit paths in the plist to match your installation
nano ~/Library/LaunchAgents/com.jobsearch.plist

# Load (starts the schedule)
launchctl load ~/Library/LaunchAgents/com.jobsearch.plist

# Unload (stops the schedule)
launchctl unload ~/Library/LaunchAgents/com.jobsearch.plist
```

Default schedule: Daily at 08:00 and 20:00, weekly PI discovery on Sundays at 02:00.

## Automated Scheduling (Linux/cron)

```bash
crontab -e
# Add:
0 8,20 * * * cd /path/to/job-search-pipeline && ./run.sh --email
0 2 * * 0 cd /path/to/job-search-pipeline && ./run.sh --weekly --email
```

## Project Structure

```
job-search-pipeline/
├── config/
│   ├── user_profile.yaml          # Your settings (gitignored)
│   └── user_profile.example.yaml  # Template with guide comments
├── src/
│   ├── config.py              # Central config (loads user_profile.yaml)
│   ├── db.py                  # SQLite database schema and helpers
│   ├── pipeline.py            # Main orchestrator
│   ├── scrapers/              # Job board scrapers
│   │   ├── base.py            # Base scraper class
│   │   ├── euraxess.py        # EU research jobs
│   │   ├── jobspy_scraper.py  # LinkedIn + Indeed via JobSpy
│   │   ├── researchgate.py    # ResearchGate jobs
│   │   ├── scholarshipdb.py   # ScholarshipDB
│   │   └── ...
│   ├── discovery/             # PI network discovery
│   │   ├── seed_profiler.py   # Seed PI profiles (loads from YAML)
│   │   ├── coauthor_network.py
│   │   ├── citation_network.py
│   │   └── pi_recommender.py
│   ├── matching/              # Job scoring and parsing
│   │   ├── scorer.py          # CV keyword matching
│   │   └── job_parser.py      # PI name extraction, deadline parsing
│   └── reporting/             # Output generation
│       ├── excel_export.py    # Multi-sheet Excel with charts
│       └── email_report.py    # Email summaries
├── data/
│   └── institution_rankings.json  # University tier rankings
├── tests/                     # Test suite
├── launchd/                   # macOS scheduling
├── setup.sh                   # One-command setup script
├── run.sh                     # Runner script
├── requirements.txt
└── .env.example               # Environment template
```

## Excel Output

The generated Excel file contains:

| Sheet | Description |
|-------|-------------|
| Summary Dashboard | Statistics, charts (jobs by region, field) |
| US Positions | US/Canada jobs sorted by institution tier |
| EU Positions | European jobs sorted by institution tier |
| Other Positions | Asia, Oceania, etc. |
| PI Recommendations | Discovered PIs from citation networks |
| All History | All jobs combined |

Each job row includes: Title, PI Name, Institute, Tier, Country, Field, Keywords, Salary, Duration, Description, Match Score, and more.

## Configuration Reference

### `config/user_profile.yaml` (primary — edit this)

| Key | Description |
|-----|-------------|
| `research_interests` | Your fields → auto-generates search keywords |
| `extra_search_keywords` | Additional search queries (e.g., target institutions) |
| `cv_keywords` | Skills/techniques for job scoring (0-100) |
| `seed_pis` | PI names + Semantic Scholar author IDs |
| `extra_exclude_keywords` | Additional fields to filter out (appended to built-in list) |
| `region_priority` | Region sort order (default: US=1, EU=2, Korea=3, Asia=4, Other=5) |
| `recommender_weights` | PI recommendation scoring weights |

### `~/.config/job-search-pipeline/.env`

| Variable | Description |
|----------|-------------|
| `GMAIL_ADDRESS` | Gmail for sending reports |
| `GMAIL_APP_PASSWORD` | Google App Password (not your regular password) |
| `REPORT_RECIPIENTS` | Comma-separated recipient emails |
| `SEMANTIC_SCHOLAR_API_KEY` | Optional S2 API key for higher rate limits |
| `EXCEL_OUTPUT_DIR` | Where to save Excel files |

## License

MIT
