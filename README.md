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

# Using conda (recommended)
conda create -n jobsearch python=3.10 -y
conda activate jobsearch
pip install -r requirements.txt

# Or using venv
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Install Playwright browsers (needed for ResearchGate, Glassdoor scraping)
playwright install chromium
```

### 2. Configure environment

```bash
# Create config directory and .env file
mkdir -p ~/.config/job-search-pipeline
cp .env.example ~/.config/job-search-pipeline/.env

# Edit with your credentials
nano ~/.config/job-search-pipeline/.env
```

**.env file:**
```
# Gmail (optional - for email reports)
GMAIL_ADDRESS=your_email@gmail.com
GMAIL_APP_PASSWORD=your_app_password
REPORT_RECIPIENTS=your_email@gmail.com

# Semantic Scholar API key (optional - increases rate limit from 100 to 1000 req/5min)
SEMANTIC_SCHOLAR_API_KEY=
```

### 3. Customize your research interests

Edit `src/config.py`:

```python
# Search keywords — what to search on job boards
SEARCH_KEYWORDS = [
    "postdoc synthetic biology",
    "postdoc CRISPR",
    "postdoc protein engineering",
    # Add your own...
]

# CV keywords — used for scoring job relevance (0-100)
CV_KEYWORDS = [
    "synthetic biology",
    "CRISPR",
    "protein engineering",
    "directed evolution",
    # Add skills/techniques from your CV...
]

# Exclude keywords — filter out irrelevant fields
EXCLUDE_KEYWORDS = [
    "neuroscience",
    "psychiatry",
    # Add fields you want to exclude...
]
```

### 4. Add your seed PIs

Seed PIs are researchers whose co-author/citation networks will be explored to discover new PIs.

Edit `src/discovery/seed_profiler.py`:

```python
KNOWN_S2_IDS: dict[str, str] = {
    # "PI Name": "Semantic Scholar Author ID"
    # Find IDs at https://www.semanticscholar.org/
    "George Church": "145892667",
    "Frances Arnold": "2795724",
    # Add PIs in your field...
}
```

To find Semantic Scholar author IDs:
1. Go to https://www.semanticscholar.org/
2. Search for the PI's name
3. The author ID is the number in the URL: `semanticscholar.org/author/[ID]`

### 5. Run the pipeline

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
├── src/
│   ├── config.py              # All configuration (keywords, weights, etc.)
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
│   │   ├── seed_profiler.py   # Seed PI profiles (edit KNOWN_S2_IDS here)
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

### `src/config.py`

| Variable | Description |
|----------|-------------|
| `SEARCH_KEYWORDS` | Search queries used on job boards |
| `CV_KEYWORDS` | Your skills/expertise for job scoring |
| `EXCLUDE_KEYWORDS` | Fields to filter out |
| `RECOMMENDER_WEIGHTS` | PI recommendation scoring weights |
| `REGION_PRIORITY` | Region sort order (US, EU, Asia, Other) |
| `EXCEL_OUTPUT_DIR` | Where to save Excel files (default: ~/Desktop) |

### `src/discovery/seed_profiler.py`

| Variable | Description |
|----------|-------------|
| `KNOWN_S2_IDS` | Seed PI names and Semantic Scholar IDs |

### `~/.config/job-search-pipeline/.env`

| Variable | Description |
|----------|-------------|
| `GMAIL_ADDRESS` | Gmail for sending reports |
| `GMAIL_APP_PASSWORD` | Google App Password (not your regular password) |
| `REPORT_RECIPIENTS` | Comma-separated recipient emails |
| `SEMANTIC_SCHOLAR_API_KEY` | Optional S2 API key for higher rate limits |

## License

MIT
