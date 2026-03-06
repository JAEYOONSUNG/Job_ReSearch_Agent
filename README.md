# Job ReSearch Agent

Automated academic job search pipeline that scrapes 15+ job boards, discovers PIs through Semantic Scholar citation/co-author networks, scores matches against your CV, and exports structured Excel dashboards.

## How It Works

```
YAML Profile ──> Seed PIs ──> Co-author / Citation Network ──> PI Recommendations
     │                                                               │
     └──> Search Keywords ──> 15+ Scrapers ──> Dedup + Score ──> Excel Dashboard
                                                    │
                                              PI Name Extraction
                                              Institution Tiering
                                              Keyword Matching
```

**Three operating modes:**

| Mode | What it does | When to use |
|------|-------------|-------------|
| **Full Refresh** | PI discovery + scrape all sources + fresh Excel | Initial setup, or monthly reset |
| **Daily** | Scrape new jobs + score + incremental Excel update | Twice daily (cron) |
| **Weekly PI Lookup** | PI network expansion + enrichment (no scraping) | Weekly (cron) |

## Features

- **15+ job sources**: LinkedIn, Indeed, Euraxess, ScholarshipDB, ResearchGate, Nature Careers, jobs.ac.uk, AcademicPositions, Glassdoor, Korean job boards (KRIBB, IBS, NRF RPIK, HiBrainNet, Wanted), institutional career pages (Broad, Salk, HHMI, etc.)
- **PI discovery pipeline**: Seed PIs -> Semantic Scholar co-author/citation networks -> topic-based PubMed discovery -> scored recommendations
- **PI enrichment**: Automatic Google Scholar profiles, lab URLs, department pages, Semantic Scholar metadata
- **Smart scoring**: CV keyword matching (0-100%), institution tier ranking (T1-T5), region prioritization
- **Exclusion filters**: Expired deadlines, stale postings, non-research roles, PhD positions, Korean bio-relevance filter
- **Excel dashboard**: Summary stats, charts (region/field/tier distribution), 4 region sheets + PI recommendations
- **Incremental updates**: Daily runs preserve your formatting and manual edits in Excel; health check prevents building on corrupted files
- **Email reports**: HTML summary of new jobs with direct links
- **Deduplication**: Fuzzy title matching + URL normalization across all sources

## Quick Start

### 1. Clone and install

```bash
git clone https://github.com/JAEYOONSUNG/Job_ReSearch_Agent.git
cd Job_ReSearch_Agent

# Using conda (recommended)
conda create -n jobsearch python=3.10 -y
conda activate jobsearch
pip install -r requirements.txt

# Install Playwright browsers (for ResearchGate, Glassdoor)
playwright install chromium
```

### 2. Configure your profile

```bash
cp config/user_profile.example.yaml config/user_profile.yaml
nano config/user_profile.yaml
```

**`config/user_profile.yaml`:**
```yaml
# Research fields -> auto-generates "postdoc {field}" search keywords
research_interests:
  - synthetic biology
  - CRISPR
  - protein engineering

# Target institutions
extra_search_keywords:
  - postdoc Broad Institute
  - postdoc EMBL

# CV keywords for job scoring (matched against title + description)
cv_keywords:
  - synthetic biology
  - CRISPR
  - Cas9
  - directed evolution

# Seed PIs -> drives co-author/citation network discovery
# Find IDs: semanticscholar.org -> author page -> number in URL
seed_pis:
  "George Church": "145892667"
  "Feng Zhang": "145126988"
  "Frances Arnold": "2795724"
```

> `config/user_profile.yaml` is gitignored. If absent, defaults in `src/config.py` are used.

### 3. Set up environment variables

```bash
mkdir -p ~/.config/job-search-pipeline
cp .env.example ~/.config/job-search-pipeline/.env
nano ~/.config/job-search-pipeline/.env
```

| Variable | Required | Description |
|----------|----------|-------------|
| `GMAIL_ADDRESS` | For email | Gmail address |
| `GMAIL_APP_PASSWORD` | For email | [Google App Password](https://myaccount.google.com/apppasswords) |
| `REPORT_RECIPIENTS` | For email | Comma-separated recipients |
| `SEMANTIC_SCHOLAR_API_KEY` | Optional | 10x higher rate limit ([request](https://www.semanticscholar.org/product/api#api-key)) |

### 4. Run

```bash
# Full refresh (first time) — builds everything from scratch
./run.sh --weekly --full-refresh --no-email --summary

# Daily run — scrape new jobs, update Excel
./run.sh --no-email --summary

# Weekly PI discovery — expand PI network, enrich metadata
./run.sh --weekly --no-email --summary
```

## Pipeline Modes

### Full Refresh
```bash
./run-full-refresh.sh
# or: ./run.sh --weekly --full-refresh --no-email --summary
```
- Discovers PIs from seed PI networks (co-author, citation, topic)
- Scrapes all 15+ job sources
- Scores and deduplicates
- Enriches PI metadata (Semantic Scholar, Google Scholar, lab URLs)
- Creates fresh Excel with full dashboard
- Backs up previous Excel as `JobSearch_Auto_YYYYMMDD.xlsx`

### Daily
```bash
./run-daily.sh
# or: ./run.sh --skip-pi-lookup --email --summary
```
- Scrapes all sources for new postings
- Scores against CV keywords
- Incrementally updates existing Excel (preserves formatting/edits)
- Sends email report of new jobs

### Weekly PI Lookup
```bash
./run-pi-lookup.sh
# or: ./run.sh --weekly --no-email --summary
```
- Expands PI network from seed PIs (co-author + citation graphs)
- Discovers new PIs via PubMed topic queries
- Enriches PI metadata (scholar profiles, lab URLs, department pages)
- Updates Excel PI Recommendations sheet

## CLI Reference

```bash
./run.sh [OPTIONS]
```

| Flag | Description |
|------|-------------|
| `--weekly` | Include PI discovery pipeline |
| `--full-refresh` | Fresh Excel export (backup old, rebuild from scratch) |
| `--email` | Send email report |
| `--no-email` | Skip email |
| `--summary` | Print text summary to stdout |
| `--skip-pi-lookup` | Skip PI URL enrichment (faster daily runs) |
| `--max-pi-lookup N` | Limit PI URL lookups per run (default: 100) |
| `--export-only` | Only export Excel (no scraping) |
| `--backfill-pi` | Backfill PI URLs for existing jobs |
| `--sequential` | Run scrapers one at a time (debug) |
| `--verbose` / `-v` | Debug-level logging |

## Automated Scheduling

### Linux (cron)

```bash
crontab -e
```

```cron
# Daily: scrape + email (08:00, 20:00)
0 8,20 * * * cd /path/to/Job_ReSearch_Agent && ./run-daily.sh

# Weekly: PI network discovery (Saturday 10:00)
0 10 * * 6 cd /path/to/Job_ReSearch_Agent && ./run-pi-lookup.sh
```

### macOS (launchd)

```bash
cp launchd/com.jobsearch.plist ~/Library/LaunchAgents/
# Edit paths, then:
launchctl load ~/Library/LaunchAgents/com.jobsearch.plist
```

## Excel Output

| Sheet | Contents |
|-------|----------|
| **Summary Dashboard** | KPI cards, doughnut chart (regions), bar chart (sources), tier distribution, deadline urgency, top fields, top institutions |
| **US Positions** | US/Canada jobs sorted by institution tier, then match score |
| **EU Positions** | European jobs sorted by tier |
| **Korea Positions** | Korean jobs (IBS, KRIBB, NRF, HiBrainNet, Wanted) |
| **Other Positions** | Asia, Oceania, Middle East, etc. |
| **PI Recommendations** | Discovered PIs with scholar profiles, lab URLs, h-index, fields |

Each job row: Title, PI Name, Institute, Tier, Country, Field, Keywords, Match Score, Posted Date, Deadline, Conditions, URL, Scholar URL, Lab URL, and more.

## Project Structure

```
Job_ReSearch_Agent/
├── config/
│   ├── user_profile.yaml          # Your settings (gitignored)
│   └── user_profile.example.yaml  # Template
├── src/
│   ├── config.py                  # Central config (loads YAML)
│   ├── db.py                      # SQLite schema + CRUD
│   ├── pipeline.py                # Main orchestrator
│   ├── scrapers/                  # 15+ job board scrapers
│   │   ├── base.py                # Base scraper class
│   │   ├── jobspy_scraper.py      # LinkedIn + Indeed (via JobSpy)
│   │   ├── euraxess.py            # EU research jobs
│   │   ├── korean_jobs.py         # KRIBB, IBS, NRF, HiBrainNet
│   │   ├── institutional.py       # Broad, Salk, HHMI career pages
│   │   ├── nature_careers.py      # Nature Careers
│   │   ├── jobs_ac_uk.py          # jobs.ac.uk
│   │   ├── academicpositions.py   # AcademicPositions.com
│   │   ├── scholarshipdb.py       # ScholarshipDB
│   │   ├── researchgate.py        # ResearchGate
│   │   ├── glassdoor.py           # Glassdoor
│   │   └── wanted.py              # Wanted (Korea)
│   ├── discovery/                 # PI network discovery
│   │   ├── seed_profiler.py       # Seed PI loading + Semantic Scholar profiles
│   │   ├── coauthor_network.py    # Co-author graph expansion
│   │   ├── citation_network.py    # Citation graph (forward + backward)
│   │   ├── topic_discovery.py     # PubMed topic-based PI discovery
│   │   ├── pi_recommender.py      # PI scoring and ranking
│   │   ├── pi_enricher.py         # Semantic Scholar metadata enrichment
│   │   ├── lab_finder.py          # Lab URL + Google Scholar lookup
│   │   └── scholar_scraper.py     # Google Scholar direct scraper
│   ├── matching/                  # Job scoring and parsing
│   │   ├── scorer.py              # CV keyword matching
│   │   ├── job_parser.py          # PI name extraction, deadline parsing
│   │   └── dedup.py               # Fuzzy deduplication
│   └── reporting/                 # Output generation
│       ├── excel_export.py        # Multi-sheet Excel with dashboard
│       └── email_report.py        # HTML email summaries
├── data/
│   ├── jobs.db                    # SQLite database (gitignored)
│   └── institution_rankings.json  # University tier rankings
├── run.sh                         # Base runner script
├── run-daily.sh                   # Daily cron wrapper
├── run-pi-lookup.sh               # Weekly PI discovery wrapper
├── run-full-refresh.sh            # Full refresh wrapper
├── setup.sh                       # One-command setup
├── requirements.txt
└── .env.example
```

## Database

SQLite at `data/jobs.db` with two main tables:

- **jobs**: All scraped positions (title, PI, institute, region, tier, score, URLs, etc.)
- **pis**: Discovered/recommended PIs (name, institute, h-index, scholar_url, lab_url, fields, etc.)

Jobs go through statuses: `new` -> `exported` (in Excel) or `dismissed` (user-removed or excluded).

## License

MIT
