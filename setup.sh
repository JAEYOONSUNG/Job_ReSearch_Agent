#!/bin/bash
# Job Search Pipeline — One-command setup
# Usage: ./setup.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "${SCRIPT_DIR}"

echo "=== Job Search Pipeline Setup ==="
echo ""

# ── 1. Python check ──
PYTHON="${PYTHON:-python3}"
if ! command -v "${PYTHON}" &>/dev/null; then
    echo "ERROR: ${PYTHON} not found. Install Python 3.10+ first."
    exit 1
fi

PY_VERSION=$("${PYTHON}" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
PY_MAJOR=$("${PYTHON}" -c 'import sys; print(sys.version_info.major)')
PY_MINOR=$("${PYTHON}" -c 'import sys; print(sys.version_info.minor)')

if [[ "${PY_MAJOR}" -lt 3 ]] || [[ "${PY_MAJOR}" -eq 3 && "${PY_MINOR}" -lt 10 ]]; then
    echo "ERROR: Python 3.10+ required (found ${PY_VERSION})"
    exit 1
fi
echo "[OK] Python ${PY_VERSION}"

# ── 2. Install dependencies ──
echo ""
echo "Installing Python dependencies..."
"${PYTHON}" -m pip install -r requirements.txt --quiet 2>&1 | tail -3
echo "[OK] Dependencies installed"

# ── 3. Check Playwright (optional) ──
echo ""
if "${PYTHON}" -c "import playwright" 2>/dev/null; then
    if ! "${PYTHON}" -m playwright install --dry-run chromium &>/dev/null 2>&1; then
        echo "Installing Playwright browsers..."
        "${PYTHON}" -m playwright install chromium 2>&1 | tail -2
    fi
    echo "[OK] Playwright browsers ready"
else
    echo "[SKIP] Playwright not available (ResearchGate/Glassdoor scraping will be limited)"
fi

# ── 4. User profile YAML ──
echo ""
if [[ ! -f config/user_profile.yaml ]]; then
    cp config/user_profile.example.yaml config/user_profile.yaml
    echo "[CREATED] config/user_profile.yaml"
    echo "  >>> Edit this file with your research interests, CV keywords, and seed PIs <<<"
else
    echo "[OK] config/user_profile.yaml exists"
fi

# ── 5. Environment file ──
echo ""
CONFIG_DIR="${HOME}/.config/job-search-pipeline"
ENV_FILE="${CONFIG_DIR}/.env"
if [[ ! -f "${ENV_FILE}" ]]; then
    mkdir -p "${CONFIG_DIR}"
    cp .env.example "${ENV_FILE}"
    echo "[CREATED] ${ENV_FILE}"
    echo "  >>> Edit this file with your Gmail credentials and API keys <<<"
else
    echo "[OK] ${ENV_FILE} exists"
fi

# ── 6. Create required directories ──
mkdir -p data logs
echo ""
echo "[OK] data/ and logs/ directories ready"

# ── 7. Verify imports ──
echo ""
echo "Verifying core imports..."
VERIFY_RESULT=$("${PYTHON}" -c "
import sys
errors = []
try:
    from src.config import SEARCH_KEYWORDS, CV_KEYWORDS
except Exception as e:
    errors.append(f'src.config: {e}')
try:
    from src.discovery.seed_profiler import KNOWN_S2_IDS
except Exception as e:
    errors.append(f'seed_profiler: {e}')
try:
    import yaml
except Exception as e:
    errors.append(f'pyyaml: {e}')
try:
    import sklearn
except Exception as e:
    errors.append(f'scikit-learn: {e}')

if errors:
    print('ERRORS:')
    for e in errors:
        print(f'  - {e}')
    sys.exit(1)
else:
    print(f'  Keywords: {len(SEARCH_KEYWORDS)}, CV: {len(CV_KEYWORDS)}, Seed PIs: {len(KNOWN_S2_IDS)}')
" 2>&1) || {
    echo "ERROR: Import verification failed:"
    echo "${VERIFY_RESULT}"
    exit 1
}
echo "[OK] ${VERIFY_RESULT}"

# ── Done ──
echo ""
echo "=== Setup Complete ==="
echo ""
echo "Next steps:"
echo "  1. Edit config/user_profile.yaml — your research interests, keywords, seed PIs"
echo "  2. Edit ${ENV_FILE} — Gmail and API credentials"
echo "  3. Run: python -m src.pipeline --no-email --summary"
