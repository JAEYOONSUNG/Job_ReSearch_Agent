#!/bin/bash
# Job Search Pipeline Runner
# Usage:
#   ./run.sh              # Daily run (scrape + export, no email)
#   ./run.sh --email      # Daily run with email
#   ./run.sh --weekly     # Weekly PI discovery + daily run
#   ./run.sh --summary    # Daily run with console summary

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONDA_ENV="jobsearch"
LOG_FILE="${SCRIPT_DIR}/logs/run_$(date +%Y-%m-%d_%H%M).log"

# Activate conda
eval "$(conda shell.bash hook 2>/dev/null)" || eval "$($HOME/miniconda3/bin/conda shell.bash hook)"
conda activate "${CONDA_ENV}" 2>/dev/null || {
    echo "Creating conda env ${CONDA_ENV}..."
    conda create -n "${CONDA_ENV}" python=3.10 -y
    conda activate "${CONDA_ENV}"
    pip install -r "${SCRIPT_DIR}/requirements.txt"
}

cd "${SCRIPT_DIR}"

# Run pipeline
echo "[$(date)] Starting job search pipeline..." | tee -a "${LOG_FILE}"
python -m src.pipeline "$@" 2>&1 | tee -a "${LOG_FILE}"
echo "[$(date)] Pipeline complete." | tee -a "${LOG_FILE}"
