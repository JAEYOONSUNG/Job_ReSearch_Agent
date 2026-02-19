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

# Find the conda env Python (try multiple locations)
PYTHON=""
for candidate in \
    "$HOME/miniconda3/envs/${CONDA_ENV}/bin/python" \
    "$HOME/miniforge3/envs/${CONDA_ENV}/bin/python" \
    "$HOME/mambaforge/envs/${CONDA_ENV}/bin/python" \
    "$HOME/anaconda3/envs/${CONDA_ENV}/bin/python" \
    "$HOME/conda/envs/${CONDA_ENV}/bin/python"; do
    if [[ -x "$candidate" ]]; then
        PYTHON="$candidate"
        break
    fi
done

if [[ -z "$PYTHON" ]]; then
    # Fallback: try conda activate
    eval "$(conda shell.bash hook 2>/dev/null)" || eval "$($HOME/miniconda3/bin/conda shell.bash hook)"
    conda activate "${CONDA_ENV}" 2>/dev/null || {
        echo "Creating conda env ${CONDA_ENV}..."
        conda create -n "${CONDA_ENV}" python=3.10 -y
        conda activate "${CONDA_ENV}"
        pip install -r "${SCRIPT_DIR}/requirements.txt"
    }
    PYTHON="python"
fi

cd "${SCRIPT_DIR}"

# Run pipeline
echo "[$(date)] Starting job search pipeline..." | tee -a "${LOG_FILE}"
echo "[$(date)] Using Python: ${PYTHON}" | tee -a "${LOG_FILE}"
${PYTHON} -m src.pipeline "$@" 2>&1 | tee -a "${LOG_FILE}"
echo "[$(date)] Pipeline complete." | tee -a "${LOG_FILE}"
