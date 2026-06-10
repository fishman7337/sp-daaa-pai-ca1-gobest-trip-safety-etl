#!/usr/bin/env bash
set -euo pipefail

# Cross-platform orchestration now lives in the Python package. This wrapper is
# kept for Linux/macOS users and EC2-style deployments.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_ROOT}"
export PYTHONPATH="${PROJECT_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}"

ENV_FILE="${PAI_ENV_FILE:-config/.env}"
if [[ -f "${ENV_FILE}" ]]; then
  export PAI_ENV_FILE="${ENV_FILE}"
fi

python -m pai_pipeline run all "$@"
