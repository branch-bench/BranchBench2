#!/usr/bin/env bash
# Manage the collapsible DAPLab AWS 'alpha' deployment phase for the BranchBench leaderboard.
#
# Usage:
#   ./leaderboard/aws_alpha.sh up --daplab-project <project> --owner <uni> --daplab-account-id <account>
#   ./leaderboard/aws_alpha.sh run --daplab-project <project> --owner <uni> --daplab-account-id <account> [--collapse-after]
#   ./leaderboard/aws_alpha.sh status --daplab-project <project> --owner <uni> --daplab-account-id <account>
#   ./leaderboard/aws_alpha.sh collapse --daplab-project <project> --owner <uni> --daplab-account-id <account>

set -euo pipefail

BOARD_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$BOARD_DIR/.." && pwd)"
cd "$ROOT_DIR"

if [ -x "$ROOT_DIR/.venv/bin/python" ]; then
  PYTHON="$ROOT_DIR/.venv/bin/python"
elif command -v uv >/dev/null 2>&1; then
  exec uv run --frozen python -m leaderboard.aws_alpha "$@"
else
  PYTHON="${PYTHON:-python3}"
fi

exec "$PYTHON" -m leaderboard.aws_alpha "$@"
