#!/usr/bin/env bash
# Deletes every billable PortPulse AWS resource this project's deploy
# scripts could have created. Safe to run at any point -- every check
# is idempotent (no-op if the resource doesn't exist).
#
# Usage:
#   ./teardown.sh                                    # dry run: lists what would be deleted
#   ./teardown.sh --confirm                           # actually deletes it
#   ./teardown.sh --confirm --delete-bucket --bucket-name portpulse-raw-<account-id>
#
# See infra/teardown.py for the real logic; this is just the entry point
# named per the project brief.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -d ".venv" ]; then
  source .venv/bin/activate
fi

python -m infra.teardown "$@"
