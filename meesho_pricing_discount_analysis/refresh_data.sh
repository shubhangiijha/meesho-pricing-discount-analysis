#!/usr/bin/env bash
set -euo pipefail
# Refresh synthetic data and rebuild local SQLite DB for the app.
# Usage:
#   ./refresh_data.sh
# Add to crontab (runs daily at 02:30):
#   30 2 * * * /bin/bash -lc "cd /path/to/meesho_pricing_discount_analysis && ./refresh_data.sh >> refresh.log 2>&1"

python3 data_gen.py
echo "[$(date '+%F %T')] Refreshed data (CSV + SQLite)"
