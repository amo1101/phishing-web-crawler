#!/usr/bin/env bash
set -euo pipefail

# ---- Usage & arg parsing ----
if [[ $# -lt 4 ]]; then
  echo "Usage: $0 <URL> <CONCURRENCY> <OUTPUT_DIR> <JOB_NAME>" >&2
  exit 2
fi

URL="$1"
CONCURRENCY="$2"
OUTPUT_DIR="$3"
JOB_NAME="$4"

DOWNLOAD="/usr/local/bin/wayback_machine_downloader"

# ---- Pre-flight checks ----
# Ensure downloader exists and is executable
if [[ ! -x "$DOWNLOAD" ]]; then
  echo "ERROR: Downloader not found or not executable at: $DOWNLOAD" >&2
  exit 3
fi

# Simple numeric validation for concurrency (positive integer)
if ! [[ "$CONCURRENCY" =~ ^[1-9][0-9]*$ ]]; then
  echo "ERROR: CONCURRENCY must be a positive integer, got: '$CONCURRENCY'" >&2
  exit 4
fi

# Create output/log directories
mkdir -p "$OUTPUT_DIR/log"

LOG_FILE="$OUTPUT_DIR/log/$JOB_NAME.log"

# Optionally avoid Gemfile-induced conflicts:
# export RUBYGEMS_GEMDEPS=-

echo "[$(date -Iseconds)] Start downloading $URL from Wayback Machine..." | tee -a "$LOG_FILE"

# ---- Run downloader ----
# Capture exit status; write stdout+stderr to the log
set +e
"$DOWNLOAD" \
  "$URL" \
  --directory "$OUTPUT_DIR" \
  --concurrency "$CONCURRENCY" \
  >> "$LOG_FILE" 2>&1
CMD_EXIT=$?
set -e

echo "[$(date -Iseconds)] Finish downloading $URL from Wayback Machine (exit=$CMD_EXIT)" | tee -a "$LOG_FILE"

# ---- Determine status ----
STATUS="FAILED"

if grep -Eiq 'Download finished' "$LOG_FILE"; then
    STATUS="FINISHED"
fi

# ---- Count files found ----
# Look for a count like: "15 files found matching criteria"
# Portable approach with awk; fallback to 0 if not found
FILES_DOWNLOADED=$(awk '
  BEGIN { n=0 }
  /[0-9]+[[:space:]]+files[[:space:]]+found[[:space:]]+matching[[:space:]]+criteria/ {
    for (i=1; i<=NF; i++) {
      if ($i ~ /^[0-9]+$/) { n=$i; break }
    }
  }
  END { print n }
' "$LOG_FILE")

# ---- Output result ----
echo "$JOB_NAME:$STATUS,$FILES_DOWNLOADED"