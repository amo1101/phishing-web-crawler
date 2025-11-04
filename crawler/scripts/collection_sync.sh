#!/usr/bin/env bash
set -Eeuo pipefail

# ---- CONFIG ----
HERITRIX_WARCS_ROOT="/Data/scratch/cw486/phishingwebarchive/heritrix-3.10.2/jobs"

# Glob(s) for WARC files; add more patterns if needed
# e.g., many installs write WARCs under each job run directory
WARCS_GLOBS=(
  "$HERITRIX_WARCS_ROOT/*/*/*/*.warc.gz"
)

# pywb collection paths
COLL_NAME="fma"
COLL_ROOT="/Data/scratch/cw486/phishingwebarchive/collections/${COLL_NAME}"
ARCHIVE_DIR="${COLL_ROOT}/archive"
INDEX_DIR="${COLL_ROOT}/indexes"

# Choose the indexer:
#   Option A: wb-manager index (uses pywb's manager; pywb notices changes live)
#   Option B: cdxj-indexer (external, fast; write .cdxj per WARC)
# Pick one by setting INDEXER="wb-manager" or "cdxj-indexer"
INDEXER="${INDEXER:-wb-manager}"

# Performance niceness to reduce impact on live replay
NICE="nice -n 10"
IONICE="ionice -c2 -n7"

# ---- FUNCTIONS ----
log() { printf '[%(%F %T)T] %s\n' -1 "$*"; }

ensure_dirs() {
  mkdir -p "$ARCHIVE_DIR" "$INDEX_DIR"
}

# Return symlink path under archive/ for a given WARC file
link_path_for_warc() {
  local warc="$1"
  local base
  base="$(basename "$warc")"
  printf '%s/%s' "$ARCHIVE_DIR" "$base"
}

# Check if an index file for this WARC already exists
index_exists_for_warc() {
  local warc="$1"
  local base
  base="$(basename "$warc")"
  local cdxj="${INDEX_DIR}/${base}.cdxj"
  [[ -s "$cdxj" ]]
}

index_with_wb_manager() {
  local symlink="$1"
  # Index only this WARC symlink; wb-manager writes/updates CDXJ in indexes/
  # and pywb can detect this without restart
  $NICE $IONICE wb-manager index "$COLL_NAME" "$symlink"
}

index_with_cdxj_indexer() {
  local warc="$1"
  local base tmp out
  base="$(basename "$warc")"
  tmp="${INDEX_DIR}/.${base}.cdxj.tmp"
  out="${INDEX_DIR}/${base}.cdxj"

  # Generate index to a temp file, then atomically move into place
  $NICE $IONICE cdxj-indexer "$warc" > "$tmp"
  mv -f "$tmp" "$out"
}

process_warc() {
  local warc="$1"

  # 1) Symlink, no copy
  local link
  link="$(link_path_for_warc "$warc")"
  if [[ ! -L "$link" ]]; then
    ln -sfn "$warc" "$link"
    log "Linked: $link -> $warc"
  fi

  # 2) Index if not present (atomic)
  if index_exists_for_warc "$warc"; then
    log "Index exists for $(basename "$warc"), skipping."
  else
    log "Indexing $(basename "$warc") with $INDEXER..."
    if [[ "$INDEXER" == "wb-manager" ]]; then
      index_with_wb_manager "$link"
    else
      index_with_cdxj_indexer "$warc"
    fi
    log "Indexed $(basename "$warc")."
  fi
}

main() {
  ensure_dirs

  # Find candidate WARCs
  shopt -s nullglob
  local wa
  for g in "${WARCS_GLOBS[@]}"; do
    for wa in $g; do
      # Only file types we expect
      case "$wa" in
        *.warc|*.warc.gz) process_warc "$wa" ;;
      esac
    done
  done
}

main "$@"

