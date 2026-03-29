#!/bin/bash

set -e  # exit on error

# ===== CONFIG =====
BACKUP_TYPE="${1:-}"
S3_BUCKET="btrix-crawls-613404686575-ap-southeast-2-an"
DATETIME=$(date +%Y%m%d_%H%M%S)

# ===== VALIDATION =====
if [[ ! "$BACKUP_TYPE" =~ ^[1-3]$ ]]; then
    echo "Usage: $0 <backup_type>"
    echo "  1 = backup db"
    echo "  2 = backup daily tasks"
    echo "  3 = backup wayback files"
    exit 1
fi

# ===== BACKUP FUNCTIONS =====
backup_db() {
    local DB_FILE="/home/ubuntu/iosco/db/state.db"
    echo "Backing up database..."
    aws s3 cp "$DB_FILE" "s3://${S3_BUCKET}/db/state.db.${DATETIME}"
    echo "Database backup completed."
}

backup_directory() {
    local DIR="$1"
    local BACKUP_NAME="$2"
    local S3_PATH="$3"
    
    cd "$DIR"
    echo "Working directory: $DIR"
    
    # Find all directories, sort by modification time (newest first), skip latest 3
    local DIRS_TO_ARCHIVE=()
    while IFS= read -r dir; do
        DIRS_TO_ARCHIVE+=("$dir")
    done < <(ls -dt */ | tail -n +4 | sed 's/\/$//')
    
    if [ ${#DIRS_TO_ARCHIVE[@]} -eq 0 ]; then
        echo "No directories to archive."
        return 0
    fi
    
    echo "Directories to archive:"
    printf '%s\n' "${DIRS_TO_ARCHIVE[@]}"
    
    local ARCHIVE_NAME="${BACKUP_NAME}.${DATETIME}.tar.gz"
    echo "Creating archive: $ARCHIVE_NAME"
    tar -czf "$ARCHIVE_NAME" "${DIRS_TO_ARCHIVE[@]}"
    
    echo "Uploading to S3..."
    aws s3 cp "$ARCHIVE_NAME" "s3://${S3_BUCKET}/${S3_PATH}/"
    
    echo "Removing archived directories..."
    for dir in "${DIRS_TO_ARCHIVE[@]}"; do
        rm -rf "$dir"
    done
    
    rm -f "$ARCHIVE_NAME"
    echo "Backup completed: $BACKUP_NAME"
}

# ===== EXECUTE BACKUP =====
echo "Time: $(date)"
case "$BACKUP_TYPE" in
    1) backup_db ;;
    2) backup_directory "/home/ubuntu/iosco/daily" "daily" "daily" ;;
    3) backup_directory "/home/ubuntu/iosco/wayback" "wayback" "wayback" ;;
esac

echo "Done."
