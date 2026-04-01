#!/bin/bash

BASE_PATH="/home/ubuntu"
SCRIPT_PATH="$BASE_PATH/phishing-web-crawler/daily-tasks"
DATA_PATH="$BASE_PATH/iosco/daily"
LOG_FILE="$BASE_PATH/iosco/log/cron.log"

# Install crontab jobs
(crontab -l 2>/dev/null; cat << EOF
0 9 * * * $SCRIPT_PATH/autorun.sh 0 $DATA_PATH > $LOG_FILE
0 10 * * * $SCRIPT_PATH/autorun.sh 1 $DATA_PATH > $LOG_FILE
0 12 * * * $SCRIPT_PATH/autorun.sh 2 $DATA_PATH > $LOG_FILE
0 21 * * 1 $SCRIPT_PATH/backup.sh 2 > $LOG_FILE
0 */6 * * * $SCRIPT_PATH/backup.sh 3 > $LOG_FILE
0 23 * * 1 $SCRIPT_PATH/backup.sh 1 > $LOG_FILE
EOF
) | crontab -
echo "Crontab jobs installed successfully."