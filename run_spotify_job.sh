#!/bin/bash

PROJECT_DIR="/Users/josephinebhadran/Documents/GitHub/my-spotify-wrapped"
PYTHON="$PROJECT_DIR/myenv/bin/python"
LOGFILE="$PROJECT_DIR/cron.log"

cd "$PROJECT_DIR" || exit 1

echo "==== $(date) ====" >> "$LOGFILE"

$PYTHON job.py >> "$LOGFILE" 2>&1
