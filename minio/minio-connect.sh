#!/bin/bash

# 1. Locate the .env file relative to the script location
ENV_FILE="$(dirname "$0")/.env"
LOCAL_NAME="local-minio"

if [ -f "$ENV_FILE" ]; then
    # 2. Load variables (ignoring comments)
    export $(grep -v '^#' "$ENV_FILE" | xargs)
    
    # 3. Set the alias (using a project-specific name to avoid conflicts)
    # We use 'local-minio' so it doesn't overwrite your global aliases
    mcli alias set "$LOCAL_NAME" "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" > /dev/null 2>&1
else
    echo "Error: .env file not found at $ENV_FILE"
    exit 1
fi

# 4. Pass all arguments to the mcli command, targeting the local alias
if [ $# -eq 0 ]; then
    mcli ls "$LOCAL_NAME"
else
    mcli "$@"
fi
