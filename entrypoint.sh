#!/bin/bash
set -e

if [[ "$1" = *'--backup'* ]] || [[ "$1" = *'--restore'* ]]; then
    echo "python solr_backups.py --host $SOLR_HOST --name $BACKUP_NAME --path $BACKUP_PATH --manifest $MANIFEST_DIR $@"
    exec python solr_backups.py --host "$SOLR_HOST" --name "$BACKUP_NAME" --path "$BACKUP_PATH" --manifest "$MANIFEST_DIR" $@
fi

exec "$@"