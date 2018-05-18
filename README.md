# Solr Backup/Restore Helper

## Docker Example Build
```
docker build -t solr_backups .
```

## Docker Example Run
```
docker run -it  \
  -e SOLR_HOST="ip-10-20-2-57.us-west-2.compute.internal:8983" \
  -e BACKUP_NAME="backup-test" \
  -e BACKUP_PATH="/opt/fusion-backups/solr" \
  -e MANIFEST_DIR="/manifests" \
  -v "$(pwd)"/manifests:/manifests \
  solr_backups \
  "--backup --blacklist logs"
```

## Script Help
```
➜ python solr_backups.py --help

Performs a backup of every collection. Retries when applicable.
Writes a <backup_name>-manifest.json file to map the attempted backup name
to the successful backup attempts for each collection.

E.g.
{
    "default": "test5-default-0",
    "default_logs": "test5-default_logs-0",
}

Usage:
  solr_backups.py --host <solr_host> --name <backup_name> --path <backup_path> [--manifest <manifest_dir>] [-c <collection>]... [--blacklist <collection>]... [--backup] [--restore]
  solr_backups.py (-h | --help)

Options:
  -h --help                   Show this screen.
  -c=<collection>             Target specific collections.
  --blacklist=<collection>    Black list specific collections.
  --name=<backup_name>        The name of the backup.
  --host=<solr_host>          Solr API URL. E.g. ip-10-20-2-57.us-west-2.compute.internal:8983
  --path=<backup_path>        Path to shared storage for backups.
  --manifest=<manifest_dir>   Where to write the manifest. Default is local to the script.
  --backup                    Default. If present, performs backup.
  --restore                   If present, performs a restore.
```
