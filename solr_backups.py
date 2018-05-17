"""
Performs a backup of every collection. Retries when applicable.
Writes a <backup_name>-manifest.json file to map the attempted backup name
to the successful backup attempts for each collection.

E.g.
{
    "default": "test5-default-0",
    "default_logs": "test5-default_logs-0",
}

Usage:
  solr_backups.py --host <solr_host> --name <backup_name> --path <backup_path> [--manifest <manifest_dir>] [-c <collection>]... [--backup] [--restore]
  solr_backups.py (-h | --help)

Options:
  -h --help                   Show this screen.
  -c=<collection>             Target specific collections.
  --name=<backup_name>        The name of the backup.
  --host=<solr_host>          Solr API URL. E.g. ip-10-20-2-57.us-west-2.compute.internal:8983
  --path=<backup_path>        Path to shared storage for backups.
  --manifest=<manifest_dir>   Where to write the manifest. Default is local to the script.
  --backup                    Default. If present, performs backup.
  --restore                   If present, performs a restore.

"""
from requests import get, post
from termcolor import colored
from time import sleep
import json
import os.path


RETRY_COUNT = 5


def solr_host_string(raw_solr_host):
    """
    Any logic and defaults around the solr host string.
    Allowing it to lack a port, in which case, assume the default.
    :param raw_solr_host: Hostname or IP with or without port. host:port
    :return: host:port
    """
    if ":" not in raw_solr_host:
        solr_host = "{}:8983".format(raw_solr_host)
    else:
        solr_host = raw_solr_host

    return solr_host


def get_collections(solr_host):
    """
    Given a solr host string (host:port), return a list of all collections.
    :param solr_host: host:port
    :return: A list of all collections
    """
    cluster_status = get("http://{}/solr/admin/collections?action=CLUSTERSTATUS&wt=json".format(solr_host)).json()
    collections = cluster_status['cluster']['collections']

    return collections


def backup(solr_target, backup_path, backup_name, collection_targets=None, manifest_dir=None):
    """
    Orchestrate a backup of all solr collections in the target solr deployment.
    :param solr_target: A host string (with or without port, 8983 assumed) to access the solr api.
    :param backup_path: The path on the hosts pointing to shared storage for the backup api.
    :param backup_name: A token to identify this batch of backups.
    :param collection_targets: Optional, override with a list of collections to explicitly backup, instead of all.
    """
    solr_host = solr_host_string(solr_target)
    backup_fmt = "http://"+solr_host+"/solr/admin/collections?action=BACKUP&collection={}&location={}&name={}"
    collections = get_collections(solr_host)
    backup_manifest = {}

    for collection_name, collection_info in collections.iteritems():
        if collection_targets and collection_name not in collection_targets:
            continue

        print(colored(collection_name, "blue"))

        # Painful but in 6.6.x this can fail often and you have to try again on a NEW name, so we're going to have
        # to treat the tokens more like a hash map with linked lists, the most recent backup attempt being
        # the "good" one. This will be put in a manifest file to map the backup name to the actual backup name
        # attempt that worked.
        # https://issues.apache.org/jira/browse/SOLR-11616
        for x in range(RETRY_COUNT):
            collection_backup_name = "{}-{}-{}".format(backup_name, collection_name, str(x))
            backup_command = backup_fmt.format(collection_name, backup_path, collection_backup_name)

            resp = post(backup_command)
            print(colored("Status: {}".format(resp.status_code), "green" if resp.status_code == 200 else "red"))

            if resp.status_code == 200:
                backup_manifest[collection_name] = collection_backup_name
                break
            elif x != RETRY_COUNT-1:
                # Exponential backoff (1, 2, 4, 8, 16, 32, ..) seconds
                sleep(pow(2, x))
            else:
                # We've exhausted all retries... let's just fail early
                print(colored("Failed after max retries.", "red"))
                exit(1)

    manifest_name = '{}-manifest.json'.format(backup_name)
    manifest_path = os.path.join(manifest_dir or "./", manifest_name)
    with open(manifest_path, 'w') as f:
        json.dump(backup_manifest, f, sort_keys=True, indent=4)

    print(colored(json.dumps(backup_manifest, sort_keys=True, indent=4), "green"))

if __name__ == '__main__':
    from docopt import docopt

    try:
        args = docopt(__doc__, argv=None, help=True, version=None, options_first=False)

        collection_targets = args['-c']
        backup_name = args['--name']
        solr_target = args['--host']
        backup_path = args['--path']
        backup_flag = args['--backup']
        restore_flag = args['--restore']
        manifest_dir = args['--manifest'] or None

        if backup_flag or not any((restore_flag,)):
            backup(solr_target, backup_path, backup_name,
                   collection_targets=collection_targets,
                   manifest_dir=manifest_dir)
        else:
            print "** RESTORE NOT IMPLEMENTED **"

    except KeyboardInterrupt:
        print "\nExiting..."
        exit(1)

