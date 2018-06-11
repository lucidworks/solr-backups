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
  solr_backups.py --host <solr_host> --name <backup_name> --path <backup_path> [--manifest <manifest_dir>] [-c <collection>]... [--blacklist <collection>]... [--backup] [--restore] [--async-timeout <async_timeout>]
  solr_backups.py (-h | --help)

Options:
  -h --help                             Show this screen.
  -c=<collection>                       Target specific collections.
  --blacklist=<collection>              Black list specific collections.
  --name=<backup_name>                  The name of the backup.
  --host=<solr_host>                    Solr API URL. E.g. ip-10-20-2-57.us-west-2.compute.internal:8983
  --path=<backup_path>                  Path to shared storage for backups.
  --manifest=<manifest_dir>             Where to write the manifest. Default is local to the script.
  --backup                              Default. If present, performs backup.
  --restore                             If present, performs a restore.
  --async-timeout=<async_timeout>       The max time in seconds to try async commands for. Default is 30 minutes.

"""
from requests import get, post
from termcolor import colored
from time import sleep
import json
import os.path
from urlparse import urlparse
from datetime import datetime
import random


RETRY_COUNT = 2
ASYNC_MAX = 10000
DEFAULT_ASYNC_TIMEOUT = 60 * 30 # 30 minutes in seconds


class MaxRetriesExceeded(RuntimeError):
    pass

class NoAvailableAsyncIDs(RuntimeError):
    pass


class SolrDeleteFailed(RuntimeError):
    pass


class SolrAsyncTimedOut(RuntimeError):
    pass


class SolrAsyncJobFailed(RuntimeError):
    pass


def _solr_host_string(raw_solr_host):
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


def _request_status(solr_host, async_id):
    """
    Request status is not RESTful. A get will not return a 404 for a missing item. Etc. Always a 200.
    This navigates those oddities.
    :param solr_host: Host:Port string
    :param async_id: The async id to get the status of.
    :return: Dictionary from the solr api.
        E.g.:
        {
            u'status':
                {
                    u'msg': u'Did not find [1] in any tasks queue',
                    u'state': u'notfound'
                },
            u'responseHeader':
                {
                    u'status': 0,
                    u'QTime': 1
                }
        }
    """
    request_status_fmt = "http://" + solr_host + "/solr/admin/collections?action=REQUESTSTATUS&requestid={}&wt=json"
    request_status_command = request_status_fmt.format(async_id)

    status = get(request_status_command).json()

    return status


def _delete_status(solr_host, request_id=None, flush=False):
    """
    Delete request status. Either by request id, or flush all.
    :param solr_host: the solr host:port string
    :param request_id: specific request id to delete by
    :param flush: whether to flush all
    :return: if delete succeeds, else raise
    """
    delete_status_command = "http://" + solr_host + "/solr/admin/collections?action=DELETESTATUS&wt=json"

    if request_id:
        delete_status_command = "{}&requestid={}".format(delete_status_command, request_id)
    elif flush:
        delete_status_command = "{}&flush={}".format(delete_status_command, str(flush).lower())

    resp = post(delete_status_command)

    if resp.status_code != 200:
        raise SolrDeleteFailed("Solr delete failed. Response: %s", str(resp.status_code))

    return


def randomly(seq):
    shuffled = list(seq)
    random.shuffle(shuffled)
    return iter(shuffled)


def find_an_async_id(solr_host):
    print(colored("Finding an unused async id.", "blue"))

    # pool of ASYNC_MAX ids, they are not cleaned up automatically
    for async_id in randomly(range(ASYNC_MAX)):
        status = _request_status(solr_host, async_id)

        state = status["status"]["state"]
        if state == "notfound":
            print(colored("Success. Using {}.".format(str(async_id)), "green"))
            return async_id
        elif state == "completed":
            print(colored("Found {} for a completed job. Deleting old job.".format(str(async_id)), "blue"))
            _delete_status(solr_host, async_id)
            print(colored("Success. Using {}.".format(str(async_id)), "green"))
            return async_id

    raise NoAvailableAsyncIDs("All available async IDs are unavailable.")


def async(solr_cmd, async_timeout=None):
    """
    Execute via the solr async api instead of directly POSTing.
    :param solr_cmd: The full command you were going to POST. E.g. "http://solr_host:solr_port/solr/admin/collections?action=BACKUP&collection={}&location={}&name={}"
    :return: The success or failure of that async command.
    """
    async_timeout = async_timeout or DEFAULT_ASYNC_TIMEOUT
    parsed_url = urlparse(solr_cmd)
    solr_host = parsed_url.netloc

    async_id = find_an_async_id(solr_host)

    # Start the job of the requested solr command
    async_solr_cmd = "{}&async={}".format(solr_cmd, async_id)
    print(colored("POSTing async job: {}".format(async_solr_cmd), "blue"))

    resp = post(async_solr_cmd)
    if resp.status_code != 200:
        print(colored("Failed to start async job.", "red"))
        print(colored(resp.content, "red"))
        raise SolrAsyncJobFailed("Failed to start async job. %s", resp.content)

    async_job_complete = False
    start_time = datetime.now()
    while not async_job_complete:
        # Check if job complete
        async_info = _request_status(solr_host, async_id)
        state = async_info["status"]["state"]
        elapsed = (datetime.now() - start_time).seconds

        # TODO: Hard fail on failed job
        if state == "completed":
            print(colored(async_info, "green"))
            print(colored("Async job {} completed after {} seconds.".format(str(async_id), str(elapsed)), "green"))
            async_job_complete = True
        elif state == "failed":
            print(colored(async_info, "red"))
            raise SolrAsyncJobFailed("Async job failed during execution.")
        else:
            # Check if too much time has elapsed
            print(colored(async_info, "blue"))
            print(colored("Waiting on Async job {} for {} seconds".format(str(async_id), str(elapsed)), "blue"))

            if elapsed > async_timeout:
                print(colored("Timed out waiting on Async job {} for {} seconds".format(str(async_id), str(elapsed)), "red"))
                raise SolrAsyncTimedOut("Async Job Timed Out: More than the maximum of %s seconds elapsed (%s).",
                                        async_timeout, elapsed)

            sleep(15)


def backup(solr_host, backup_path, backup_name, collection_name,
           async_timeout=None,
           **kwargs):
    """
    Performs a backup of a solr collection.
    :param solr_target: The solr host:port to access the solr collections api.
    :param backup_path:  The path to shared storage to pass on to the collections api.
    :param backup_name: The token identifying this backup. The final backup name will be longer.
    :param collection_name: collection_name
    :return: A string containing the name of the successful backup.
    :raises: MaxRetriesExceeded: If the backup attempts fail more than the maximum allowed.
    """
    backup_fmt = "http://" + solr_host + "/solr/admin/collections?action=BACKUP&collection={}&location={}&name={}"

    # Painful but in 6.6.x this can fail often and you have to try again on a NEW name.
    # This will be put in a manifest file to map the backup name to the actual backup name
    # attempt that worked.
    # https://issues.apache.org/jira/browse/SOLR-11616
    for x in range(RETRY_COUNT):
        print(colored("{} Backup of collection: {}".format("* Re-Trying *" if x > 0 else "Trying", collection_name),
                      "yellow" if x > 0 else "green"))

        collection_backup_name = "{}-{}-{}".format(backup_name, collection_name, str(x))
        backup_command = backup_fmt.format(collection_name, backup_path, collection_backup_name)

        try:
            async(backup_command, async_timeout=async_timeout or DEFAULT_ASYNC_TIMEOUT)
        except SolrAsyncJobFailed:
            # Try again on legit failures. TODO: Remove this retry logic
            print(colored(collection_name, "red"))
        except SolrAsyncTimedOut:
            # Do not try again on a timeout.
            print(colored(collection_name, "red"))
            raise
        else:
            return collection_backup_name

    print(colored("Backup of collection '{}' has failed.".format(collection_name), "red"))
    raise MaxRetriesExceeded("Max retries exceeded backing up collection: %s", collection_name)


def restore(solr_host, backup_path, restore_name, collection_backup_name, original_collection_name,
            async_timeout=None,
            **kwargs):
    """
    Performs a backup of a solr collection.
    :param solr_target: The solr host:port to access the solr collections api.
    :param backup_path:  The path to shared storage to pass on to the collections api.
    :param collection_backup_name: The collection's backup name to use for the restore.
    :param original_collection_name: The original collection_name
    :param restore_name: Similar to backup name in backup, a token to identify this restore.
    :return: A string containing the name of the successful backup.
    :raises: MaxRetriesExceeded: If the backup attempts fail more than the maximum allowed.
    """
    restore_fmt = "http://" + solr_host + "/solr/admin/collections?action=RESTORE&collection={}&location={}&name={}"
    restored_name = "{}-{}".format(original_collection_name, restore_name)
    restore_cmd = restore_fmt.format(restored_name, backup_path, collection_backup_name)

    print(colored("Restoring {} into {}".format(original_collection_name, restored_name), "yellow"))

    try:
        async(restore_cmd, async_timeout=async_timeout or DEFAULT_ASYNC_TIMEOUT)
    except (SolrAsyncJobFailed, SolrAsyncTimedOut):
        print(colored("Restore of collection {} into collection {} has failed.".format(original_collection_name,
                                                                                       restored_name), "red"))
        raise
    else:
        return restored_name


def _read_manifest(manifest_dir, token):
    manifest_name = '{}-manifest.json'.format(token)
    manifest_path = os.path.join(manifest_dir or "./", manifest_name)

    print(colored("Reading manifest: {}".format(manifest_path), "blue"))

    with open(manifest_path, 'r') as f:
        manifest = json.load(f)
        print(colored(json.dumps(manifest, sort_keys=True, indent=4), "green"))

        return manifest


def _write_manifest(manifest_dir, token, manifest):
    manifest_name = '{}-manifest.json'.format(token)
    manifest_path = os.path.join(manifest_dir or "./", manifest_name)

    print(colored("Writing manifest: {}".format(manifest_path), "blue"))

    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, sort_keys=True, indent=4)

        print(colored(json.dumps(manifest, sort_keys=True, indent=4), "green"))


def start(solr_target, backup_path, backup_name,
          collection_targets=None,
          collection_blacklist=None,
          manifest_dir=None,
          action=None,
          async_timeout=None):
    """
    Orchestrate a backup of all solr collections in the target solr deployment.
    :param solr_target: A host string (with or without port, 8983 assumed) to access the solr api.
    :param backup_path: The path on the hosts pointing to shared storage for the backup api.
    :param backup_name: A token to identify this batch of backups.
    :param collection_targets: Optional, override with a list of collections to explicitly backup, instead of all.
    :param collection_blacklist: Optional, a list of collections to ignore in this run.
    :param manifest_dir: Optional, override the manifest directory.
    :param action: Optional, if None then 'backup' is used. If 'backup' a backup is performed. If 'restore' then restore is performed.
    """
    action = action or "backup"
    async_timeout = async_timeout or DEFAULT_ASYNC_TIMEOUT
    solr_host = _solr_host_string(solr_target)
    collections = get_collections(solr_host)
    # Blank slate manifest if backing up. Else we're working off an existing manifest.
    backup_manifest = {} if action == "backup" else _read_manifest(manifest_dir, backup_name)

    for collection_name, collection_info in collections.iteritems():
        if collection_targets and collection_name not in collection_targets:
            # Skip collections not in the explicit list if the explicit list is set
            continue
        elif collection_blacklist and collection_name in collection_blacklist:
            # Skip blacklisted collection
            continue

        print(colored(collection_name, "blue"))

        if action == "backup":
            successful_backup_name = backup(solr_host, backup_path, backup_name, collection_name,
                                            async_timeout=async_timeout)
            backup_manifest[collection_name] = successful_backup_name
        elif action == "restore":
            # TODO: Make this argument list make sense
            successful_restored_name = restore(solr_host,
                                               backup_path,                       # Path to shared disk where all backups are
                                               backup_name,                       # Token passed in for this restore
                                               backup_manifest[collection_name],  # Name of the actual backup on disk
                                               collection_name,                   # Name of the original collection
                                               async_timeout=async_timeout)
            print(colored("{} restored into {}".format(collection_name, successful_restored_name), "green"))

    if action == "backup":
        _write_manifest(manifest_dir, backup_name, backup_manifest)

if __name__ == '__main__':
    from docopt import docopt

    try:
        args = docopt(__doc__, argv=None, help=True, version=None, options_first=False)

        collection_targets = args['-c']
        collection_blacklist = args['--blacklist']
        backup_name = args['--name']
        solr_target = args['--host']
        backup_path = args['--path']
        backup_flag = args['--backup']
        restore_flag = args['--restore']
        manifest_dir = args['--manifest'] or None
        async_timeout = args['--async-timeout'] or DEFAULT_ASYNC_TIMEOUT

        action = "backup"

        if backup_flag or not any((restore_flag,)):
            action = "backup"
        elif restore_flag:
            action = "restore"

        start(solr_target, backup_path, backup_name,
              collection_targets=collection_targets,
              collection_blacklist=collection_blacklist,
              manifest_dir=manifest_dir,
              action=action,
              async_timeout=async_timeout)


    except KeyboardInterrupt:
        print "\nExiting..."
        exit(1)

