#! /usr/bin/env python


# dependencies to handle:
# - gevent
# - pymongo
# - apt: python-dev
# - apt: libevent-dev

import copier
from copy_state_db import CopyStateDB
import multiprocessing
import oplog_applier
import os
import os.path
from pymongo.read_preferences import ReadPreference
import string
import sys
import utils

log = utils.get_logger(__name__)

PARENT_PROCESS_NAME = 'parent process'

#
# child processes
#

def die(msg):
    log.error(msg)
    sys.exit(1)


def ensure_empty_dest(dest):
    client = utils.mongo_connect(dest['host'], dest['port'],
                                 ensure_direct=True,
                                 max_pool_size=1,
                                 read_preference=ReadPreference.PRIMARY)
    collection = client[dest['db']][dest['collection']]
    if collection.count() > 0:
        die("destination must be empty!")


def copy_collection_parent(sources, dest, state_db, args):
    """
    drive the collection copying process by delegating work to a pool of worker processes
    """

    # ensure state db has rows for each source/dest pair
    for source in sources:
        state_db.add_source_and_dest(source, dest)

    # space-pad all process names so that tabular output formats line up
    process_names = {repr(source): "%s:%d" % (source['host'], source['port'])
                     for source in sources}
    process_names['parent'] = PARENT_PROCESS_NAME
    max_process_name_len = max(len(name) for name in process_names.itervalues())
    for key in process_names:
        process_names[key] = string.ljust(process_names[key], max_process_name_len)

    multiprocessing.current_process().name = process_names['parent']

    # -----------------------------------------------------------------------
    # perform initial copy, if it hasn't been done yet
    # -----------------------------------------------------------------------
    in_initial_copy = len(state_db.select_by_state(CopyStateDB.STATE_INITIAL_COPY))
    if in_initial_copy and in_initial_copy < len(sources):
        die("prior attempt at initial copy failed; rerun with --restart")
    if in_initial_copy > 0:
        ensure_empty_dest(dest)

        # each worker process copies one shard
        processes = []
        for source in sources:
            name = process_names[repr(source)]
            process = multiprocessing.Process(target=copier.copy_collection,
                                              name=name,
                                              kwargs=dict(source=source,
                                                          dest=dest,
                                                          state_path=state_db._path,
                                                          percent=args.percent))
            process.start()
            processes.append(process)


        # wait for all workers to finish
        utils.wait_for_processes(processes)

    # -----------------------------------------------------------------------
    # build indices on main process, since that only needs to be done once
    # -----------------------------------------------------------------------
    waiting_for_indices = len(state_db.select_by_state(CopyStateDB.STATE_WAITING_FOR_INDICES))
    if waiting_for_indices and waiting_for_indices < len(sources):
        die("not all initial copies have been completed; rerun with --restart")
    if waiting_for_indices > 0:
        log.info("building indices")
        copier.copy_indexes(sources[0], dest)
        for source in sources:
            state_db.update_state(source, dest, CopyStateDB.STATE_APPLYING_OPLOG)

    # -----------------------------------------------------------------------
    # apply oplogs
    # -----------------------------------------------------------------------
    applying_oplog = state_db.select_by_state(CopyStateDB.STATE_APPLYING_OPLOG)
    if len(applying_oplog) < len(sources):
        die("this shouldn't happen!")

    log.info("starting oplog apply")

    # create worker thread that prints headers for oplog stats on a regular basis;
    # we do this to prevent the visual clutter caused by multiple processes doing this
    #
    # we avoid using gevent in the parent process to avoid weirdness I've seen with fork()ed
    # gevent loops
    header_delay = max(float(20) / len(sources),10) 
    stats_name = string.ljust("stats", max_process_name_len)
    stats_proc = multiprocessing.Process(target=oplog_applier.print_header_worker,
                                         args=(header_delay,),
                                         name=stats_name)
    stats_proc.start()

    # need to isolate calls to gevent here, to avoid forking with monkey-patched modules
    # (which seems to create funkiness)
    processes = []
    for source in sources:
        name = process_names[repr(source)]
        process = multiprocessing.Process(target=oplog_applier.apply_oplog,
                                          name=name,
                                          kwargs=dict(source=source,
                                                      dest=dest,
                                                      percent=args.percent,
                                                      state_path=state_db._path))
        process.start()
        processes.append(process)

    # this should *never* finish
    processes.append(stats_proc)
    utils.wait_for_processes(processes)


if __name__ == '__main__':
    # NOTE: we are not gevent monkey-patched here; only child processes are monkey-patched,
    #       so all ops below are synchronous

    # parse command-line options
    import argparse
    parser = argparse.ArgumentParser(description='Copies a collection from one mongod to another.')
    parser.add_argument(
        '--source', type=str, required=True, metavar='URL',
        help='source to read from; can be a file containing sources or a url like: host[:port]/db/collection; '
             'e.g. localhost:27017/prod_maestro.emails')
    parser.add_argument(
        '--dest', type=str, required=True, metavar='URL',
        help='source to read from; see --source for format')
    parser.add_argument(
        '--percent', type=int, metavar='PCT', default=None,
        help='copy only PCT%% of data')
    parser.add_argument(
        '--restart', action='store_true',
        help='restart from the beginning, ignoring any prior progress')
    parser.add_argument(
        '--state-db', type=str, metavar='PATH', default=None,
        help='path to state file (defaults to ./<source_database>.<source_collection>.db)')
    args = parser.parse_args()

    # parse source and destination
    dest = utils.parse_mongo_url(args.dest)
    if os.path.exists(args.source):
        sources = utils.parse_source_file(args.source)
    else:
        sources = [utils.parse_mongo_url(args.source)]

    # initialize sqlite database that holds our state (this may seem like overkill,
    # but it's actually needed to ensure proper synchronization of subprocesses)
    if not args.state_db:
        args.state_db = '%s.%s.db' % (sources[0]['db'], sources[0]['collection'])

    if args.state_db.startswith('/'):
        state_db_path = args.state_db
    else:
        state_db_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                     args.state_db)

    log.info('using state db %s' % state_db_path)
    state_db_exists = os.path.exists(state_db_path)
    state_db = CopyStateDB(state_db_path)
    if not state_db_exists:
        state_db.drop_and_create()

    if args.restart:
        state_db.drop_and_create()

    # do the real work
    copy_collection_parent(sources, dest, state_db, args)

    log.error("shouldn't reach this point")
    sys.exit(1)
