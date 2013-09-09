#! /usr/bin/env python

import pymongo
from pymongo.read_preferences import ReadPreference
import sys
import time
import utils

shard_clients = {}


def syntax():
    print >>sys.stderr, "syntax:  %s mongos_host[:port]" % sys.argv[0]
    print >>sys.stderr, "purpose: monitor cluster for changes that might affect copy_collection.py"


def get_cluster_state(mongos):
    """
    returns a dictionary that contains the subset of cluster state we care about
    """
    global shard_clients

    # this won't work well with a large (thousands?) number of shards
    shards_collection = client['config']['shards']
    shards = [shard for shard in shards_collection.find()]

    state = {}
    state['shard_names'] = [shard['_id'] for shard in shards]
    state['shard_names'].sort()

    members = {}
    oplog_positions = {}
    for shard in shards:
        # get statuses for all replica set members
        try:
            repl_set, host = shard['host'].split('/')
        except ValueError:
            print >>sys.stderr, "ERROR: can't get replica set status for %s" % shard['_id']
            sys.exit(1)

        # get cached connection, if one exists
        if repl_set in shard_clients:
            shard_client = shard_clients[repl_set]
        else:
            shard_client = pymongo.MongoClient(host, replicaSet=repl_set,
                                               read_preference=ReadPreference.PRIMARY,
                                               socketTimeoutMS=120000)
            shard_clients[repl_set] = shard_client

        rs_status = shard_client.admin.command('replSetGetStatus')
        for member in rs_status['members']:
            members[member['name']] = member['stateStr']

        # get last oplog positions
        last_oplog_entry = utils.get_last_oplog_entry(shard_client)
        oplog_positions[repl_set] = last_oplog_entry['ts']


    state['members'] = members
    state['oplog_positions'] = oplog_positions

    return state


if __name__ == '__main__':
    errors = 0

    # parse command-line parameters
    log = utils.get_logger('cluster_cop')
    if len(sys.argv) != 2:
        syntax()
        sys.exit(1)

    host_tokens = sys.argv[1].split(':')
    if len(host_tokens) == 2:
        host, port = host_tokens
        port = int(port)
    else:
        host = host_tokens[0]
        port = 27017

    # connect to mongo
    log.info("connecting to %s:%d", host, port)
    client = pymongo.MongoClient(host, port, max_pool_size=1)
    shards_collection = client['config']['shards']
    log.info("connected")

    # take initial snapshot of cluster state
    prev_state = get_cluster_state(client)
    while True:
        time.sleep(10)
        curr_state = get_cluster_state(client)

        # ensure balancer is off
        settings = client['config']['settings']
        balancer_setting = settings.find_one({'_id': 'balancer'})
        if not balancer_setting['stopped']:
            log.error("chunk balancer is ON; this can be catastrophic!")
            sys.exit(1)

        # ensure primaries stay primaries and secondaries stay secondaries
        if prev_state['members'] != curr_state['members']:
            errors += 1
            log.error("previous member state (%r) doesn't match current state(%r)",
                      prev_state, curr_state)

        # figure out most recent op
        latest_ts = None
        latest_repl_set = None
        for repl_set, op_ts in curr_state['oplog_positions'].iteritems():
            if (not latest_ts or latest_ts.time < op_ts.time or 
                (latest_ts.time == op_ts.time and latest_ts.inc < op_ts.inc)):
                latest_ts = op_ts
                latest_repl_set = repl_set

        secs_ago = int(time.time() - latest_ts.time)
        log.info("%d errors | last op was %d secs ago on %s", errors, secs_ago, latest_repl_set)

        prev_state = curr_state