import bson
from copy_state_db import CopyStateDB
import gevent
from faster_ordered_dict import FasterOrderedDict
from gevent.pool import Pool
import pymongo
from pymongo.errors import DuplicateKeyError
from pymongo.read_preferences import ReadPreference
import time
import utils
from utils import auto_retry, log_exceptions, squelch_keyboard_interrupt

log = utils.get_logger(__name__)

TS_REWIND = 30 # seconds
HEADER_INTERVAL = 15 # entries

#
# Apply oplogs
#

class ApplyStats(object):
    def __init__(self):
        self.ops_retrieved = 0
        self.inserts = 0
        self.insert_warnings = 0
        self.deletes = 0
        self.delete_warnings = 0
        self.updates = 0
        self.update_warnings = 0
        self.last_ts = bson.Timestamp(int(time.time()), 0)
        self.sleeps = 0
        self.exceptions = 0
        self.retries = 0

        self.paused = False
        self.pending_ids = set()

    def log(self):
        # we record warnings but don't print them, because they haven't been that useful
        #
        # that said, we track them just in case 
        lag = int(time.time() - self.last_ts.time)
        log.info(FMT, self.ops_retrieved, lag, 
                 self.inserts, self.deletes, self.updates,
                 self.sleeps, self.exceptions, self.retries)

SH1 = "OPS APPLIED                                    | WARNINGS"
SH2 = "total     lag    inserts   removes   updates   | sleeps    exceptions retries"
FMT = "%-9d %-6d %-9d %-9d %-9d | %-9d %-10d %d"

def _op_id(op):
    if op['op'] == 'u':
        return op['o2']['_id']
    else:
        return op['o']['_id']

def print_header_worker(sleep_interval):
    while True:
        log.info(SH1)
        log.info(SH2)
        time.sleep(sleep_interval)


@log_exceptions
def oplog_stats_worker(stats):
    """
    Greenlet for printing state for oplog applies
    """
    while True:
        if not stats.paused:
            stats.log()
        gevent.sleep(3)


def oplog_checkpoint_worker(stats, source, dest, state_db):
    """
    Greenlet for persisting oplog position to disk. This only has to do work periodically,
    because it's ok if the recorded position is behind the position of the last applied 
    op. Oplog entries are idempotent, so we don't worry about applying an op twice.
    """
    while True:
        state_db.update_oplog_ts(source, dest, stats.last_ts)
        gevent.sleep(3)


@auto_retry
def _apply_op(op, source_collection, dest_collection, stats):
    """
    Actually applies an op. Assumes that we are the only one mutating a document with
    the _id referenced by the op.
    """
    _id = _op_id(op)
    if op['op'] == 'i':
        # insert
        try:
            inserted_id = dest_collection.insert(op['o'])
            if inserted_id:
                if inserted_id != _id:
                    raise SystemError("inserted _id doesn't match given _id") 
                stats.inserts += 1 
            else:
                stats.insert_warnings += 1
        except DuplicateKeyError:
            stats.insert_warnings += 1
    elif op['op'] == 'd':
        # delete
        result = dest_collection.remove({'_id': _id})
        if result:
            if result['n'] == 1:
                # success
                stats.deletes += 1
            else:
                # we're deleting by _id, so we should have deleted exactly one document;
                # anything else is a warning
                #log.debug("warn delete _id = %s; result = %r", base64.b64encode(_id), result)
                stats.delete_warnings += 1
                if result.get('err', None):
                    log.error("error while deleting: %r" % op['err'])
    elif op['op'] == 'u':
        # update. which involves re-reading the document from the source and updating the
        # destination with the updated contents
        doc = source_collection.find_one({'_id': _id}, slave_okay=True)
        if not doc:
            # document not found (might have been deleted in a subsequent oplog entry)
            stats.update_warnings += 1
            return
        stats.updates += 1
        dest_collection.save(doc)
    else:
        raise TypeError("unknown op type %s" % op['op'])


def _apply_op_worker(op, source_collection, dest_collection, stats):
    """
    Applies an op. Meant to be run as part of a greenlet.

    @param op                 op we're applying
    @param source_collection  collection we're reading from
    @param dest_collection    collection we're writing to
    @param stats              an ApplyStats object
    """
    _id = _op_id(op)

    # apply the op, ensuring that all ops on this _id execute serially
    try:
        _apply_op(op, source_collection, dest_collection, stats)
    finally:
        stats.pending_ids.remove(_id)


@log_exceptions
@squelch_keyboard_interrupt
def apply_oplog(source, dest, percent, state_path):
    """
    Applies oplog entries from source to destination. Since the oplog storage format
    has known and possibly unknown idiosyncracies, we take a conservative approach. For
    each insert or delete op, we can easily replay those. For updates, we do the following:

    1. Note the _id of the updated document
    2. Retrieved the updated document from the source
    3. Upsert the updated document in the destination

    @param oplog              oplog collection from the source mongod instance
    @param start_ts           timestamp at which we should start replaying oplog entries
    @param source_collection  collection we're reading from
    @param dest_collection    collection we're writing to
    @param checkpoint_ts_func function that, when called, persists oplog timestamp to disk
    @param 
    """
    gevent.monkey.patch_socket()

    stats = ApplyStats()
    apply_workers = Pool(20) 

    # connect to state db
    state_db = CopyStateDB(state_path)

    # connect to mongo
    source_client = utils.mongo_connect(source['host'], source['port'],
                                        ensure_direct=True,
                                        max_pool_size=30,
                                        read_preference=ReadPreference.SECONDARY,
                                        document_class=FasterOrderedDict)
    source_collection = source_client[source['db']][source['collection']]

    dest_client = utils.mongo_connect(dest['host'], dest['port'],
                                      max_pool_size=30,
                                      document_class=FasterOrderedDict)
    dest_collection = dest_client[dest['db']][dest['collection']] 
    oplog = source_client['local']['oplog.rs']

    # print stats periodically
    stats.paused = True
    stats_greenlet = gevent.spawn(oplog_stats_worker, stats)

    # checkpoint oplog position to disk periodically
    checkpoint_greenlet = gevent.spawn(oplog_checkpoint_worker, stats, source, dest, state_db)

    # figure out where we need to start reading oplog entries; rewind our oplog timestamp
    # a bit, to avoid issues with the user pressing Control-C while some ops are pending
    #
    # this works, because oplog entries are idempotent
    start_ts_orig = state_db.get_oplog_ts(source, dest)
    start_ts = bson.Timestamp(time=start_ts_orig.time-TS_REWIND, inc=0)
    log.info("starting apply at %s", start_ts)

    # perform tailing oplog query using the oplog_replay option to efficiently find
    # our starting position in the oplog
    query = {}
    query['ts'] = {'$gte': start_ts}
    query['ns'] = source_collection.full_name 
    cursor = oplog.find(query, timeout=False, tailable=True, slave_okay=True, await_data=True)
    cursor.add_option(pymongo.cursor._QUERY_OPTIONS['oplog_replay'])
    while True:
        for op in cursor:
            stats.paused = False

            _id = _op_id(op)
            if percent and not utils.id_in_subset(_id, percent):
                continue

            stats.ops_retrieved += 1

            # block *all* further ops from being applied if there's a pending
            # op on the current _id, to ensure serialization
            while _id in stats.pending_ids:
                gevent.sleep(0.1)
                stats.sleeps += 1

            # do the real oplog work in a greenlet from the pool
            stats.pending_ids.add(_id)
            apply_workers.spawn(_apply_op_worker,
                                op,
                                source_collection,
                                dest_collection,
                                stats)

            # update our last timestamp; this is *not* guaranteed to be the timestamp of the
            # most recent op, which is impossible because of our out-of-order execution
            #
            # this is an approximation that needs to be accurate to within TS_REWIND seconds
            stats.last_ts = op['ts']

        # while we have a tailable cursor, it can stop iteration if no more results come back
        # in a reasonable time, so sleep for a bit then try to continue iteration
        if cursor.alive:
            log.debug("replayed all oplog entries; sleeping...")
            stats.paused = True
            gevent.sleep(2)
            stats.paused = False
        else:
            log.error("cursor died on us!")
            break

    # just to silence pyflakes...
    stats_greenlet.kill()
    checkpoint_greenlet.kill()