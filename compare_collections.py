#! /usr/bin/env python
import base64
from bson import Binary
import gevent
import gevent.monkey
import gevent.pool
import json
import multiprocessing
from multiprocessing import Process
import os.path
from oplog_applier import _op_id
import pymongo
import random
import time
import utils
from utils import squelch_keyboard_interrupt

log = utils.get_logger(__name__)

POOL_SIZE = 20
READ_SIZE = 100 # documents


class CompareStats(object):
    def __init__(self):
        self.total_docs = None
        self.compared = 0
        self.retries = 0
        self.mismatches = 0
        self.start_time = time.time()


    def log(self):
        pct = int(float(self.compared) / self.total_docs * 100.0)
        qps = int(float(self.compared) / (time.time() - self.start_time))
        log.info("%d%% | %d / %d compared | %s/sec | %d retries | %d mismatches" %
                 (pct, self.compared, self.total_docs, qps, self.retries, self.mismatches))


class MismatchLogger(object):
    _mismatches_file = None
    collection_name = None

    @classmethod
    def log_mismatch(cls, doc, _id):
        if not cls._mismatches_file:
            proc_name = multiprocessing.current_process().name
            proc_name = proc_name.replace(':', '_')
            if cls.collection_name:
                filename = '%s_mismatches.txt' % cls.collection_name
            else:
                filename = 'mismatches.txt' 
            cls._mismatches_file = open(filename, 'a', 0)
        entry = {'_id': base64.b64encode(_id)}
        cls._mismatches_file.write('%s\n' % json.dumps(entry))


    @classmethod
    def decode_mismatch_id(cls, _json):
        doc = json.loads(_json)
        return Binary(base64.b64decode(doc['_id']), 0)


def _stats_worker(stats):
    while True:
        stats.log()
        gevent.sleep(1)


def _retry_id_worker(_id, source_collection, dest_collection, retries, retry_delay, stats):
    """
    Compares the source and destination's versions of the document with the given _id.
    For each data inconsistency, we retry the fetches and compare a fixed number of times
    to check if the data is eventually consistent. We perform an exponential backoff between
    each retry.

    @param _id                 _id of document to compare
    @param source_collection   source for data
    @param dest_collection     copied data to verify
    @param retries             number of times to retry comparison
    @param retry_delay         how many seconds to wait between retries (used as a starting point)
    @param stats               instance of CompareStats
    """
    backoff_factor = random.uniform(1.2, 1.4)

    for i in xrange(retries):
        # back off aggressively on successive retries, to reduce chances of false mismatches
        gevent.sleep(retry_delay * backoff_factor**(i+1))
        stats.retries += 1

        source_doc = source_collection.find_one({'_id': _id})
        dest_doc = dest_collection.find_one({'_id': _id})

        # doc was deleted from both places -- great
        if source_doc is None and dest_doc is None:
            stats.compared += 1
            return

        # docs match! we're done
        if source_doc == dest_doc:
            stats.compared += 1
            return

    # we've exhausted our retries, bail...
    stats.compared += 1
    stats.mismatches += 1
    id_base64 = base64.b64encode(_id)
    log.error("MISMATCH: _id = %s", id_base64)
    MismatchLogger.log_mismatch(source_doc, _id)


def _compare_ids_worker(_ids, source_collection, dest_collection, stats, retry_pool):
    """
    compare a set of ids between source and destination, creating new greenlets if needed
    to handle retries

    @param source_collection   source for data
    @param dest_collection     copied data to verify
    @param retries             number of times to retry comparison
    @param retry_delay         how many seconds to wait between retries
    @param retry_pool          pool of greenlets with which we can retry comparisons of
                               mismatched documents
    """
    # read docs in
    source_docs = [doc for doc in source_collection.find({'_id': {'$in': _ids}})]
    source_docs_dict = {doc['_id']: doc for doc in source_docs}

    dest_docs = [doc for doc in dest_collection.find({'_id': {'$in': _ids}})]
    dest_docs_dict = {doc['_id']: doc for doc in dest_docs}

    # find mismatching docs
    for _id in _ids:
        source_doc = source_docs_dict.get(_id, None)
        dest_doc = dest_docs_dict.get(_id, None)

        # doc was deleted from both places -- ok
        if source_doc is None and dest_doc is None:
            stats.compared += 1
            continue

        # docs match! we're done
        if source_doc == dest_doc:
            stats.compared += 1
            continue

        # docs don't match, so spawn a separate greenlet to handle the retries for this
        # particular _id
        retry_pool.spawn(_retry_id_worker,
                         _id=_id,
                         source_collection=source_collection,
                         dest_collection=dest_collection,
                         retries=10,
                         retry_delay=1.0,
                         stats=stats)


def _get_all_ids(collection):
    """
    generator that yields every _id in the given collection
    """
    cursor = collection.find(fields=['_id'], timeout=False, snapshot=True)
    cursor.batch_size(5000)
    for doc in cursor:
        yield doc['_id']


def _get_ids_for_recent_ops(client, recent_ops):
    """
    generator that yields the _id's that were touched by recent ops
    """
    oplog = client['local']['oplog.rs']
    fields = ['o._id', 'o2._id', 'op']
    cursor = oplog.find(fields=fields)
    cursor.limit(recent_ops)
    cursor.sort("$natural", pymongo.DESCENDING)
    cursor.batch_size(100)
    for op in cursor:
        yield _op_id(op)


def _get_ids_in_file(filename):
    """
    generator that yields the number of lines in the file, then each document containing
    the _id to read
    """
    with open(filename, 'r') as ids_file:
        lines = ids_file.readlines()
    yield len(lines)
    for line in lines:
        yield MismatchLogger.decode_mismatch_doc(line)['_id']


@squelch_keyboard_interrupt
def compare_collections(source, dest, percent, error_bp, recent_ops, ids_file):
    """
    compares two collections, using retries to see if collections are eventually consistent

    @param source_collection   source for data
    @param dest_collection     copied data to verify
    @param percent             percentage of documents to verify
    @param ids_file            files containing querie
    """
    MismatchLogger.collection_name = source['collection']

    # setup client connections
    source_client = utils.mongo_connect(source['host'], source['port'],
                                        ensure_direct=True,
                                        max_pool_size=POOL_SIZE,
                                        slave_okay=True,
                                        document_class=dict)
    source_collection = source_client[source['db']][source['collection']]

    dest_client = utils.mongo_connect(dest['host'], dest['port'],
                                      ensure_direct=True,
                                      max_pool_size=POOL_SIZE,
                                      slave_okay=True,
                                      document_class=dict)

    dest_collection = dest_client[dest['db']][dest['collection']]

    # setup stats
    stats = CompareStats()
    compare_pool = gevent.pool.Pool(POOL_SIZE)
    retry_pool = gevent.pool.Pool(POOL_SIZE * 5)

    # get just _id's first, because long-running queries degrade significantly
    # over time; reading just _ids is fast enough (or small enough?) not to suffer
    # from this degradation
    if recent_ops:
        id_getter = _get_ids_for_recent_ops(source_client, recent_ops)
        stats.total_docs = recent_ops
        if source_client.is_mongos:
            log.error("cannot read oplogs through mongos; specify mongod instances instead")
            return
    elif ids_file:
        id_getter = _get_ids_in_file(ids_file)
        stats.total_docs = id_getter.next()
    else:
        id_getter = _get_all_ids(source_collection)
        stats.total_docs = source_collection.count()

    if percent is not None:
        stats.total_docs = int(float(stats.total_docs) * percent / 100.0)

    stats_greenlet = gevent.spawn(_stats_worker, stats)

    # read documents in batches, but perform retries individually in separate greenlets
    _ids = []
    for _id in id_getter:
        if percent is not None and not utils.id_in_subset(_id, percent):
            continue

        _ids.append(_id)
        if len(_ids) == READ_SIZE:
            _ids_to_compare = _ids
            _ids = []
            compare_pool.spawn(_compare_ids_worker,
                               _ids=_ids_to_compare,
                               source_collection=source_collection,
                               dest_collection=dest_collection,
                               stats=stats,
                               retry_pool=retry_pool)

    # compare final batch of _id's
    if _ids:
        compare_pool.spawn(_compare_ids_worker,
                           _ids=_ids,
                           source_collection=source_collection,
                           dest_collection=dest_collection,
                           stats=stats,
                           retry_pool=retry_pool)

    # wait for all greenlets to finish
    compare_pool.join()
    retry_pool.join()
    stats_greenlet.kill()
    stats.log()
    log.info("compare finished")


if __name__ == '__main__':
    # make GC perform reasonably
    utils.tune_gc()

    # setup async socket ops
    gevent.monkey.patch_socket()

    # parse command-line options 
    import argparse
    parser = argparse.ArgumentParser(description='Copies a collection from one mongod to another.')
    parser.add_argument(
        '--source', type=str, required=True, metavar='URL',
        help='source to read from; can be a file containing sources or a url like: host[:port]/db/collection; '
             'e.g. localhost:27017/test_database.source_collection')
    parser.add_argument(
        '--ids-file', type=str, default=None,
        help='read ids to compare from this file')
    parser.add_argument(
        '--dest', type=str, required=True, metavar='URL',
        help='source to read from; see --source for format of URL')
    parser.add_argument(
        '--percent', type=int, metavar='PCT', default=None,
        help='verify only PCT%% of data')
    parser.add_argument(
        '--error-bp', type=int, metavar='BP', default=None,
        help='intentionally introduce errors at a rate of BP basis points')
    parser.add_argument(
        '--recent-ops', type=int, metavar='COUNT', default=None,
        help='verify documents touched by the last N ops')

    args = parser.parse_args()

    dest = utils.parse_mongo_url(args.dest)
    if os.path.exists(args.source):
        sources = utils.parse_source_file(args.source)
    else:
        sources = [utils.parse_mongo_url(args.source)]

    if args.ids_file and args.recent_ops:
        raise ValueError("the --ids-file and --recent-ops parameters cannot be combined")

    # finally, compare stuff!
    processes = []
    for source in sources:
        name = "%s:%s" % (source['host'], source['port'])
        process = Process(target=compare_collections,
                          name=name,
                          kwargs=dict(
                            source=source,
                            dest=dest,
                            percent=args.percent,
                            error_bp=args.error_bp,
                            recent_ops=args.recent_ops,
                            ids_file=args.ids_file,
                          ))
        process.start()

    utils.wait_for_processes(processes)
