import functools
import gc
import gevent
import logging
import pymongo
from pymongo.errors import AutoReconnect, ConnectionFailure, OperationFailure, TimeoutError
import signal
import sys

loggers = {}
def get_logger(name):
    """
    get a logger object with reasonable defaults for formatting

    @param name used to identify the logger (though not currently useful for anything)
    """
    global loggers
    if name in loggers:
        return loggers[name]

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s:%(processName)s] %(message)s",
                                      "%m-%d %H:%M:%S"))
    logger.addHandler(sh)

    loggers[name] = logger
    return logger

log = get_logger("utils")


def mongo_connect(host, port, ensure_direct=False, secondary_only=False, max_pool_size=1,
                  socketTimeoutMS=None, w=0, read_preference=None, document_class=dict,
                  replicaSet=None, slave_okay=None):
    """
    Same as MongoClient.connect, except that we are paranoid and ensure that cursors
    # point to what we intended to point them to. Also sets reasonable defaults for our
    needs.

    @param host            host to connect to
    @param port            port to connect to
    @param ensure_direct   do safety checks to ensure we are connected to specified mongo instance

    most other keyword arguments mirror those for pymongo.MongoClient
    """
    options = dict(
        host=host,
        port=port,
        socketTimeoutMS=socketTimeoutMS,
        use_greenlets=True,
        max_pool_size=max_pool_size,
        w=1,
        document_class=document_class)
    if replicaSet is not None:
        options['replicaSet'] = replicaSet
    if read_preference is not None:
        options['read_preference'] = read_preference
    if slave_okay is not None:
        options['slave_okay'] = slave_okay
    client = pymongo.MongoClient(**options)

    if ensure_direct:
        # make sure we are connected to mongod/mongos that was specified; mongodb drivers
        # have the tendency of doing "magical" things in terms of connecting to other boxes
        test_collection = client['local']['test']
        test_cursor = test_collection.find(slave_okay=True, limit=1)
        connection = test_cursor.collection.database.connection
        if connection.host != host or connection.port != port:
            raise ValueError("connected to %s:%d (expected %s:%d)" %
                             (connection.host, connection.port, host, port))

    return client


def parse_mongo_url(url):
    """
    Takes in pseudo-URL of form

    host[:port]/db/collection (e.g. localhost/prod_maestro/emails)

    and returns a dictionary containing elements 'host', 'port', 'db', 'collection'
    """
    try:
        host, db, collection = url.split('/')
    except ValueError:
        raise ValueError("urls be of format: host[:port]/db/collection")

    host_tokens = host.split(':')
    if len(host_tokens) == 2:
        host = host_tokens[0]
        port = int(host_tokens[1])
    elif len(host_tokens) == 1:
        port = 27017
    elif len(host_tokens) > 2:
        raise ValueError("urls be of format: host[:port]/db/collection")

    return dict(host=host, port=port, db=db, collection=collection)


def _source_file_syntax():
    print "--source files must be of the following format:"
    print "database_name.collection_name"
    print "mongo-shard-1.foo.com"
    print "mongo-shard-2.foo.com:27019"
    print "..."
    sys.exit(1)


def parse_source_file(filename):
    """
    parses an input file passed to the --source parameter as a list of dicts that contain
    these fields:

    host
    port
    db: database name
    collection
    """
    sources = []

    with open(filename, "r") as source_file:
        fullname = source_file.readline().strip()
        try:
            db, collection = fullname.split('.')
        except ValueError:
            _source_file_syntax()

        for source in [line.strip() for line in source_file]:
            tokens = source.split(':')
            if len(tokens) == 1:
                host = tokens[0]
                port = 27017
            elif len(tokens) == 2:
                host, port = tokens
                port = int(port)
            else:
                raise ValueError("%s is not a valid source", source)

            sources.append(dict(host=host, port=port, db=db, collection=collection))

    return sources


def get_last_oplog_entry(client):
    """
    gets most recent oplog entry from the given pymongo.MongoClient
    """
    oplog = client['local']['oplog.rs']
    cursor = oplog.find().sort('$natural', pymongo.DESCENDING).limit(1)
    docs = [doc for doc in cursor]
    if not docs:
        raise ValueError("oplog has no entries!")
    return docs[0]


def tune_gc():
    """
    normally, GC is too aggressive; use kmod's suggestion for tuning it
    """
    gc.set_threshold(25000, 25, 10)


def id_in_subset(_id, pct):
    """
    Returns True if _id fits in our definition of a "subset" of documents.
    Used for testing only.
    """
    return (hash(_id) % 100) < pct


def trim(s, prefixes, suffixes):
    """
    naive function that trims off prefixes and suffixes
    """
    for prefix in prefixes:
        if s.startswith(prefix):
            s = s[len(prefix):]

    for suffix in suffixes:
        if s.endswith(suffix):
            s = s[:-len(suffix)]

    return s


def log_exceptions(func):
    """
    logs exceptions using logger, which includes host:port info
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if 'stats' in kwargs:
            # stats could be a keyword arg
            stats = kwargs['stats']
        elif len(args) > 0:
            # or the last positional arg
            stats = args[-1]
            if not hasattr(stats, 'exceptions'):
                stats = None
        else:
            # or not...
            stats = None

        try:
            return func(*args, **kwargs)
        except SystemExit:
            # just exit, don't log / catch when we're trying to exit()
            raise
        except:
            log.exception("uncaught exception")
            # increment exception counter if one is available to us
            if stats:
                stats.exceptions += 1
    return wrapper


def squelch_keyboard_interrupt(func):
    """
    suppresses KeyboardInterrupts, to avoid stack trace explosion when pressing Control-C
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            sys.exit(1)
    return wrapper


def wait_for_processes(processes):
    try:
        [process.join() for process in processes]
    except KeyboardInterrupt:
        # prevent a frustrated user from interrupting our cleanup
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        # if a user presses Control-C, we still need to terminate child processes and join()
        # them, to avoid zombie processes
        for process in processes:
            process.terminate()
            process.join()
        log.error("exiting...")
        sys.exit(1)


def auto_retry(func):
    """
    decorator that automatically retries a MongoDB operation if we get an AutoReconnect
    exception

    do not combine with @log_exceptions!!
    """
    MAX_RETRIES = 20  # yes, this is sometimes needed
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # try to keep track of # of retries we've had to do
        if 'stats' in kwargs:
            # stats could be a keyword arg
            stats = kwargs['stats']
        elif len(args) > 0:
            # or the last positional arg
            stats = args[-1]
            if not hasattr(stats, 'retries'):
                stats = None
                log.warning("couldn't find stats")
        else:
            # or not...
            stats = None
            log.warning("couldn't find stats")

        failures = 0
        while True:
            try:
                return func(*args, **kwargs)
            except (AutoReconnect, ConnectionFailure, OperationFailure, TimeoutError):
                failures += 1
                if stats:
                    stats.retries += 1
                if failures >= MAX_RETRIES:
                    log.exception("FAILED after %d retries", MAX_RETRIES)
                    if stats:
                        stats.exceptions += 1
                    raise
                gevent.sleep(2 * failures)
                log.exception("retry %d after exception", failures)
    return wrapper