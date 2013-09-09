from bson import Timestamp
import itertools
import json
import sqlite3
import time

CREATE_STATE_SQL = (
"""
CREATE TABLE {table_name}
(
    source TEXT NOT NULL,
    dest TEXT NOT NULL,
    updated_at REAL NOT NULL,
    state TEXT NOT NULL,
    oplog_ts TEXT DEFAULT NULL,
    PRIMARY KEY(source, dest)
)
""")


def _mongo_dict_to_str(d):
    if 'id_source' in d:
        return d['id_source']['shard_name']

    return "%s:%d/%s/%s" % (d['host'], d['port'], d['db'], d['collection'])


def _results_as_dicts(cursor):
    """
    given a sqlite cursor, yields results as a dictionary mapping column names to
    column values

    probably slightly overengineered
    """
    results = []
    col_names = [d[0] for d in cursor.description]
    while True:
        rows = cursor.fetchmany()
        if not rows:
            break
        for row in rows:
            results.append(dict(itertools.izip(col_names, row)))
    return results


class CopyStateDB(object):
    """
    contains state of a collection copy in a sqlite3 database, for ease of
    use in other code

    a separate state file should be used for each sharded collection being copied,
    to avoid deleting state should copy_collection.py be run with --restart; if that's
    not a concern, share away!
    """

    STATE_TABLE = 'state'

    STATE_INITIAL_COPY = 'initial copy'
    STATE_WAITING_FOR_INDICES = 'waiting for indices'
    STATE_APPLYING_OPLOG = 'applying oplog'

    def __init__(self, path):
        self._conn = sqlite3.connect(path)
        self._path = path


    def drop_and_create(self):
        with self._conn:
            cursor = self._conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS %s" % self.STATE_TABLE)
            cursor.execute(CREATE_STATE_SQL.format(table_name=self.STATE_TABLE))


    def add_source_and_dest(self, source, dest):
        """
        adds a state entry for the given source and destination, not complaining
        if it already exists

        assumes source and dest are dict's with these fields: host, port, db, collection
        """
        source_str = _mongo_dict_to_str(source)
        dest_str = _mongo_dict_to_str(dest)
        with self._conn:
            cursor = self._conn.cursor()
            query  = "INSERT OR IGNORE INTO "+self.STATE_TABLE+" "
            query += "(source, dest, updated_at, state, oplog_ts) VALUES (?, ?, ?, ?, ?) "
            cursor.execute(query,
                           (source_str, dest_str, time.time(), self.STATE_INITIAL_COPY, None))


    def select_by_state(self, state):
        cursor = self._conn.cursor()
        query = "SELECT * FROM "+self.STATE_TABLE+" WHERE state=?"
        cursor.execute(query, (state,))
        return _results_as_dicts(cursor)


    def update_oplog_ts(self, source, dest, oplog_ts):
        """
        updates where we are in applying oplog entries
        """
        assert isinstance(oplog_ts, Timestamp)
        source_str = _mongo_dict_to_str(source)
        dest_str = _mongo_dict_to_str(dest)
        oplog_ts_json = json.dumps({'time': oplog_ts.time, 'inc': oplog_ts.inc})
        query  = "UPDATE "+self.STATE_TABLE+" "
        query += "SET oplog_ts = ? "
        query += "WHERE source = ? AND dest = ?"
        with self._conn:
            cursor = self._conn.cursor()
            cursor.execute(query, (oplog_ts_json, source_str, dest_str))


    def update_state(self, source, dest, state):
        source_str = _mongo_dict_to_str(source)
        dest_str = _mongo_dict_to_str(dest)
        query  = "UPDATE "+self.STATE_TABLE+" "
        query += "SET state = ? "
        query += "WHERE source = ? AND dest = ?"
        with self._conn:
            cursor = self._conn.cursor()
            cursor.execute(query, (state, source_str, dest_str))


    def get_oplog_ts(self, source, dest):
        source_str = _mongo_dict_to_str(source)
        dest_str = _mongo_dict_to_str(dest)
        query  = "SELECT oplog_ts "
        query += "FROM %s " % self.STATE_TABLE
        query += "WHERE source = ? AND dest = ?"
        with self._conn:
            cursor = self._conn.cursor()
            cursor.execute(query, (source_str, dest_str))
            result = json.loads(cursor.fetchone()[0])
            return Timestamp(time=result['time'], inc=result['inc'])