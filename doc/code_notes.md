# mongo-copier

## Purpose

Perform a live copy of a collection from one sharded MongoDB cluster to another, with a minimum of downtime. Also do this safely with a lot of verification.

## General Design

The two main scripts, `copy_collection.py` and `compare_collections.py`, are written in Python. The main 3rd-party libraries used are [gevent](http://www.gevent.org/) (version 1.0rc2) and [pymongo](http://api.mongodb.org/python/current/). Both scripts fork a worker process for each shard that handles all data contained in a secondary mongod instance. After much trial and error, using `mongos` was ruled out for severe performance issues.


## Code Description

### copy_collection.py

`copy_collection.py` is the core tool. As mentioned before, it has a worker process per shard that it's reading from. Reads are done directly from `mongod` instances. Any IPC is done through a sqlite database, known as the "state db."

The main function in `copy_collection.py` is `copy_collection_parent()`, which implements a relatively simple state machine with this logic:

1. (`copier.py`) Each worker process calls `copier.copy_collection()` to perform an initial copy of the data from one `mongod` instance to the destination `mongos`.
	* `copier.copy_collection()` does the following:
		* updates the state db with the current oplog position
		* queries for all `_id`'s in source collection
		* for each batch of `_id`'s (currently 500), spawns the greenlet `_find_and_insert_batch_worker` to to read all documents in the batch and insert them into destination
2. (`copier.py`) When *all* worker processes have finished step #1, the parent process calls `copier.copy_indexes()` to copy all indexes from the source to the destination collection.
	* Carries over index names (though you shouldn't really use these in hints), sparseness, and uniqueness.
3. (`oplog_applier.py`) When indexes have finished building, we create worker processes to apply oplogs from each shard.
	* **NOTE:** it is critical that the chunk balancer be disabled; otherwise, nasty race conditions can occur while applying ops
	* for all ops performed after the oplog timestamp recorded in step #1, `oplog_applier.apply_oplog()` does the following:
		* for *inserts*, we insert the new document
		* for *removes*, we remove the referenced document
		* for *updates*, we query the source for the updated version of the document and save that to the destination (the format of update oplog entries is pretty funky, and most drivers can't actually read it correctly)
	* All ops are applied in parallel. *However*, we temporarily stop applying new ops when we read an op that touches the same `_id` as an already pending op. This avoids a nasty race condition that can be caused by out-of-order op execution on the same `_id`.

`copy_collection.py` is designed to be fully resumable after step #1 finishes. All state is persisted through the state sqlite db.

#### copy_collection.py - useful debugging options

* `--percent`: copies a percentage of documents; pair this with `compare_collections.py`'s `--percent` parameter
* `--restart`: restart a failed copy; this reinitializes the state db


### compare_collections.py

Compared to `copy_collection.py`, `compare_collections.py` is very straightforward. It ensures that all documents that exist in the source collection exist with identical contents in the destination collection. It is intended to run in parallel with `copy_collection.py`, once it has caught up in its application of oplog entries.

For each source shard, `copy_collection.py` spawns a worker process to compare documents in the source and destination collections. Each comparison is done by a separate greenlet. Each comparison also calls `EmailDocumentManager.should_copy_doc()` to check whether the document should be compared at all (see the above description for `copy_collection.py` for an explanation).

If a mismatch is detected, a fixed number of retries (currently 5) is attempted after increasingly long sleeps. This allows us to verify "eventual consistency," which is necessary for asynchronous replication.

#### compare_collections.py -- useful options

* `--percent`: see description of identical option for `copy_collection.py`
* `--recent-ops`: this one is **vital**; it compares all documents touched by the last *N* ops, which ensures that op replays are fully caught up

### cluster_cop.py

Simple tool to ensure that the following are true throughout the collection migration process:

* chunk balancing is off
* no new shards are added
* no primary promotions happen (we don't want broken cursors)

The tool also prints the time of the most recent op in the cluster. This helps ensure that no new ops are hitting the "old" cluster before the final cutover.

