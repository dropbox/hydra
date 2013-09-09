# hydra - the multi-process MongoDB sharded collection copier

## License

See the accompanying LICENSE.txt file for licensing terms.

## Purpose

This is working *reference code* that performs a live copy from one MongoDB collection to another, with minimal or no visible impact to your production MongoDB clusters. Keeps the destination up-to-date with changes from the source with a typically small amount of lag.

There are two conditions that must remain true while running the tools in this suite:

1. `mongos`'s chunk balancer must be disabled using [sh.setBalancerState()](http://docs.mongodb.org/manual/reference/method/sh.setBalancerState/).
2. The set of source `mongod` instances (I recommend using secondaries) must remain up. Also, primary sources must remain primaries, and secondary sources must remain secondaries. This prevents dead cursors from interfering with the copy.

This has only been tested on MongoDB 2.2.3 on Ubuntu 12.04. This should work on other Linux platforms but may require work to operate with MongoDB 2.4.x and beyond.

## Required Python Packages

To use this software, use [pip](http://www.pip-installer.org/en/latest/) to install the following packages into your Python environment:

* [pymongo](https://pypi.python.org/pypi/pymongo/)
* [gevent version 1.0rc2](https://github.com/surfly/gevent#installing-from-github)
	* NOTE: Do not use anything older than 1.0rc2. Earlier versions may have stability issues under load.

## Usage

### copy_collection.py

`copy_collection.py` copies a MongoDB collection from one MongoDB cluster or standalone `mongod` instance to another. It does this in three steps:

1. Creates an initial snapshot of the source collection on the destination cluster/instance.
2. Copies indexes from source to destination.
3. Applies oplog entries from source to destination.

Steps #1 and #3 are performed by worker processeses, one for each source you define (more on this below). `copy_collection.py` routinely records its progress in its *state database*. After step #1 finishes, steps #2 and #3 can be resume at any time without issue.

Typical usage for `copy_collection.py` looks like:

~~~
copy_collection.py --source source_file.txt --dest mongos_host/database/collection
~~~

The file passed to `--source` must have the following format:

~~~
source_database_name.source_collection_name
mongod-instance-1.foo.com
mongod-instance-2.foo.com:27019
mongod-instance-3.foo.com:27018
~~~

Alternatively, `--source` can also accept as a parameter a `mongod` URL of a form similar to `--dest` (host[:port]/database/collection).

**NOTE:** sources need to be `mongod` instances, and preferably secondaries rather than primaries. I had a difficult time getting sufficient reliability and performance when copying from a `mongos` instance. However, the destination must be either a `mongod` instance (for a non-shared MongoDB setup) or a `mongos` instance (for a sharded setup).

Useful options:

* `--percent PCT`: limits your copy to a percentage of the source's documents; meant to be used with the corresponding `--percent` option for `compare_collections.py`
* `--restart`: re-initialize the state database, to restart from the initial snapshot, rather than continuing where we left off
* `--state-db`: specify a path in which to store the state database; this defaults to the current directory


#### copy_collection.py output

~~~
06-04 00:59:27 [INFO:MainProcess   ] using state db /home/user/hydra/test.collection.db
...
06-04 00:59:29 [INFO:shard1.foo.com] 4% | 5000 / 103993 copied | 2215/sec | 0 dupes | 0 exceptions | 0 retries
06-04 00:59:29 [INFO:shard2.foo.com] 3% | 3500 / 105326 copied | 1579/sec | 0 dupes | 0 exceptions | 0 retries
...
06-04 01:06:23 [INFO:shard1.foo.com] done with initial copy
06-04 01:06:23 [INFO:shard2.foo.com] done with initial copy
06-04 01:06:23 [INFO:parent process] building indices
06-04 01:06:23 [INFO:parent process] ensuring index on [(u'_id', 1)] (options = {'name': u'_id_'})
06-04 01:06:23 [INFO:parent process] starting oplog apply
06-04 01:06:23 [INFO:stats         ] OPS APPLIED                                    | WARNINGS
06-04 01:06:23 [INFO:stats         ] total     lag    inserts   removes   updates   | sleeps    exceptions retries
06-04 01:06:26 [INFO:shard1.foo.com] 204        2      0         0         204       | 0         0          0
06-04 01:06:29 [INFO:shard2.foo.com] 214        1      0         0         214       | 0         0          0
~~~

Watch out for an excessive number of retries and exceptions. Sleeps are generally OK unless there are an excessive number. Unfortunately, the definition of "excessive" depends on your specific situation.

After `copy_collection.py` begins applying ops, keep an eye on the `lag` column, which shows how many seconds behind `copy_collection.py`'s replication is.

### compare_collections.py

`compare_collections.py` compares two collections and is meant to be used with `copy_collection.py`. The two scripts can run simultaneously, once `copy_collection.py` is up-to-date with applying ops.

To compensate for small amounts of `copy_collection.py` lag, `compare_collections.py` tries the comparison of each document multiple times to check whether the documents eventually match. The number of retries and delay between retries is generous, to compensate for frequently updated documents and lag in `copy_collection.py`.

#### compare_collections.py output

~~~
06-04 01:23:00 [INFO:shard1.foo.com] 30% | 32000 / 104001 compared | 7659/sec | 1 retries | 0 mismatches
06-04 01:23:00 [INFO:shard2.foo.com] 21% | 22700 / 105831 compared | 5402/sec | 0 retries | 0 mismatches
~~~

Retries are OK, but watch out for frequent retries. Those might presage mismatches. The `_id`'s for mismatching documents are written to a file named `COLLECTION_mismatches.txt`. For example, if your collection name is albums, you'll find any mismatches in `albums_mismatches.txt`. The mismatches file can be used with the `copy_stragglers.py` tool that will be discussed below.

### copy_stragglers.py

Given the list of `_id`s in the `[collection_name]_mismatches.txt` file generated by `compare_collections.py`, this tool re-copies all documents with the given `_id`s.

For example, if you had just finished comparing the collection `albums` and `compare_collections.py` reported some mismatches, you'd run `copy_stragglers.py` as follows:


~~~
./copy_stragglers.py --source source-mongos.foo.com --dest destination-mongos.foo.com --mismatches-file albums_mismatches.txt
~~~

**NOTE**: Unlike `copy_collection.py` and `compare_collections.py`, `copy_stragglers.py` expects the source to be a `mongos` instance. This is mainly to keep the code extremely simple.

### cluster_cop.py

`cluster_cop.py` monitors the source MongoDB cluster for configuration changes that can impact `copy_collection.py` and `compare_collections.py`. These are:

1. Chunk balancing must be off throughout the whole migration
2. Primary `mongod` instances must remain primaries, secondaries must remain secondaries (this prevents cursors from dying while being used)