# MongoDB oplog format notes (for MongoDB 2.2.x)

## high level

* by default, oplog is in `oplog.rs`, in database `local`
* op log entries are idempotent
* `applyLog` internal MongoDB command is used to apply oplog entries for built-in replication

## example oplog entry

    {
        "ts" : Timestamp(1366075364000, 109),
        "h" : NumberLong("338445281403071495"),
        "v" : 2,
        "op" : "u",
        "ns" : "prod_maestro.emails",
        "o2" : {
            "_id" : BinData(0,"RexdN/+nCUlwlQkhvxZXf20T3SxC11tgAw==")
        },
        "o" : {
            "$set" : {
                "_r" : true
            },
        "$unset" : {
            "st" : 1
         }
        }
    }

## fields

* `ts`: MongoDB timestamp (time + counter to serialize op entries)
* `h`: unique operation ID
* `op`: type of operation
	* `i` - insert
	* `u` - update
	* `d` - delete
	* `c` - db command
	* `n` - no op
* `v`: oplog version # (should be "2" for our MongoDB 2.2.x)
* `ns`: the namespace that this op should be applied to (e.g. `prod_maestro.emails`)
* `o`: object whose meaning depends on `op`, as follows:
	* inserts: the document to be inserted
	* deletes: document containing the `_id` of the document to be removed
	* updates: contains the updates to be done (e.g. `$set: foo`, `$inc: foo`)
	* commands: unknown (don't occur in our oplog)
* `o2`: extra object, used as follows:
	* updates: document containing the `_id` of the document to be updated using the update operators in `o` 
	
## relevant MongoDB source files

* `src/mongo/db/oplog.h`
* `src/mongo/db/repl/rs_sync.cpp`

## relevant open-source projects

Node-based oplog watcher:
<https://github.com/TorchlightSoftware/mongo-watch>