/*---------------------------------------------------------------------
 *
 * tableamapi.h
 *		API for Postgres table access methods
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * src/include/access/tableamapi.h
 *---------------------------------------------------------------------
 */
#ifndef TABLEEAMAPI_H
#define TABLEEAMAPI_H

#include "access/heapam.h"
#include "access/tableam.h"
#include "nodes/execnodes.h"
#include "nodes/nodes.h"
#include "fmgr.h"
#include "utils/snapshot.h"

struct IndexFetchTableData;

/*
 * Storage routine function hooks
 */
typedef bool (*SnapshotSatisfies_function) (TupleTableSlot *slot, Snapshot snapshot);
typedef HTSU_Result (*SnapshotSatisfiesUpdate_function) (TupleTableSlot *slot, CommandId curcid);
typedef HTSV_Result (*SnapshotSatisfiesVacuum_function) (TupleTableSlot *slot, TransactionId OldestXmin);

typedef Oid (*TupleInsert_function) (Relation rel, TupleTableSlot *slot, CommandId cid,
									 int options, BulkInsertState bistate);

typedef Oid (*TupleInsertSpeculative_function) (Relation rel,
												 TupleTableSlot *slot,
												 CommandId cid,
												 int options,
												 BulkInsertState bistate,
												 uint32 specToken);


typedef void (*TupleCompleteSpeculative_function) (Relation rel,
												  TupleTableSlot *slot,
												  uint32 specToken,
												  bool succeeded);

typedef HTSU_Result (*TupleDelete_function) (Relation relation,
											 ItemPointer tid,
											 CommandId cid,
											 Snapshot crosscheck,
											 bool wait,
											 HeapUpdateFailureData *hufd,
											 bool changingPart);

typedef HTSU_Result (*TupleUpdate_function) (Relation relation,
											 ItemPointer otid,
											 TupleTableSlot *slot,
											 CommandId cid,
											 Snapshot crosscheck,
											 bool wait,
											 HeapUpdateFailureData *hufd,
											 LockTupleMode *lockmode,
											 bool *update_indexes);

typedef bool (*TupleFetchRowVersion_function) (Relation relation,
											   ItemPointer tid,
											   Snapshot snapshot,
											   TupleTableSlot *slot,
											   Relation stats_relation);

typedef HTSU_Result (*TupleLock_function) (Relation relation,
										   ItemPointer tid,
										   Snapshot snapshot,
										   TupleTableSlot *slot,
										   CommandId cid,
										   LockTupleMode mode,
										   LockWaitPolicy wait_policy,
										   uint8 flags,
										   HeapUpdateFailureData *hufd);

typedef void (*MultiInsert_function) (Relation relation, HeapTuple *tuples, int ntuples,
									  CommandId cid, int options, BulkInsertState bistate);

typedef void (*TupleGetLatestTid_function) (Relation relation,
											Snapshot snapshot,
											ItemPointer tid);

typedef tuple_data(*GetTupleData_function) (TupleTableSlot *slot, tuple_data_flags flags);

struct VacuumParams;
typedef void (*RelationVacuum_function)(Relation onerel, int options,
				struct VacuumParams *params, BufferAccessStrategy bstrategy);

typedef void (*RelationSync_function) (Relation relation);

typedef BulkInsertState (*GetBulkInsertState_function) (void);
typedef void (*FreeBulkInsertState_function) (BulkInsertState bistate);
typedef void (*ReleaseBulkInsertState_function) (BulkInsertState bistate);

typedef RewriteState (*BeginHeapRewrite_function) (Relation OldHeap, Relation NewHeap,
				   TransactionId OldestXmin, TransactionId FreezeXid,
				   MultiXactId MultiXactCutoff, bool use_wal);
typedef void (*EndHeapRewrite_function) (RewriteState state);
typedef void (*RewriteHeapTuple_function) (RewriteState state, HeapTuple oldTuple,
				   HeapTuple newTuple);
typedef bool (*RewriteHeapDeadTuple_function) (RewriteState state, HeapTuple oldTuple);

typedef TupleTableSlot* (*Slot_function) (Relation relation);

typedef TableScanDesc (*ScanBegin_function) (Relation relation,
											Snapshot snapshot,
											int nkeys, ScanKey key,
											ParallelHeapScanDesc parallel_scan,
											bool allow_strat,
											bool allow_sync,
											bool allow_pagemode,
											bool is_bitmapscan,
											bool is_samplescan,
											bool temp_snap);

typedef struct IndexFetchTableData* (*BeginIndexFetchTable_function) (Relation relation);
typedef void (*ResetIndexFetchTable_function) (struct IndexFetchTableData* data);
typedef void (*EndIndexFetchTable_function) (struct IndexFetchTableData* data);

typedef ParallelHeapScanDesc (*ScanGetParallelheapscandesc_function) (TableScanDesc scan);
typedef HeapPageScanDesc(*ScanGetHeappagescandesc_function) (TableScanDesc scan);
typedef void (*SyncScanReportLocation_function) (Relation rel, BlockNumber location);
typedef void (*ScanSetlimits_function) (TableScanDesc sscan, BlockNumber startBlk, BlockNumber numBlks);

typedef TupleTableSlot *(*ScanGetnextSlot_function) (TableScanDesc scan,
													 ScanDirection direction, TupleTableSlot *slot);

typedef bool (*ScanFetchTupleFromOffset_function) (TableScanDesc scan,
												   BlockNumber blkno, OffsetNumber offset, TupleTableSlot *slot);

typedef void (*ScanEnd_function) (TableScanDesc scan);


typedef void (*ScanGetpage_function) (TableScanDesc scan, BlockNumber page);
typedef void (*ScanRescan_function) (TableScanDesc scan, ScanKey key, bool set_params,
									 bool allow_strat, bool allow_sync, bool allow_pagemode);
typedef void (*ScanUpdateSnapshot_function) (TableScanDesc scan, Snapshot snapshot);

typedef bool (*TupleFetchFollow_function)(struct IndexFetchTableData *scan,
										  ItemPointer tid,
										  Snapshot snapshot,
										  TupleTableSlot *slot,
										  bool *call_again, bool *all_dead);

/*
 * API struct for a table AM.  Note this must be stored in a single palloc'd
 * chunk of memory.
 */
typedef struct TableAmRoutine
{
	NodeTag		type;

	Slot_function gimmegimmeslot;

	SnapshotSatisfies_function snapshot_satisfies;
	SnapshotSatisfiesUpdate_function snapshot_satisfiesUpdate;
	SnapshotSatisfiesVacuum_function snapshot_satisfiesVacuum;

	/* Operations on physical tuples */
	TupleInsert_function tuple_insert;
	TupleInsertSpeculative_function tuple_insert_speculative;
	TupleCompleteSpeculative_function tuple_complete_speculative;
	TupleUpdate_function tuple_update;
	TupleDelete_function tuple_delete;
	TupleFetchRowVersion_function tuple_fetch_row_version;
	TupleLock_function tuple_lock;
	MultiInsert_function multi_insert;
	TupleGetLatestTid_function tuple_get_latest_tid;
	TupleFetchFollow_function tuple_fetch_follow;

	GetTupleData_function get_tuple_data;

	RelationVacuum_function relation_vacuum;
	RelationSync_function relation_sync;

	GetBulkInsertState_function getbulkinsertstate;
	FreeBulkInsertState_function freebulkinsertstate;
	ReleaseBulkInsertState_function releasebulkinsertstate;

	BeginHeapRewrite_function begin_heap_rewrite;
	EndHeapRewrite_function end_heap_rewrite;
	RewriteHeapTuple_function rewrite_heap_tuple;
	RewriteHeapDeadTuple_function rewrite_heap_dead_tuple;

	/* Operations on relation scans */
	ScanBegin_function scan_begin;
	ScanGetParallelheapscandesc_function scan_get_parallelheapscandesc;
	ScanGetHeappagescandesc_function scan_get_heappagescandesc;
	SyncScanReportLocation_function sync_scan_report_location;
	ScanSetlimits_function scansetlimits;
	ScanGetnextSlot_function scan_getnextslot;
	ScanFetchTupleFromOffset_function scan_fetch_tuple_from_offset;
	ScanEnd_function scan_end;
	ScanGetpage_function scan_getpage;
	ScanRescan_function scan_rescan;
	ScanUpdateSnapshot_function scan_update_snapshot;

	BeginIndexFetchTable_function begin_index_fetch;
	EndIndexFetchTable_function reset_index_fetch;
	EndIndexFetchTable_function end_index_fetch;

}			TableAmRoutine;

extern TableAmRoutine * GetTableAmRoutine(Oid amhandler);
extern TableAmRoutine * GetTableAmRoutineByAmId(Oid amoid);
extern TableAmRoutine * GetHeapamTableAmRoutine(void);

#endif							/* TABLEEAMAPI_H */
