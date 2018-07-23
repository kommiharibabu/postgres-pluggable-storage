/*-------------------------------------------------------------------------
 *
 * tableam.h
 *	  POSTGRES table access method definitions.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/tableam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TABLEAM_H
#define TABLEAM_H

#include "postgres.h"

#include "access/heapam.h"
#include "access/relscan.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "nodes/nodes.h"
#include "fmgr.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/snapshot.h"
#include "utils/tqual.h"

#define DEFAULT_TABLE_ACCESS_METHOD	"heap_tableam"

typedef union tuple_data
{
	TransactionId xid;
	CommandId	cid;
	ItemPointerData tid;
}			tuple_data;

typedef enum tuple_data_flags
{
	XMIN = 0,
	UPDATED_XID,
	CMIN,
	TID,
	CTID
}			tuple_data_flags;


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

/*
 * INLINE functions
 */
static inline TupleTableSlot*
table_gimmegimmeslot(Relation relation, List **reglist)
{
	TupleTableSlot *slot;

	slot = relation->rd_tableamroutine->gimmegimmeslot(relation);

	if (reglist)
		*reglist = lappend(*reglist, slot);

	return slot;
}

/*
 *	table_fetch_row_version		- retrieve tuple with given tid
 *
 *  XXX: This shouldn't just take a tid, but tid + additional information
 */
static inline bool
table_fetch_row_version(Relation r,
						ItemPointer tid,
						Snapshot snapshot,
						TupleTableSlot *slot,
						Relation stats_relation)
{
	return r->rd_tableamroutine->tuple_fetch_row_version(r, tid,
														 snapshot, slot,
														 stats_relation);
}


/*
 *	table_lock_tuple - lock a tuple in shared or exclusive mode
 *
 *  XXX: This shouldn't just take a tid, but tid + additional information
 */
static inline HTSU_Result
table_lock_tuple(Relation relation, ItemPointer tid, Snapshot snapshot,
				 TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
				 LockWaitPolicy wait_policy, uint8 flags,
				 HeapUpdateFailureData *hufd)
{
	return relation->rd_tableamroutine->tuple_lock(relation, tid, snapshot, slot,
												cid, mode, wait_policy,
												flags, hufd);
}

/* ----------------
 *		heap_beginscan_parallel - join a parallel scan
 *
 *		Caller must hold a suitable lock on the correct relation.
 * ----------------
 */
static inline TableScanDesc
table_beginscan_parallel(Relation relation, ParallelHeapScanDesc parallel_scan)
{
	Snapshot	snapshot;

	Assert(RelationGetRelid(relation) == parallel_scan->phs_relid);

	if (!parallel_scan->phs_snapshot_any)
	{
		/* Snapshot was serialized -- restore it */
		snapshot = RestoreSnapshot(parallel_scan->phs_snapshot_data);
		RegisterSnapshot(snapshot);
	}
	else
	{
		/* SnapshotAny passed by caller (not serialized) */
		snapshot = SnapshotAny;
	}

	return relation->rd_tableamroutine->scan_begin(relation, snapshot, 0, NULL, parallel_scan,
												true, true, true, false, false, !parallel_scan->phs_snapshot_any);
}

static inline ParallelHeapScanDesc
tableam_get_parallelheapscandesc(TableScanDesc sscan)
{
	return sscan->rs_rd->rd_tableamroutine->scan_get_parallelheapscandesc(sscan);
}

static inline HeapPageScanDesc
tableam_get_heappagescandesc(TableScanDesc sscan)
{
	/*
	 * Planner should have already validated whether the current storage
	 * supports Page scans are not? This function will be called only from
	 * Bitmap Heap scan and sample scan
	 */
	Assert(sscan->rs_rd->rd_tableamroutine->scan_get_heappagescandesc != NULL);

	return sscan->rs_rd->rd_tableamroutine->scan_get_heappagescandesc(sscan);
}

static inline void
table_syncscan_report_location(Relation rel, BlockNumber location)
{
	return rel->rd_tableamroutine->sync_scan_report_location(rel, location);
}

/*
 * heap_setscanlimits - restrict range of a heapscan
 *
 * startBlk is the page to start at
 * numBlks is number of pages to scan (InvalidBlockNumber means "all")
 */
static inline void
table_setscanlimits(TableScanDesc sscan, BlockNumber startBlk, BlockNumber numBlks)
{
	sscan->rs_rd->rd_tableamroutine->scansetlimits(sscan, startBlk, numBlks);
}


/* ----------------
 *		heap_beginscan	- begin relation scan
 *
 * heap_beginscan is the "standard" case.
 *
 * heap_beginscan_catalog differs in setting up its own temporary snapshot.
 *
 * heap_beginscan_strat offers an extended API that lets the caller control
 * whether a nondefault buffer access strategy can be used, and whether
 * syncscan can be chosen (possibly resulting in the scan not starting from
 * block zero).  Both of these default to true with plain heap_beginscan.
 *
 * heap_beginscan_bm is an alternative entry point for setting up a
 * TableScanDesc for a bitmap heap scan.  Although that scan technology is
 * really quite unlike a standard seqscan, there is just enough commonality
 * to make it worth using the same data structure.
 *
 * heap_beginscan_sampling is an alternative entry point for setting up a
 * TableScanDesc for a TABLESAMPLE scan.  As with bitmap scans, it's worth
 * using the same data structure although the behavior is rather different.
 * In addition to the options offered by heap_beginscan_strat, this call
 * also allows control of whether page-mode visibility checking is used.
 * ----------------
 */
static inline TableScanDesc
table_beginscan(Relation relation, Snapshot snapshot,
				  int nkeys, ScanKey key)
{
	return relation->rd_tableamroutine->scan_begin(relation, snapshot, nkeys, key, NULL,
												true, true, true, false, false, false);
}

static inline TableScanDesc
table_beginscan_catalog(Relation relation, int nkeys, ScanKey key)
{
	Oid			relid = RelationGetRelid(relation);
	Snapshot	snapshot = RegisterSnapshot(GetCatalogSnapshot(relid));

	return relation->rd_tableamroutine->scan_begin(relation, snapshot, nkeys, key, NULL,
												true, true, true, false, false, true);
}

static inline TableScanDesc
table_beginscan_strat(Relation relation, Snapshot snapshot,
						int nkeys, ScanKey key,
						bool allow_strat, bool allow_sync)
{
	return relation->rd_tableamroutine->scan_begin(relation, snapshot, nkeys, key, NULL,
												allow_strat, allow_sync, true,
												false, false, false);
}

static inline TableScanDesc
table_beginscan_bm(Relation relation, Snapshot snapshot,
					 int nkeys, ScanKey key)
{
	return relation->rd_tableamroutine->scan_begin(relation, snapshot, nkeys, key, NULL,
												false, false, true, true, false, false);
}

static inline TableScanDesc
table_beginscan_sampling(Relation relation, Snapshot snapshot,
						   int nkeys, ScanKey key,
						   bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	return relation->rd_tableamroutine->scan_begin(relation, snapshot, nkeys, key, NULL,
												allow_strat, allow_sync, allow_pagemode,
												false, true, false);
}

/* ----------------
 *		heap_rescan		- restart a relation scan
 * ----------------
 */
static inline void
table_rescan(TableScanDesc scan,
			   ScanKey key)
{
	scan->rs_rd->rd_tableamroutine->scan_rescan(scan, key, false, false, false, false);
}

/* ----------------
 *		heap_rescan_set_params	- restart a relation scan after changing params
 *
 * This call allows changing the buffer strategy, syncscan, and pagemode
 * options before starting a fresh scan.  Note that although the actual use
 * of syncscan might change (effectively, enabling or disabling reporting),
 * the previously selected startblock will be kept.
 * ----------------
 */
static inline void
table_rescan_set_params(TableScanDesc scan, ScanKey key,
						  bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	scan->rs_rd->rd_tableamroutine->scan_rescan(scan, key, true,
											 allow_strat, allow_sync, (allow_pagemode && IsMVCCSnapshot(scan->rs_snapshot)));
}

/* ----------------
 *		heap_endscan	- end relation scan
 *
 *		See how to integrate with index scans.
 *		Check handling if reldesc caching.
 * ----------------
 */
static inline void
table_endscan(TableScanDesc scan)
{
	scan->rs_rd->rd_tableamroutine->scan_end(scan);
}


/* ----------------
 *		heap_update_snapshot
 *
 *		Update snapshot info in heap scan descriptor.
 * ----------------
 */
static inline void
table_scan_update_snapshot(TableScanDesc scan, Snapshot snapshot)
{
	scan->rs_rd->rd_tableamroutine->scan_update_snapshot(scan, snapshot);
}

static inline TupleTableSlot *
table_scan_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	return sscan->rs_rd->rd_tableamroutine->scan_getnextslot(sscan, direction, slot);
}

static inline bool
table_tuple_fetch_from_offset(TableScanDesc sscan, BlockNumber blkno, OffsetNumber offset, TupleTableSlot *slot)
{
	return sscan->rs_rd->rd_tableamroutine->scan_fetch_tuple_from_offset(sscan, blkno, offset, slot);
}


static inline IndexFetchTableData*
table_begin_index_fetch_table(Relation rel)
{
	return rel->rd_tableamroutine->begin_index_fetch(rel);
}

static inline void
table_reset_index_fetch_table(struct IndexFetchTableData* scan)
{
	scan->rel->rd_tableamroutine->reset_index_fetch(scan);
}

static inline void
table_end_index_fetch_table(struct IndexFetchTableData* scan)
{
	scan->rel->rd_tableamroutine->end_index_fetch(scan);
}

/*
 * Insert a tuple from a slot into table AM routine
 */
static inline Oid
table_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
			   int options, BulkInsertState bistate)
{
	return relation->rd_tableamroutine->tuple_insert(relation, slot, cid, options,
													 bistate);
}

static inline Oid
table_insert_speculative(Relation relation, TupleTableSlot *slot, CommandId cid,
						 int options, BulkInsertState bistate, uint32 specToken)
{
	return relation->rd_tableamroutine->tuple_insert_speculative(relation, slot, cid, options,
																 bistate, specToken);
}

static inline void
table_complete_speculative(Relation relation, TupleTableSlot *slot, uint32 specToken,
								bool succeeded)
{
	return relation->rd_tableamroutine->tuple_complete_speculative(relation, slot, specToken, succeeded);
}

/*
 * Delete a tuple from tid using table AM routine
 */
static inline HTSU_Result
table_delete(Relation relation, ItemPointer tid, CommandId cid,
			 Snapshot crosscheck, bool wait,
			 HeapUpdateFailureData *hufd, bool changingPart)
{
	return relation->rd_tableamroutine->tuple_delete(relation, tid, cid,
												  crosscheck, wait, hufd, changingPart);
}

/*
 * update a tuple from tid using table AM routine
 */
static inline HTSU_Result
table_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
			 CommandId cid, Snapshot crosscheck, bool wait,
			 HeapUpdateFailureData *hufd, LockTupleMode *lockmode,
			 bool *update_indexes)
{
	return relation->rd_tableamroutine->tuple_update(relation, otid, slot,
												  cid, crosscheck, wait, hufd,
												  lockmode, update_indexes);
}

static inline bool
table_fetch_follow(struct IndexFetchTableData *scan,
				   ItemPointer tid,
				   Snapshot snapshot,
				   TupleTableSlot *slot,
				   bool *call_again, bool *all_dead)
{

	return scan->rel->rd_tableamroutine->tuple_fetch_follow(scan, tid, snapshot,
														   slot, call_again,
														   all_dead);
}

static inline bool
table_fetch_follow_check(Relation rel,
						 ItemPointer tid,
						 Snapshot snapshot,
						 bool *all_dead)
{
	IndexFetchTableData *scan = table_begin_index_fetch_table(rel);
	TupleTableSlot *slot = table_gimmegimmeslot(rel, NULL);
	bool call_again = false;
	bool found;

	found = table_fetch_follow(scan, tid, snapshot, slot, &call_again, all_dead);

	table_end_index_fetch_table(scan);
	ExecDropSingleTupleTableSlot(slot);

	return found;
}

/*
 *	table_multi_insert	- insert multiple tuple into a table
 */
static inline void
table_multi_insert(Relation relation, HeapTuple *tuples, int ntuples,
					 CommandId cid, int options, BulkInsertState bistate)
{
	relation->rd_tableamroutine->multi_insert(relation, tuples, ntuples,
										   cid, options, bistate);
}

static inline tuple_data
table_tuple_get_data(Relation relation, TupleTableSlot *slot, tuple_data_flags flags)
{
	return relation->rd_tableamroutine->get_tuple_data(slot, flags);
}

static inline void
table_get_latest_tid(Relation relation,
					   Snapshot snapshot,
					   ItemPointer tid)
{
	relation->rd_tableamroutine->tuple_get_latest_tid(relation, snapshot, tid);
}


static inline void
table_vacuum_rel(Relation rel, int options,
			 struct VacuumParams *params, BufferAccessStrategy bstrategy)
{
	rel->rd_tableamroutine->relation_vacuum(rel, options, params, bstrategy);
}

/*
 *	table_sync		- sync a heap, for use when no WAL has been written
 */
static inline void
table_sync(Relation rel)
{
	rel->rd_tableamroutine->relation_sync(rel);
}

/*
 * -------------------
 * storage Bulk Insert functions
 * -------------------
 */
static inline BulkInsertState
table_getbulkinsertstate(Relation rel)
{
	return rel->rd_tableamroutine->getbulkinsertstate();
}

static inline void
table_freebulkinsertstate(Relation rel, BulkInsertState bistate)
{
	rel->rd_tableamroutine->freebulkinsertstate(bistate);
}

static inline void
table_releasebulkinsertstate(Relation rel, BulkInsertState bistate)
{
	rel->rd_tableamroutine->releasebulkinsertstate(bistate);
}

/*
 * -------------------
 * storage tuple rewrite functions
 * -------------------
 */
static inline RewriteState
table_begin_rewrite(Relation OldHeap, Relation NewHeap,
				   TransactionId OldestXmin, TransactionId FreezeXid,
				   MultiXactId MultiXactCutoff, bool use_wal)
{
	return NewHeap->rd_tableamroutine->begin_heap_rewrite(OldHeap, NewHeap,
			OldestXmin, FreezeXid, MultiXactCutoff, use_wal);
}

static inline void
table_end_rewrite(Relation rel, RewriteState state)
{
	rel->rd_tableamroutine->end_heap_rewrite(state);
}

static inline void
table_rewrite_tuple(Relation rel, RewriteState state, HeapTuple oldTuple,
				   HeapTuple newTuple)
{
	rel->rd_tableamroutine->rewrite_heap_tuple(state, oldTuple, newTuple);
}

static inline bool
table_rewrite_dead_tuple(Relation rel, RewriteState state, HeapTuple oldTuple)
{
	return rel->rd_tableamroutine->rewrite_heap_dead_tuple(state, oldTuple);
}

/*
 * HeapTupleSatisfiesVisibility
 *		True iff heap tuple satisfies a time qual.
 *
 * Notes:
 *	Assumes heap tuple is valid.
 *	Beware of multiple evaluations of snapshot argument.
 *	Hint bits in the HeapTuple's t_infomask may be updated as a side effect;
 *	if so, the indicated buffer is marked dirty.
 */
#define HeapTupleSatisfiesVisibility(method, slot, snapshot) \
	(((method)->snapshot_satisfies) (slot, snapshot))

extern TableAmRoutine * GetTableAmRoutine(Oid amhandler);
extern TableAmRoutine * GetTableAmRoutineByAmId(Oid amoid);
extern TableAmRoutine * GetHeapamTableAmRoutine(void);

#endif		/* TABLEAM_H */
