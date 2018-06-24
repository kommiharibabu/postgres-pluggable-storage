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

#include "access/heapam.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "utils/rel.h"

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

extern TupleTableSlot* table_gimmegimmeslot(Relation relation, List **reglist);
extern TableScanDesc table_beginscan_parallel(Relation relation, ParallelHeapScanDesc parallel_scan);
extern ParallelHeapScanDesc tableam_get_parallelheapscandesc(TableScanDesc sscan);
extern HeapPageScanDesc tableam_get_heappagescandesc(TableScanDesc sscan);
extern void table_syncscan_report_location(Relation rel, BlockNumber location);
extern void table_setscanlimits(TableScanDesc sscan, BlockNumber startBlk, BlockNumber numBlks);
extern TableScanDesc table_beginscan(Relation relation, Snapshot snapshot,
				  int nkeys, ScanKey key);
extern TableScanDesc table_beginscan_catalog(Relation relation, int nkeys, ScanKey key);
extern TableScanDesc table_beginscan_strat(Relation relation, Snapshot snapshot,
						int nkeys, ScanKey key,
						bool allow_strat, bool allow_sync);
extern TableScanDesc table_beginscan_bm(Relation relation, Snapshot snapshot,
					 int nkeys, ScanKey key);
extern TableScanDesc table_beginscan_sampling(Relation relation, Snapshot snapshot,
						   int nkeys, ScanKey key,
						   bool allow_strat, bool allow_sync, bool allow_pagemode);

extern struct IndexFetchTableData* table_begin_index_fetch_table(Relation rel);
extern void table_reset_index_fetch_table(struct IndexFetchTableData* scan);
extern void table_end_index_fetch_table(struct IndexFetchTableData* scan);

extern void table_endscan(TableScanDesc scan);
extern void table_rescan(TableScanDesc scan, ScanKey key);
extern void table_rescan_set_params(TableScanDesc scan, ScanKey key,
						  bool allow_strat, bool allow_sync, bool allow_pagemode);
extern void table_scan_update_snapshot(TableScanDesc scan, Snapshot snapshot);

extern TupleTableSlot *table_scan_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot);
extern bool table_tuple_fetch_from_offset(TableScanDesc sscan, BlockNumber blkno, OffsetNumber offset, TupleTableSlot *slot);

extern void storage_get_latest_tid(Relation relation,
					   Snapshot snapshot,
					   ItemPointer tid);

extern bool table_fetch_row_version(Relation relation,
			  ItemPointer tid,
			  Snapshot snapshot,
			  TupleTableSlot *slot,
			  Relation stats_relation);

extern bool table_fetch_follow(struct IndexFetchTableData *scan,
							   ItemPointer tid,
							   Snapshot snapshot,
							   TupleTableSlot *slot,
							   bool *call_again, bool *all_dead);

extern bool table_fetch_follow_check(Relation rel,
									 ItemPointer tid,
									 Snapshot snapshot,
									 bool *all_dead);

extern HTSU_Result table_lock_tuple(Relation relation, ItemPointer tid, Snapshot snapshot,
				   TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
				   LockWaitPolicy wait_policy, uint8 flags,
				   HeapUpdateFailureData *hufd);

extern Oid table_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
			   int options, BulkInsertState bistate);
extern Oid table_insert_speculative(Relation relation, TupleTableSlot *slot, CommandId cid,
									int options, BulkInsertState bistate, uint32 specToken);
extern void table_complete_speculative(Relation relation, TupleTableSlot *slot, uint32 specToken,
									   bool succeeded);

extern HTSU_Result table_delete(Relation relation, ItemPointer tid, CommandId cid,
			   Snapshot crosscheck, bool wait, HeapUpdateFailureData *hufd,
			   bool changingPart);

extern HTSU_Result table_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
			   CommandId cid, Snapshot crosscheck, bool wait,
			   HeapUpdateFailureData *hufd, LockTupleMode *lockmode,
			   bool *upddate_indexes);

extern void table_multi_insert(Relation relation, HeapTuple *tuples, int ntuples,
					 CommandId cid, int options, BulkInsertState bistate);

extern tuple_data table_tuple_get_data(Relation relation, TupleTableSlot *slot, tuple_data_flags flags);

extern void table_get_latest_tid(Relation relation,
					   Snapshot snapshot,
					   ItemPointer tid);

extern void table_sync(Relation rel);
struct VacuumParams;
extern void table_vacuum_rel(Relation onerel, int options,
				struct VacuumParams *params, BufferAccessStrategy bstrategy);

extern BulkInsertState table_getbulkinsertstate(Relation rel);
extern void table_freebulkinsertstate(Relation rel, BulkInsertState bistate);
extern void table_releasebulkinsertstate(Relation rel, BulkInsertState bistate);

extern RewriteState table_begin_rewrite(Relation OldHeap, Relation NewHeap,
				   TransactionId OldestXmin, TransactionId FreezeXid,
				   MultiXactId MultiXactCutoff, bool use_wal);
extern void table_end_rewrite(Relation rel, RewriteState state);
extern void table_rewrite_tuple(Relation rel, RewriteState state, HeapTuple oldTuple,
				   HeapTuple newTuple);
extern bool table_rewrite_dead_tuple(Relation rel, RewriteState state, HeapTuple oldTuple);

#endif		/* TABLEAM_H */
