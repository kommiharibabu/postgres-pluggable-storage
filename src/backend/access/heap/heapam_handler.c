/*-------------------------------------------------------------------------
 *
 * heapam_handler.c
 *	  heap table access method code
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/heapam_handler.c
 *
 *
 * NOTES
 *	  This file contains the heap_ routines which implement
 *	  the POSTGRES heap table access method used for all POSTGRES
 *	  relations.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/relscan.h"
#include "access/rewriteheap.h"
#include "access/tableam.h"
#include "catalog/pg_am_d.h"
#include "pgstat.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/tqual.h"

#include "storage/bufpage.h"
#include "storage/bufmgr.h"
#include "access/xact.h"


/* ----------------------------------------------------------------
 *				storage AM support routines for heapam
 * ----------------------------------------------------------------
 */

static bool
heapam_fetch_row_version(Relation relation,
						 ItemPointer tid,
						 Snapshot snapshot,
						 TupleTableSlot *slot,
						 Relation stats_relation)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;
	Buffer buffer;

	if (heap_fetch(relation, tid, snapshot, &bslot->base.tupdata, &buffer, stats_relation))
	{
		ExecStoreTuple(&bslot->base.tupdata, slot, buffer, false);
		ReleaseBuffer(buffer);
		return true;
	}

	slot->tts_tableOid = RelationGetRelid(relation);

	return false;
}

/*
 * Insert a heap tuple from a slot, which may contain an OID and speculative
 * insertion token.
 */
static Oid
heapam_heap_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
				   int options, BulkInsertState bistate)
{
	Oid			oid;
	HeapTuple	tuple = ExecGetHeapTupleFromSlot(slot);

	/* Set the OID, if the slot has one */
	if (slot->tts_tupleOid != InvalidOid)
		HeapTupleHeaderSetOid(tuple->t_data, slot->tts_tupleOid);

	/* Update the tuple with table oid */
	slot->tts_tableOid = RelationGetRelid(relation);
	if (slot->tts_tableOid != InvalidOid)
		tuple->t_tableOid = slot->tts_tableOid;

	/* Perform the insertion, and copy the resulting ItemPointer */
	oid = heap_insert(relation, tuple, cid, options, bistate);
	ItemPointerCopy(&tuple->t_self, &slot->tts_tid);

	return oid;
}

static Oid
heapam_heap_insert_speculative(Relation relation, TupleTableSlot *slot, CommandId cid,
							   int options, BulkInsertState bistate, uint32 specToken)
{
	Oid			oid;
	HeapTuple	tuple = ExecGetHeapTupleFromSlot(slot);

	/* Set the OID, if the slot has one */
	if (slot->tts_tupleOid != InvalidOid)
		HeapTupleHeaderSetOid(tuple->t_data, slot->tts_tupleOid);

	/* Update the tuple with table oid */
	slot->tts_tableOid = RelationGetRelid(relation);
	if (slot->tts_tableOid != InvalidOid)
		tuple->t_tableOid = slot->tts_tableOid;

	HeapTupleHeaderSetSpeculativeToken(tuple->t_data, specToken);

	/* Perform the insertion, and copy the resulting ItemPointer */
	oid = heap_insert(relation, tuple, cid, options, bistate);
	ItemPointerCopy(&tuple->t_self, &slot->tts_tid);

	return oid;
}

static void
heapam_heap_complete_speculative(Relation relation, TupleTableSlot *slot, uint32 spekToken,
								 bool succeeded)
{
	HeapTuple	tuple = ExecGetHeapTupleFromSlot(slot);

	/* adjust the tuple's state accordingly */
	if (!succeeded)
		heap_finish_speculative(relation, tuple);
	else
	{
		heap_abort_speculative(relation, tuple);
	}
}


static HTSU_Result
heapam_heap_delete(Relation relation, ItemPointer tid, CommandId cid,
				   Snapshot crosscheck, bool wait,
				   HeapUpdateFailureData *hufd, bool changingPart)
{
	/*
	 * Currently Deleting of index tuples are handled at vacuum, in case
	 * if the storage itself is cleaning the dead tuples by itself, it is
	 * the time to call the index tuple deletion also.
	 */
	return heap_delete(relation, tid, cid, crosscheck, wait, hufd, changingPart);
}


/*
 * Locks tuple and fetches its newest version and TID.
 *
 *	relation - table containing tuple
 *	tid - TID of tuple to lock
 *	snapshot - snapshot indentifying required version (used for assert check only)
 *	slot - tuple to be returned
 *	cid - current command ID (used for visibility test, and stored into
 *		  tuple's cmax if lock is successful)
 *	mode - indicates if shared or exclusive tuple lock is desired
 *	wait_policy - what to do if tuple lock is not available
 *	flags – indicating how do we handle updated tuples
 *	*hufd - filled in failure cases
 *
 * Function result may be:
 *	HeapTupleMayBeUpdated: lock was successfully acquired
 *	HeapTupleInvisible: lock failed because tuple was never visible to us
 *	HeapTupleSelfUpdated: lock failed because tuple updated by self
 *	HeapTupleUpdated: lock failed because tuple updated by other xact
 *	HeapTupleDeleted: lock failed because tuple deleted by other xact
 *	HeapTupleWouldBlock: lock couldn't be acquired and wait_policy is skip
 *
 * In the failure cases other than HeapTupleInvisible, the routine fills
 * *hufd with the tuple's t_ctid, t_xmax (resolving a possible MultiXact,
 * if necessary), and t_cmax (the last only for HeapTupleSelfUpdated,
 * since we cannot obtain cmax from a combocid generated by another
 * transaction).
 * See comments for struct HeapUpdateFailureData for additional info.
 */
static HTSU_Result
heapam_lock_tuple(Relation relation, ItemPointer tid, Snapshot snapshot,
				TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
				LockWaitPolicy wait_policy, uint8 flags,
				HeapUpdateFailureData *hufd)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;
	HTSU_Result		result;
	Buffer			buffer;
	HeapTuple		tuple = &bslot->base.tupdata;

	hufd->traversed = false;

retry:
	result = heap_lock_tuple(relation, tid, cid, mode, wait_policy,
		(flags & TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS) ? true : false,
							 tuple, &buffer, hufd);

	if (result == HeapTupleUpdated &&
		(flags & TUPLE_LOCK_FLAG_FIND_LAST_VERSION))
	{
		ReleaseBuffer(buffer);
		/* Should not encounter speculative tuple on recheck */
		Assert(!HeapTupleHeaderIsSpeculative(tuple->t_data));

		if (!ItemPointerEquals(&hufd->ctid, &tuple->t_self))
		{
			SnapshotData	SnapshotDirty;
			TransactionId	priorXmax;

			/* it was updated, so look at the updated version */
			*tid = hufd->ctid;
			/* updated row should have xmin matching this xmax */
			priorXmax = hufd->xmax;

			/*
			 * fetch target tuple
			 *
			 * Loop here to deal with updated or busy tuples
			 */
			InitDirtySnapshot(SnapshotDirty);
			for (;;)
			{
				if (ItemPointerIndicatesMovedPartitions(tid))
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("tuple to be locked was already moved to another partition due to concurrent update")));


				if (heap_fetch(relation, tid, &SnapshotDirty, tuple, &buffer, NULL))
				{
					/*
					 * If xmin isn't what we're expecting, the slot must have been
					 * recycled and reused for an unrelated tuple.  This implies that
					 * the latest version of the row was deleted, so we need do
					 * nothing.  (Should be safe to examine xmin without getting
					 * buffer's content lock.  We assume reading a TransactionId to be
					 * atomic, and Xmin never changes in an existing tuple, except to
					 * invalid or frozen, and neither of those can match priorXmax.)
					 */
					if (!TransactionIdEquals(HeapTupleHeaderGetXmin(tuple->t_data),
											 priorXmax))
					{
						ReleaseBuffer(buffer);
						return HeapTupleDeleted;
					}

					/* otherwise xmin should not be dirty... */
					if (TransactionIdIsValid(SnapshotDirty.xmin))
						elog(ERROR, "t_xmin is uncommitted in tuple to be updated");

					/*
					 * If tuple is being updated by other transaction then we have to
					 * wait for its commit/abort, or die trying.
					 */
					if (TransactionIdIsValid(SnapshotDirty.xmax))
					{
						ReleaseBuffer(buffer);
						switch (wait_policy)
						{
							case LockWaitBlock:
								XactLockTableWait(SnapshotDirty.xmax,
												  relation, &tuple->t_self,
												  XLTW_FetchUpdated);
								break;
							case LockWaitSkip:
								if (!ConditionalXactLockTableWait(SnapshotDirty.xmax))
									return result;	/* skip instead of waiting */
								break;
							case LockWaitError:
								if (!ConditionalXactLockTableWait(SnapshotDirty.xmax))
									ereport(ERROR,
											(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
											 errmsg("could not obtain lock on row in relation \"%s\"",
													RelationGetRelationName(relation))));
								break;
						}
						continue;		/* loop back to repeat heap_fetch */
					}

					/*
					 * If tuple was inserted by our own transaction, we have to check
					 * cmin against es_output_cid: cmin >= current CID means our
					 * command cannot see the tuple, so we should ignore it. Otherwise
					 * heap_lock_tuple() will throw an error, and so would any later
					 * attempt to update or delete the tuple.  (We need not check cmax
					 * because HeapTupleSatisfiesDirty will consider a tuple deleted
					 * by our transaction dead, regardless of cmax.) We just checked
					 * that priorXmax == xmin, so we can test that variable instead of
					 * doing HeapTupleHeaderGetXmin again.
					 */
					if (TransactionIdIsCurrentTransactionId(priorXmax) &&
						HeapTupleHeaderGetCmin(tuple->t_data) >= cid)
					{
						ReleaseBuffer(buffer);
						return result;
					}

					hufd->traversed = true;
					*tid = tuple->t_data->t_ctid;
					ReleaseBuffer(buffer);
					goto retry;
				}

				/*
				 * If the referenced slot was actually empty, the latest version of
				 * the row must have been deleted, so we need do nothing.
				 */
				if (tuple->t_data == NULL)
				{
					return HeapTupleDeleted;
				}

				/*
				 * As above, if xmin isn't what we're expecting, do nothing.
				 */
				if (!TransactionIdEquals(HeapTupleHeaderGetXmin(tuple->t_data),
										 priorXmax))
				{
					ReleaseBuffer(buffer);
					return HeapTupleDeleted;
				}

				/*
				 * If we get here, the tuple was found but failed SnapshotDirty.
				 * Assuming the xmin is either a committed xact or our own xact (as it
				 * certainly should be if we're trying to modify the tuple), this must
				 * mean that the row was updated or deleted by either a committed xact
				 * or our own xact.  If it was deleted, we can ignore it; if it was
				 * updated then chain up to the next version and repeat the whole
				 * process.
				 *
				 * As above, it should be safe to examine xmax and t_ctid without the
				 * buffer content lock, because they can't be changing.
				 */
				if (ItemPointerEquals(&tuple->t_self, &tuple->t_data->t_ctid))
				{
					/* deleted, so forget about it */
					ReleaseBuffer(buffer);
					return HeapTupleDeleted;
				}

				/* updated, so look at the updated row */
				*tid = tuple->t_data->t_ctid;
				/* updated row should have xmin matching this xmax */
				priorXmax = HeapTupleHeaderGetUpdateXid(tuple->t_data);
				ReleaseBuffer(buffer);
				/* loop back to fetch next in chain */
			}
		}
		else
		{
			/* tuple was deleted, so give up */
			return HeapTupleDeleted;
		}
	}

	slot->tts_tableOid = RelationGetRelid(relation);
	ExecStoreTuple(tuple, slot, buffer, false);
	ReleaseBuffer(buffer); // FIXME: invent option to just transfer pin?

	return result;
}


static HTSU_Result
heapam_heap_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
				   CommandId cid, Snapshot crosscheck, bool wait,
				   HeapUpdateFailureData *hufd, LockTupleMode *lockmode,
				   bool *update_indexes)
{
	HeapTuple	tuple = ExecGetHeapTupleFromSlot(slot);
	HTSU_Result result;

	/* Set the OID, if the slot has one */
	if (slot->tts_tupleOid != InvalidOid)
		HeapTupleHeaderSetOid(tuple->t_data, slot->tts_tupleOid);

	/* Update the tuple with table oid */
	if (slot->tts_tableOid != InvalidOid)
		tuple->t_tableOid = slot->tts_tableOid;

	result = heap_update(relation, otid, tuple, cid, crosscheck, wait,
						 hufd, lockmode);
	ItemPointerCopy(&tuple->t_self, &slot->tts_tid);

#if FIXME
	if (slot->tts_storage == NULL)
		ExecStoreTuple(tuple, slot, InvalidBuffer, true);
#endif
	slot->tts_tableOid = RelationGetRelid(relation);

	/*
	 * Note: instead of having to update the old index tuples associated with
	 * the heap tuple, all we do is form and insert new index tuples. This is
	 * because UPDATEs are actually DELETEs and INSERTs, and index tuple
	 * deletion is done later by VACUUM (see notes in ExecDelete). All we do
	 * here is insert new index tuples.  -cim 9/27/89
	 */

	/*
	 * insert index entries for tuple
	 *
	 * Note: heap_update returns the tid (location) of the new tuple in the
	 * t_self field.
	 *
	 * If it's a HOT update, we mustn't insert new index entries.
	 */
	*update_indexes = result == HeapTupleMayBeUpdated &&
		!HeapTupleIsHeapOnly(tuple);

	return result;
}

static tuple_data
heapam_get_tuple_data(TupleTableSlot *slot, tuple_data_flags flags)
{
	HeapTupleTableSlot *bslot = (HeapTupleTableSlot *) slot;
	HeapTuple	tuple = bslot->tuple;
	tuple_data	result;

	switch (flags)
	{
		case XMIN:
			result.xid = HeapTupleHeaderGetXmin(tuple->t_data);
			break;
		case UPDATED_XID:
			result.xid = HeapTupleHeaderGetUpdateXid(tuple->t_data);
			break;
		case CMIN:
			result.cid = HeapTupleHeaderGetCmin(tuple->t_data);
			break;
		case TID:
			result.tid = tuple->t_self;
			break;
		case CTID:
			result.tid = tuple->t_data->t_ctid;
			break;
		default:
			Assert(0);
			break;
	}

	return result;
}

static TupleTableSlot *
heapam_gimmegimmeslot(Relation relation)
{
	return MakeTupleTableSlot(RelationGetDescr(relation), TTS_TYPE_BUFFER);
}

static ParallelHeapScanDesc
heapam_get_parallelheapscandesc(TableScanDesc sscan)
{
	HeapScanDesc scan = (HeapScanDesc) sscan;

	return scan->rs_parallel;
}

static HeapPageScanDesc
heapam_get_heappagescandesc(TableScanDesc sscan)
{
	HeapScanDesc scan = (HeapScanDesc) sscan;

	return &scan->rs_pagescan;
}

static bool
heapam_fetch_tuple_from_offset(TableScanDesc sscan, BlockNumber blkno, OffsetNumber offset, TupleTableSlot *slot)
{
	HeapScanDesc scan = (HeapScanDesc) sscan;
	Page		dp;
	ItemId		lp;

	dp = (Page) BufferGetPage(scan->rs_scan.rs_cbuf);
	lp = PageGetItemId(dp, offset);
	Assert(ItemIdIsNormal(lp));

	scan->rs_ctup.t_data = (HeapTupleHeader) PageGetItem((Page) dp, lp);
	scan->rs_ctup.t_len = ItemIdGetLength(lp);
	scan->rs_ctup.t_tableOid = scan->rs_scan.rs_rd->rd_id;
	ItemPointerSet(&scan->rs_ctup.t_self, blkno, offset);

	pgstat_count_heap_fetch(scan->rs_scan.rs_rd);

	slot->tts_tableOid = RelationGetRelid(sscan->rs_rd);
	ExecStoreTuple(&scan->rs_ctup, slot, scan->rs_scan.rs_cbuf, false);

	return true;
}

Size
heapam_storage_shmem_size()
{
	return SyncScanShmemSize();
}

void
heapam_storage_shmem_init()
{
	return SyncScanShmemInit();
}

HeapTuple
heap_scan_getnext(TableScanDesc sscan, ScanDirection direction)
{
	if (sscan->rs_rd->rd_rel->relam != HEAP_TABLE_AM_OID)
		ereport(PANIC, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("only heap AM is supported")));
	return heap_getnext(sscan, direction);
}

static bool
heapam_satisfies(TupleTableSlot *slot, Snapshot snapshot)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;
	bool res;

	/*
	 * We need buffer pin and lock to call HeapTupleSatisfiesVisibility.
	 * Caller should be holding pin, but not lock.
	 */
	LockBuffer(bslot->buffer, BUFFER_LOCK_SHARE);
	res = HeapTupleSatisfies(bslot->base.tuple, snapshot, bslot->buffer);
	LockBuffer(bslot->buffer, BUFFER_LOCK_UNLOCK);

	return res;
}

static HTSU_Result
heapam_satisfies_update(TupleTableSlot *slot, CommandId curcid)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;
	HTSU_Result res;

	LockBuffer(bslot->buffer, BUFFER_LOCK_SHARE);
	res = HeapTupleSatisfiesUpdate(bslot->base.tuple,
								   curcid,
								   bslot->buffer);
	LockBuffer(bslot->buffer, BUFFER_LOCK_UNLOCK);

	return res;
}

static HTSV_Result
heapam_satisfies_vacuum(TupleTableSlot *slot, TransactionId OldestXmin)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;
	HTSV_Result res;

	LockBuffer(bslot->buffer, BUFFER_LOCK_SHARE);
	res = HeapTupleSatisfiesVacuum(bslot->base.tuple,
								   OldestXmin,
								   bslot->buffer);
	LockBuffer(bslot->buffer, BUFFER_LOCK_UNLOCK);

	return res;
}

static IndexFetchTableData*
heapam_begin_index_fetch(Relation rel)
{
	IndexFetchHeapData *hscan = palloc0(sizeof(IndexFetchHeapData));

	hscan->xs_base.rel = rel;
	hscan->xs_cbuf = InvalidBuffer;
	//hscan->xs_continue_hot = false;

	return &hscan->xs_base;
}


static void
heapam_reset_index_fetch(IndexFetchTableData* scan)
{
	IndexFetchHeapData *hscan = (IndexFetchHeapData *) scan;

	if (BufferIsValid(hscan->xs_cbuf))
	{
		ReleaseBuffer(hscan->xs_cbuf);
		hscan->xs_cbuf = InvalidBuffer;
	}

	//hscan->xs_continue_hot = false;
}

static void
heapam_end_index_fetch(IndexFetchTableData* scan)
{
	IndexFetchHeapData *hscan = (IndexFetchHeapData *) scan;

	heapam_reset_index_fetch(scan);

	pfree(hscan);
}

static bool
heapam_fetch_follow(struct IndexFetchTableData *scan,
					ItemPointer tid,
					Snapshot snapshot,
					TupleTableSlot *slot,
					bool *call_again, bool *all_dead)
{
	IndexFetchHeapData *hscan = (IndexFetchHeapData *) scan;
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;
	bool got_heap_tuple;

	/* We can skip the buffer-switching logic if we're in mid-HOT chain. */
	if (!*call_again)
	{
		/* Switch to correct buffer if we don't have it already */
		Buffer		prev_buf = hscan->xs_cbuf;

		hscan->xs_cbuf = ReleaseAndReadBuffer(hscan->xs_cbuf,
											  hscan->xs_base.rel,
											  ItemPointerGetBlockNumber(tid));

		/*
		 * Prune page, but only if we weren't already on this page
		 */
		if (prev_buf != hscan->xs_cbuf)
			heap_page_prune_opt(hscan->xs_base.rel, hscan->xs_cbuf);
	}

	/* Obtain share-lock on the buffer so we can examine visibility */
	LockBuffer(hscan->xs_cbuf, BUFFER_LOCK_SHARE);
	got_heap_tuple = heap_hot_search_buffer(tid,
											hscan->xs_base.rel,
											hscan->xs_cbuf,
											snapshot,
											&bslot->base.tupdata,
											all_dead,
											!*call_again);
	bslot->base.tupdata.t_self = *tid;
	LockBuffer(hscan->xs_cbuf, BUFFER_LOCK_UNLOCK);

	if (got_heap_tuple)
	{
		/*
		 * Only in a non-MVCC snapshot can more than one member of the HOT
		 * chain be visible.
		 */
		*call_again = !IsMVCCSnapshot(snapshot);
		// FIXME pgstat_count_heap_fetch(scan->indexRelation);

		slot->tts_tableOid = RelationGetRelid(scan->rel);
		ExecStoreTuple(&bslot->base.tupdata, slot, hscan->xs_cbuf, false);
	}
	else
	{
		/* We've reached the end of the HOT chain. */
		*call_again = false;
	}

	return got_heap_tuple;
}

Datum
heap_tableam_handler(PG_FUNCTION_ARGS)
{
	TableAmRoutine *amroutine = makeNode(TableAmRoutine);

	amroutine->gimmegimmeslot = heapam_gimmegimmeslot;

	amroutine->snapshot_satisfies = heapam_satisfies;
	amroutine->snapshot_satisfiesUpdate = heapam_satisfies_update;
	amroutine->snapshot_satisfiesVacuum = heapam_satisfies_vacuum;

	amroutine->scan_begin = heap_beginscan;
	amroutine->scansetlimits = heap_setscanlimits;
	amroutine->scan_getnextslot = heap_getnextslot;
	amroutine->scan_end = heap_endscan;
	amroutine->scan_rescan = heap_rescan;
	amroutine->scan_update_snapshot = heap_update_snapshot;

	amroutine->scan_fetch_tuple_from_offset = heapam_fetch_tuple_from_offset;

	/*
	 * The following routine needs to be provided when the storage support
	 * parallel sequential scan
	 */
	amroutine->scan_get_parallelheapscandesc = heapam_get_parallelheapscandesc;

	/*
	 * The following routine needs to be provided when the storage support
	 * BitmapHeap and Sample Scans
	 */
	amroutine->scan_get_heappagescandesc = heapam_get_heappagescandesc;
	amroutine->sync_scan_report_location = ss_report_location;

	amroutine->tuple_fetch_row_version = heapam_fetch_row_version;
	amroutine->tuple_fetch_follow = heapam_fetch_follow;
	amroutine->tuple_insert = heapam_heap_insert;
	amroutine->tuple_insert_speculative = heapam_heap_insert_speculative;
	amroutine->tuple_complete_speculative = heapam_heap_complete_speculative;
	amroutine->tuple_delete = heapam_heap_delete;
	amroutine->tuple_update = heapam_heap_update;
	amroutine->tuple_lock = heapam_lock_tuple;
	amroutine->multi_insert = heap_multi_insert;

	amroutine->get_tuple_data = heapam_get_tuple_data;
	amroutine->tuple_get_latest_tid = heap_get_latest_tid;

	amroutine->relation_vacuum = heap_vacuum_rel;
	amroutine->relation_sync = heap_sync;

	amroutine->getbulkinsertstate = GetBulkInsertState;
	amroutine->freebulkinsertstate = FreeBulkInsertState;
	amroutine->releasebulkinsertstate = ReleaseBulkInsertStatePin;

	amroutine->begin_heap_rewrite = begin_heap_rewrite;
	amroutine->end_heap_rewrite = end_heap_rewrite;
	amroutine->rewrite_heap_tuple = rewrite_heap_tuple;
	amroutine->rewrite_heap_dead_tuple = rewrite_heap_dead_tuple;

	amroutine->begin_index_fetch = heapam_begin_index_fetch;
	amroutine->reset_index_fetch = heapam_reset_index_fetch;
	amroutine->end_index_fetch = heapam_end_index_fetch;
	PG_RETURN_POINTER(amroutine);
}
