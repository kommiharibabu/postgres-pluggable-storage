/*-------------------------------------------------------------------------
 *
 * execTuples.c
 *	  Routines dealing with TupleTableSlots.  These are used for resource
 *	  management associated with tuples (eg, releasing buffer pins for
 *	  tuples in disk buffers, or freeing the memory occupied by transient
 *	  tuples).  Slots also provide access abstraction that lets us implement
 *	  "virtual" tuples to reduce data-copying overhead.
 *
 *	  Routines dealing with the type information for tuples. Currently,
 *	  the type information for a tuple is an array of FormData_pg_attribute.
 *	  This information is needed by routines manipulating tuples
 *	  (getattribute, formtuple, etc.).
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execTuples.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *
 *	 SLOT CREATION/DESTRUCTION
 *		MakeTupleTableSlot		- create an empty slot
 *		ExecAllocTableSlot		- create a slot within a tuple table
 *		ExecResetTupleTable		- clear and optionally delete a tuple table
 *		MakeSingleTupleTableSlot - make a standalone slot, set its descriptor
 *		ExecDropSingleTupleTableSlot - destroy a standalone slot
 *
 *	 SLOT ACCESSORS
 *		ExecSetSlotDescriptor	- set a slot's tuple descriptor
 *		ExecStoreTuple			- store a physical tuple in the slot
 *		ExecStoreMinimalTuple	- store a minimal physical tuple in the slot
 *		ExecClearTuple			- clear contents of a slot
 *		ExecStoreVirtualTuple	- mark slot as containing a virtual tuple
 *		ExecCopySlotTuple		- build a physical tuple from a slot
 *		ExecCopySlotMinimalTuple - build a minimal physical tuple from a slot
 *		ExecMaterializeSlot		- convert virtual to physical storage
 *		ExecCopySlot			- copy one slot's contents to another
 *
 *	 CONVENIENCE INITIALIZATION ROUTINES
 *		ExecInitResultTupleSlot    \	convenience routines to initialize
 *		ExecInitScanTupleSlot		\	the various tuple slots for nodes
 *		ExecInitExtraTupleSlot		/	which store copies of tuples.
 *		ExecInitNullTupleSlot	   /
 *
 *	 Routines that probably belong somewhere else:
 *		ExecTypeFromTL			- form a TupleDesc from a target list
 *
 *	 EXAMPLE OF HOW TABLE ROUTINES WORK
 *		Suppose we have a query such as SELECT emp.name FROM emp and we have
 *		a single SeqScan node in the query plan.
 *
 *		At ExecutorStart()
 *		----------------
 *		- ExecInitSeqScan() calls ExecInitScanTupleSlot() and
 *		  ExecInitResultTupleSlotTL() to construct TupleTableSlots
 *		  for the tuples returned by the access methods and the
 *		  tuples resulting from performing target list projections.
 *
 *		During ExecutorRun()
 *		----------------
 *		- SeqNext() calls ExecStoreTuple() to place the tuple returned
 *		  by the access methods into the scan tuple slot.
 *
 *		- ExecSeqScan() calls ExecStoreTuple() to take the result
 *		  tuple from ExecProject() and place it into the result tuple slot.
 *
 *		- ExecutePlan() calls the output function.
 *
 *		The important thing to watch in the executor code is how pointers
 *		to the slots containing tuples are passed instead of the tuples
 *		themselves.  This facilitates the communication of related information
 *		(such as whether or not a tuple should be pfreed, what buffer contains
 *		this tuple, the tuple's tuple descriptor, etc).  It also allows us
 *		to avoid physically constructing projection tuples in many cases.
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/tupdesc_details.h"
#include "access/tuptoaster.h"
#include "funcapi.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/datum.h" /* TODO: Included for datumCopy(), remove if that function is not used. */
#include "utils/lsyscache.h"
#include "utils/typcache.h"


static TupleDesc ExecTypeFromTLInternal(List *targetList,
					   bool hasoid, bool skipjunk);
static void slot_deform_tuple_ex(TupleTableSlot *slot, HeapTuple tuple,
					 uint32 *offp, int attnum);


/*
 * Fill in missing values for a TupleTableSlot.
 *
 * This is only exposed because it's needed for JIT compiled tuple
 * deforming. That exception aside, there should be no callers outside of this
 * file.
 */
void
slot_getmissingattrs(TupleTableSlot *slot, int startAttNum, int lastAttNum)
{
	AttrMissing *attrmiss = NULL;
	int			missattnum;

	if (slot->tts_tupleDescriptor->constr)
		attrmiss = slot->tts_tupleDescriptor->constr->missing;

	if (!attrmiss)
	{
		/* no missing values array at all, so just fill everything in as NULL */
		memset(slot->tts_values + startAttNum, 0,
			   (lastAttNum - startAttNum) * sizeof(Datum));
		memset(slot->tts_isnull + startAttNum, 1,
			   (lastAttNum - startAttNum) * sizeof(bool));
	}
	else
	{
		/* if there is a missing values array we must process them one by one */
		for (missattnum = startAttNum;
			 missattnum < lastAttNum;
			 missattnum++)
		{
			slot->tts_values[missattnum] = attrmiss[missattnum].am_value;
			slot->tts_isnull[missattnum] = !attrmiss[missattnum].am_present;
		}
	}
}

/*
 * TODO: rename this better. This acts as wrapper, which replaces earlier
 * slot_getallattrs() or slot_getsomeattrs(), around slot_deform_tuple().
 */
static void
slot_deform_tuple_ex(TupleTableSlot *slot, HeapTuple tuple, uint32 *offp,
					 int attnum)
{
	int			attno;

	/* Quick out if we have 'em all already */
	if (slot->tts_nvalid >= attnum)
		return;

	/* Check for caller error */
	if (attnum <= 0 || attnum > slot->tts_tupleDescriptor->natts)
		elog(ERROR, "invalid attribute number %d", attnum);

	/*
	 * otherwise we had better have a physical tuple (tts_nvalid should equal
	 * natts in all virtual-tuple cases)
	 */
	if (tuple == NULL)			/* internal error */
		elog(ERROR, "cannot extract attribute from empty tuple slot");

	/*
	 * load up any slots available from physical tuple
	 */
	attno = HeapTupleHeaderGetNatts(tuple->t_data);
	attno = Min(attno, attnum);

	slot_deform_tuple(slot, tuple, offp, attno);

	attno = slot->tts_nvalid;

	/*
	 * If tuple doesn't have all the atts indicated by attnum, read the rest
	 * as NULLs or missing values
	 */
	if (attno < attnum)
		slot_getmissingattrs(slot, attno, attnum);

	slot->tts_nvalid = attnum;
}

/*
 * TupleTableSlotOps implementations.
 */

/*
 * TupleTableSlotOps implementation for VirtualTupleTableSlot.
 */
static void
tts_virtual_init(TupleTableSlot *slot)
{
}

static void
tts_virtual_release(TupleTableSlot *slot)
{
}

/*
 * TODO: If the datums were deep copied, we need to free those.
 */
static void
tts_virtual_clear(TupleTableSlot *slot)
{
	slot->tts_nvalid = 0;
	SET_TTS_EMPTY(slot);
}

/*
 * Attribute values are readily available in tts_values and tts_isnull array
 * in a VirtualTupleTableSlot. So there should be no need to call this
 * function.
 */
static void
tts_virtual_getsomeattrs(TupleTableSlot *slot, int natts)
{
	elog(ERROR, "getsomeattrs is not required to be called on a virtual tuple table slot");
}

/*
 * VirtualTupleTableSlot stores the individual attribute values in tts_values
 * and tts_isnull arrays. Unlike other TupleTableSlot types it does not have
 * any separate storage to materialize into. Hence it doesn't support
 * materialize method.
 *
 * TODO:
 * But we might want to save the contents of tts_values and tts_isnull into the
 * memory context of a VirtualTupleTableSlot when requested. But as of now, we
 * do not see any need for the same.
 */
static void
tts_virtual_materialize(TupleTableSlot *slot)
{
	elog(ERROR, "materializing a virtual tuple is not supported");
}

/*
 * TODO: Earlier versions of ExecMaterializeSlot(), which used to return a heap
 * tuple combing tts_values and tts_isnull, the heap tuple once created was
 * cached and reused till it was freed. But in this call we are creating a new
 * heap tuple every time. This might leak memory. Do we want to cache the
 * created heap tuple in the memory context specific for this slot? In order to
 * do that, we need to answer the question as to whether a heap tuple can be
 * requested multiple times from a VirtualTupleTableSlot.
 *
 * Similarly for tts_virtual_get_minimal_tuple().
 */
static HeapTuple
tts_virtual_get_heap_tuple(TupleTableSlot *slot)
{
	Assert(!IS_TTS_EMPTY(slot));

	return heap_form_tuple(slot->tts_tupleDescriptor,
						   slot->tts_values,
						   slot->tts_isnull);

}

static MinimalTuple
tts_virtual_get_minimal_tuple(TupleTableSlot *slot)
{
	Assert(!IS_TTS_EMPTY(slot));

	return heap_form_minimal_tuple(slot->tts_tupleDescriptor,
								   slot->tts_values,
								   slot->tts_isnull);
}

static HeapTuple
tts_virtual_copy_heap_tuple(TupleTableSlot *slot)
{
	Assert(!IS_TTS_EMPTY(slot));

	return heap_form_tuple(slot->tts_tupleDescriptor,
						   slot->tts_values,
						   slot->tts_isnull);

}

static MinimalTuple
tts_virtual_copy_minimal_tuple(TupleTableSlot *slot)
{
	Assert(!IS_TTS_EMPTY(slot));

	return heap_form_minimal_tuple(slot->tts_tupleDescriptor,
								   slot->tts_values,
								   slot->tts_isnull);
}

/*
 * TupleTableSlotOps implementation for HeapTupleTableSlot.
 */

static void
tts_heap_init(TupleTableSlot *slot)
{
}

static void
tts_heap_release(TupleTableSlot *slot)
{
}

static void
tts_heap_clear(TupleTableSlot *slot)
{
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;

	/* Free the memory for the heap tuple if it's allowed. */
	if (IS_TTS_SHOULDFREE(slot))
	{
		heap_freetuple(hslot->tuple);
		RESET_TTS_SHOULDFREE(slot);
	}

	slot->tts_nvalid = 0;
	SET_TTS_EMPTY(slot);
	hslot->off = 0;
	hslot->tuple = NULL;
}

static void
tts_heap_getsomeattrs(TupleTableSlot *slot, int natts)
{
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;

	Assert(!IS_TTS_EMPTY(slot));

	slot_deform_tuple_ex(slot, hslot->tuple, &hslot->off, natts);
}

/*
 * Materialize the heap tuple contained in the given slot into its own memory
 * context.
 */
static void
tts_heap_materialize(TupleTableSlot *slot)
{
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;
	MemoryContext oldContext;

	Assert(!IS_TTS_EMPTY(slot));

	/* This slot has it's tuple already materialized. Nothing to do. */
	if (IS_TTS_SHOULDFREE(slot))
		return;

	SET_TTS_SHOULDFREE(slot);
	oldContext = MemoryContextSwitchTo(slot->tts_mcxt);

	if (!hslot->tuple)
		hslot->tuple = heap_form_tuple(slot->tts_tupleDescriptor,
									   slot->tts_values,
									   slot->tts_isnull);
	else
	{
		/*
		 * The tuple contained in this slot is not allocated in the memory
		 * context of the given slot (else it would have TTS_SHOULDFREE set).
		 * Copy the tuple into the given slot's memory context.
		 */
		hslot->tuple = heap_copytuple(hslot->tuple);
	}

	slot->tts_nvalid = 0;
	hslot->off = 0;

	MemoryContextSwitchTo(oldContext);
}

/*
 * Return the heap tuple in the slot as is if it contains one. Otherwise,
 * materialize a heap tuple using contents of the slot and return it.
 */
static HeapTuple
tts_heap_get_heap_tuple(TupleTableSlot *slot)
{
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;

	Assert(!IS_TTS_EMPTY(slot));
	if (!hslot->tuple)
		tts_heap_materialize(slot);

	return hslot->tuple;
}

/*
 * Return a minimal tuple constructed from the contents of the slot.
 *
 * TODO: This function will always construct a new minimal tuple and return it.
 * If called more than once on the given slot without changing the contents, we
 * will leak memory. Need to figure out whether that's true.
 */
static MinimalTuple
tts_heap_get_minimal_tuple(TupleTableSlot *slot)
{
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;

	Assert(!IS_TTS_EMPTY(slot));
	if (!hslot->tuple)
		tts_heap_materialize(slot);

	return minimal_tuple_from_heap_tuple(hslot->tuple);
}

/*
 * Return a copy of heap tuple contained in the slot, materialising one if
 * necessary.
 */
static HeapTuple
tts_heap_copy_heap_tuple(TupleTableSlot *slot)
{
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;

	Assert(!IS_TTS_EMPTY(slot));
	if (!hslot->tuple)
		tts_heap_materialize(slot);

	return heap_copytuple(hslot->tuple);
}

/*
 * Return a minimal tuple constructed from the contents of the slot.
 *
 * We always return a new minimal tuple so no copy, per say, is needed.
 *
 * TODO:
 * This function is exact copy of tts_heap_get_minimal_tuple() and thus the
 * callback should point to that one instead of a new implementation. But
 * there's one TODO there which might change tts_heap_get_minimal_tuple().
 */
static MinimalTuple
tts_heap_copy_minimal_tuple(TupleTableSlot *slot)
{
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;

	if (!hslot->tuple)
		tts_heap_materialize(slot);

	return minimal_tuple_from_heap_tuple(hslot->tuple);
}

/*
 * Store the given tuple into the given HeapTupleTableSlot. If the slot
 * already contains a tuple, we will free the memory for the same before
 * storing a new one there.
 */
static void
tts_heap_store_tuple(TupleTableSlot *slot, HeapTuple tuple, bool shouldFree)
{
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;

	tts_heap_clear(slot);

	slot->tts_nvalid = 0;
	hslot->tuple = tuple;
	hslot->off = 0;
	RESET_TTS_EMPTY(slot);

	if (shouldFree)
		SET_TTS_SHOULDFREE(slot);
}


/*
 * TODO:
 * We set mslot->tuple = &mslot->minhdr everywhere when we store a minimal
 * tuple in the slot or localize it. So, either that statement is not needed
 * here OR it's not needed in those functions.
 *
 */
static void
tts_minimal_init(TupleTableSlot *slot)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;

	mslot->tuple = &mslot->minhdr;
}

static void
tts_minimal_release(TupleTableSlot *slot)
{
}

static void
tts_minimal_clear(TupleTableSlot *slot)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;

	/* FIXME */
	/*
	 * TODO: a minimal tuple never comes from a buffer, and hence it should be
	 * fine to free the memory unconditionally. That means we don't need
	 * SHOULDFREE flag on the minimal tuple.
	 */
	if (IS_TTS_SHOULDFREE(slot))
	{
		heap_free_minimal_tuple(mslot->mintuple);
		RESET_TTS_SHOULDFREE(slot);
	}

	slot->tts_nvalid = 0;
	SET_TTS_EMPTY(slot);
	mslot->off = 0;
	mslot->mintuple = NULL;
}

static void
tts_minimal_getsomeattrs(TupleTableSlot *slot, int natts)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;

	Assert(!IS_TTS_EMPTY(slot));

	slot_deform_tuple_ex(slot, mslot->tuple, &mslot->off, natts);
}

/*
 * TODO:
 * We don't expect a minimal tuple to be materialized into a heap tuple as
 * such, but we do expect a minimal tuple to be saved into its own storage.
 * That's being done by tts_minimal_localize() call below, which is used at
 * various places. Earlier ExecMaterializeSlot() would not materialize the
 * contents of slot into a minimal tuple. From that point of view we don't
 * expect to materialize a minimal tuple, but then that time there weren't
 * different slot types. Now that we are replacing ExecMaterializeSlot() by
 * ExecGetHeapTupleFromSlot() or something like that, may be we could use
 * "materialize" to mean creating a minimal tuple in slot's own context.
 */
static void
tts_minimal_materialize(TupleTableSlot *slot)
{
	elog(ERROR, "materializing a minimal tuple");
}

/*
 * Materialize the minimal tuple contained in the given slot into its own
 * memory context.
 */
static void
tts_minimal_localize(TupleTableSlot *slot)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;
	MemoryContext oldContext;

	Assert(!IS_TTS_EMPTY(slot));

	/* This slot has it's tuple already materialized. Nothing to do. */
	if (IS_TTS_SHOULDFREE(slot))
		return;

	SET_TTS_SHOULDFREE(slot);
	oldContext = MemoryContextSwitchTo(slot->tts_mcxt);

	if (!mslot->mintuple)
		mslot->mintuple = heap_form_minimal_tuple(slot->tts_tupleDescriptor,
												  slot->tts_values,
												  slot->tts_isnull);
	else
	{
		/*
		 * The minimal tuple contained in this slot is not allocated in the
		 * memory context of the given slot (else it would have TTS_SHOULDFREE
		 * set).  Copy the minimal tuple into the given slot's memory context.
		 */
		mslot->mintuple = heap_copy_minimal_tuple(mslot->mintuple);
	}
	mslot->tuple = &mslot->minhdr;
	mslot->minhdr.t_len = mslot->mintuple->t_len + MINIMAL_TUPLE_OFFSET;
	mslot->minhdr.t_data = (HeapTupleHeader) ((char *) mslot->mintuple - MINIMAL_TUPLE_OFFSET);

	MemoryContextSwitchTo(oldContext);

	slot->tts_nvalid = 0;
	mslot->off = 0;
}

/*
 * Return the heap tuple equivalent to the minimal tuple contained in the slot,
 * materializing the contents into a minimal tuple if necessary.
 */
static HeapTuple
tts_minimal_get_heap_tuple(TupleTableSlot *slot)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;

	Assert(!IS_TTS_EMPTY(slot));
	if (!mslot->mintuple)
		tts_minimal_localize(slot);

	/*
	 * TODO
	 * A minimal tuple is only allocated "len" number of bytes whereas a heap
	 * tuple is allocated "len" + HEAPTUPLESIZE bytes. So, just returning the
	 * address of minhdr doesn't look sufficient. We can replace such instances
	 * by CopyHeapTuple request instead but have not verified the feasibility.
	 * Also this means that we will create a new heap tuple every time this
	 * function is called even when the slot contents do not change. Need to
	 * see if that's possible.
	 */
	return heap_tuple_from_minimal_tuple(mslot->mintuple);
}

/*
 * Return the minimal tuple if slot contains one. Otherwise materialize the
 * contents of slot into a minimal tuple and return that.
 */
static MinimalTuple
tts_minimal_get_minimal_tuple(TupleTableSlot *slot)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;

	Assert(!IS_TTS_EMPTY(slot));

	/* See note at tts_minimal_localize. */
	if (!mslot->mintuple)
		tts_minimal_localize(slot);

	return mslot->mintuple;
}

/*
 * Return a heap tuple constructed from the minimal tuple contained in the slot.
 *
 * We always construct a new heap tuple, so there is nothing to copy as such.
 *
 * TODO:
 * this function is exactly same as tts_minimal_get_heap_tuple, so may be
 * callback can point to that function instead of writing exactly same
 * implementation. But there's TODO in that function which can change things.
 */
static HeapTuple
tts_minimal_copy_heap_tuple(TupleTableSlot *slot)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;

	/* Materialize the minimal tuple if not already present. */
	if (!mslot->mintuple)
		tts_minimal_localize(slot);

	return heap_tuple_from_minimal_tuple(mslot->mintuple);
}

/*
 * Return a copy of minimal tuple contained in the slot, materializing one if
 * necessary.
 */
static MinimalTuple
tts_minimal_copy_minimal_tuple(TupleTableSlot *slot)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;

	if (!mslot->mintuple)
		tts_minimal_localize(slot);

	return heap_copy_minimal_tuple(mslot->mintuple);
}

/*
 * Store the given minimal tuple into the given MinimalTupleTableSlot. If the
 * slot already contains a tuple, we will free the memory for the same before
 * storing a new one there.
 */
static void
tts_minimal_store_tuple(TupleTableSlot *slot, MinimalTuple mtup, bool shouldFree)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;

	tts_minimal_clear(slot);

	RESET_TTS_EMPTY(slot);
	slot->tts_nvalid = 0;
	mslot->off = 0;

	mslot->mintuple = mtup;
	mslot->tuple = &mslot->minhdr;
	mslot->minhdr.t_len = mtup->t_len + MINIMAL_TUPLE_OFFSET;
	mslot->minhdr.t_data = (HeapTupleHeader) ((char *) mtup - MINIMAL_TUPLE_OFFSET);
	/* no need to set t_self or t_tableOid since we won't allow access */

	if (shouldFree)
		SET_TTS_SHOULDFREE(slot);
	else
		Assert(!IS_TTS_SHOULDFREE(slot));
}

/*
 * TupleTableSlotOps implementation for BufferHeapTupleTableSlot.
 */

static void
tts_buffer_init(TupleTableSlot *slot)
{
}

static void
tts_buffer_release(TupleTableSlot *slot)
{
}

static void
tts_buffer_clear(TupleTableSlot *slot)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;

	/*
	 * Free the memory for heap tuple if allowed. A tuple coming from buffer
	 * can never be freed. But we may have materialized a tuple from buffer.
	 * Such a tuple can be freed.
	 */
	if (IS_TTS_SHOULDFREE(slot))
	{
		/* We should have unpinned the buffer while materializing the tuple. */
		Assert(!BufferIsValid(bslot->buffer));

		heap_freetuple(bslot->base.tuple);
		RESET_TTS_SHOULDFREE(slot);
	}

	if (BufferIsValid(bslot->buffer))
		ReleaseBuffer(bslot->buffer);

	slot->tts_nvalid = 0;
	SET_TTS_EMPTY(slot);
	bslot->base.tuple = NULL;
	bslot->buffer = InvalidBuffer;
}

static void
tts_buffer_getsomeattrs(TupleTableSlot *slot, int natts)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;

	Assert(!IS_TTS_EMPTY(slot));

	slot_deform_tuple_ex(slot, bslot->base.tuple, &bslot->base.off, natts);
}

/*
 * Materialize the heap tuple contained in the given slot into its own memory
 * context.
 */
static void
tts_buffer_materialize(TupleTableSlot *slot)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;
	MemoryContext oldContext;

	Assert(!IS_TTS_EMPTY(slot));

	/* If already materialized nothing to do. */
	if (IS_TTS_SHOULDFREE(slot))
		return;

	SET_TTS_SHOULDFREE(slot);
	oldContext = MemoryContextSwitchTo(slot->tts_mcxt);

	if (!bslot->base.tuple)
	{
		/*
		 * TODO: I would expect a BufferHeapTupleTableSlot to have a tuple
		 * first which then gets deformed into values and isnull. But I do not
		 * see a scenario when we would materialize a tuple from values and
		 * isnull.
		 */
		bslot->base.tuple = heap_form_tuple(slot->tts_tupleDescriptor,
											slot->tts_values,
											slot->tts_isnull);
	}
	else
		bslot->base.tuple = heap_copytuple(bslot->base.tuple);
	MemoryContextSwitchTo(oldContext);

	/*
	 * TODO: I expect a BufferHeapTupleTableSlot to always have a buffer to be
	 * associated with it OR the tuple is materialized. In the later case we
	 * won't come here. So, we should always see a valid buffer here to be
	 * unpinned.
	 */
	if (BufferIsValid(bslot->buffer))
	{
		ReleaseBuffer(bslot->buffer);
		bslot->buffer = InvalidBuffer;
	}

	bslot->base.off = 0;
	slot->tts_nvalid = 0;
}

/*
 * Return the heap tuple in the slot as is if it contains one. Otherwise,
 * materialize a heap tuple using contents of the slot and return it.
 */
static HeapTuple
tts_buffer_get_heap_tuple(TupleTableSlot *slot)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;

	Assert(!IS_TTS_EMPTY(slot));

	/* Is this even possible for a buffer tuple? */
	if (!bslot->base.tuple)
		tts_buffer_materialize(slot);

	return bslot->base.tuple;
}

/*
 * Return a minimal tuple constructed from the contents of the slot.
 *
 * TODO: This function will always construct a new minimal tuple and return it.
 * If called more than once on the given slot without changing the contents, we
 * will leak memory. Need to figure out whether that's true.
 *
 * TODO: Also need to check whether just pointer swizzling work here.
 */
static MinimalTuple
tts_buffer_get_minimal_tuple(TupleTableSlot *slot)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;

	Assert(!IS_TTS_EMPTY(slot));

	/* Is this even possible for a buffer tuple? */
	if (!bslot->base.tuple)
		tts_buffer_materialize(slot);

	return minimal_tuple_from_heap_tuple(bslot->base.tuple);
}

/*
 * Return a copy of heap tuple contained in the slot, materialising one if
 * necessary.
 */
static HeapTuple
tts_buffer_copy_heap_tuple(TupleTableSlot *slot)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;

	Assert(!IS_TTS_EMPTY(slot));

	/* Is this even possible for a buffer tuple? */
	if (!bslot->base.tuple)
		tts_buffer_materialize(slot);

	return heap_copytuple(bslot->base.tuple);
}

/*
 * Return a minimal tuple constructed from the contents of the slot.
 *
 * We always return a new minimal tuple so no copy, per say, is needed.
 *
 * TODO:
 * This function is exact copy of tts_buffer_get_minimal_tuple() and thus the
 * callback should point to that one instead of a new implementation. But
 * there's one TODO there which might change tts_heap_get_minimal_tuple().
 */
static MinimalTuple
tts_buffer_copy_minimal_tuple(TupleTableSlot *slot)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;

	Assert(!IS_TTS_EMPTY(slot));

	/* Is this even possible for a buffer tuple? */
	if (!bslot->base.tuple)
		tts_buffer_materialize(slot);

	return minimal_tuple_from_heap_tuple(bslot->base.tuple);
}

/*
 * Store the given tuple into the given BufferHeapTupleTableSlot and pin the
 * given buffer. If the tuple already contained in the slot can be freed free
 * it.
 */
static void
tts_buffer_store_tuple(TupleTableSlot *slot, HeapTuple tuple, Buffer buffer)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;

	if (IS_TTS_SHOULDFREE(slot))
	{
		/*
		 * TODO: no buffer should be associated with the tuple in this case.
		 * See tts_buffer_clear() for more.
		 */
		heap_freetuple(bslot->base.tuple);
		RESET_TTS_SHOULDFREE(slot);
	}

	RESET_TTS_EMPTY(slot);
	slot->tts_nvalid = 0;
	bslot->base.tuple = tuple;
	bslot->base.off = 0;

	/*
	 * If tuple is on a disk page, keep the page pinned as long as we hold a
	 * pointer into it.  We assume the caller already has such a pin.
	 *
	 * This is coded to optimize the case where the slot previously held a
	 * tuple on the same disk page: in that case releasing and re-acquiring
	 * the pin is a waste of cycles.  This is a common situation during
	 * seqscans, so it's worth troubling over.
	 */
	if (bslot->buffer != buffer)
	{
		if (BufferIsValid(bslot->buffer))
			ReleaseBuffer(bslot->buffer);
		bslot->buffer = buffer;
		IncrBufferRefCount(buffer);
	}
}

/*
 * TupleTableSlotOps for each of TupleTableSlotTypes. These are used to
 * identify the type of slot.
 */
const TupleTableSlotOps TTSOpsVirtual = {
	tts_virtual_init,
	tts_virtual_release,
	tts_virtual_clear,
	tts_virtual_getsomeattrs,
	tts_virtual_materialize,
	tts_virtual_get_heap_tuple,
	tts_virtual_get_minimal_tuple,
	tts_virtual_copy_heap_tuple,
	tts_virtual_copy_minimal_tuple,
};

const TupleTableSlotOps TTSOpsHeapTuple = {
	tts_heap_init,
	tts_heap_release,
	tts_heap_clear,
	tts_heap_getsomeattrs,
	tts_heap_materialize,
	tts_heap_get_heap_tuple,
	tts_heap_get_minimal_tuple,
	tts_heap_copy_heap_tuple,
	tts_heap_copy_minimal_tuple,
};

const TupleTableSlotOps TTSOpsMinimalTuple = {
	tts_minimal_init,
	tts_minimal_release,
	tts_minimal_clear,
	tts_minimal_getsomeattrs,
	tts_minimal_materialize,
	tts_minimal_get_heap_tuple,
	tts_minimal_get_minimal_tuple,
	tts_minimal_copy_heap_tuple,
	tts_minimal_copy_minimal_tuple,
};

const TupleTableSlotOps TTSOpsBufferTuple = {
	tts_buffer_init,
	tts_buffer_release,
	tts_buffer_clear,
	tts_buffer_getsomeattrs,
	tts_buffer_materialize,
	tts_buffer_get_heap_tuple,
	tts_buffer_get_minimal_tuple,
	tts_buffer_copy_heap_tuple,
	tts_buffer_copy_minimal_tuple,
};

/* ----------------------------------------------------------------
 *				  tuple table create/delete functions
 * ----------------------------------------------------------------
 */

/* --------------------------------
 *		MakeTupleTableSlot
 *
 *		Basic routine to make an empty TupleTableSlot of given
 *		TupleTableSlotType. If tupleDesc is specified the slot's descriptor is
 *		fixed for it's lifetime, gaining some efficiency. If that's
 *		undesirable, pass NULL.
 *
 *		TODO: This work allows a TupleTableSlot to hold a tuple of format other
 *		than heap or minimal heap tuple format. Such tuple formats are required
 *		for pluggable storage. In that context having TupleTableSlotType as an
 *		enum doesn't work since an extension or pluggable code can not add new
 *		members to an enum. TupleTableSlotType argument to this function is
 *		used to set appropriate set of callbacks (TupleTableSlotOps) and decide
 *		the minimum size of TupleTableSlot being created. So instead of passing
 *		TupleTableSlotType, we could instead pass the set of callbacks and the
 *		minimum size of TupleTableSlot to this function. There could be
 *		callback which returns the minimum size as well.
 * --------------------------------
 */
TupleTableSlot *
MakeTupleTableSlot(TupleDesc tupleDesc, TupleTableSlotType st)
{
	Size		basesz, allocsz;
	TupleTableSlot *slot;
	const TupleTableSlotOps *ops;

	switch (st)
	{
		case TTS_TYPE_VIRTUAL:
			ops = &TTSOpsVirtual;
			basesz = sizeof(TupleTableSlot);
			break;
		case TTS_TYPE_HEAPTUPLE:
			ops = &TTSOpsHeapTuple;
			basesz = sizeof(HeapTupleTableSlot);
			break;
		case TTS_TYPE_MINIMALTUPLE:
			ops = &TTSOpsMinimalTuple;
			basesz = sizeof(MinimalTupleTableSlot);
			break;
		case TTS_TYPE_BUFFER:
			ops = &TTSOpsBufferTuple;
			basesz = sizeof(BufferHeapTupleTableSlot);
			break;
	}

	/*
	 * XXX: Main body of this should be in separate initialization
	 * function so external types of slots can reuse.
	 */

	/*
	 * When a fixed descriptor is specified, we can reduce overhead by
	 * allocating the entire slot in one go.
	 */
	if (tupleDesc)
		allocsz = MAXALIGN(basesz) +
			MAXALIGN(tupleDesc->natts * sizeof(Datum)) +
			MAXALIGN(tupleDesc->natts * sizeof(bool));
	else
		allocsz = basesz;

	slot = palloc0(allocsz);
	*((const TupleTableSlotOps **) &slot->tts_cb) = ops;
	slot->type = T_TupleTableSlot;
	SET_TTS_EMPTY(slot);
	if (tupleDesc != NULL)
		SET_TTS_FIXED(slot);
	else
		RESET_TTS_FIXED(slot);
	slot->tts_tupleDescriptor = tupleDesc;
	slot->tts_mcxt = CurrentMemoryContext;
	slot->tts_nvalid = 0;

	if (tupleDesc != NULL)
	{
		slot->tts_values = (Datum *)
			(((char *) slot)
			 + MAXALIGN(basesz));
		slot->tts_isnull = (bool *)
			(((char *) slot)
			 + MAXALIGN(basesz)
			 + MAXALIGN(tupleDesc->natts * sizeof(Datum)));

		PinTupleDesc(tupleDesc);
	}

	/*
	 * And allow slot type specific initialization.
	 */
	slot->tts_cb->init(slot);

	return slot;
}

/* --------------------------------
 *		ExecAllocTableSlot
 *
 *		Create a tuple table slot within a tuple table (which is just a List).
 * --------------------------------
 */
TupleTableSlot *
ExecAllocTableSlot(List **tupleTable, TupleDesc desc, TupleTableSlotType st)
{
	TupleTableSlot *slot = MakeTupleTableSlot(desc, st);

	*tupleTable = lappend(*tupleTable, slot);

	return slot;
}

/* --------------------------------
 *		ExecResetTupleTable
 *
 *		This releases any resources (buffer pins, tupdesc refcounts)
 *		held by the tuple table, and optionally releases the memory
 *		occupied by the tuple table data structure.
 *		It is expected that this routine be called by EndPlan().
 * --------------------------------
 */
void
ExecResetTupleTable(List *tupleTable,	/* tuple table */
					bool shouldFree)	/* true if we should free memory */
{
	ListCell   *lc;

	foreach(lc, tupleTable)
	{
		TupleTableSlot *slot = lfirst_node(TupleTableSlot, lc);

		/* Always release resources and reset the slot to empty */
		ExecClearTuple(slot);
		if (slot->tts_tupleDescriptor)
		{
			ReleaseTupleDesc(slot->tts_tupleDescriptor);
			slot->tts_tupleDescriptor = NULL;
		}

		/* If shouldFree, release memory occupied by the slot itself */
		if (shouldFree)
		{
			if (!IS_TTS_FIXED(slot))
			{
				if (slot->tts_values)
					pfree(slot->tts_values);
				if (slot->tts_isnull)
					pfree(slot->tts_isnull);
			}
			pfree(slot);
		}
	}

	/* If shouldFree, release the list structure */
	if (shouldFree)
		list_free(tupleTable);
}

/* --------------------------------
 *		MakeSingleTupleTableSlot
 *
 *		This is a convenience routine for operations that need a standalone
 *		TupleTableSlot not gotten from the main executor tuple table.  It makes
 *		a single slot of given TupleTableSlotType and initializes it to use the
 *		given tuple descriptor.
 * --------------------------------
 */
TupleTableSlot *
MakeSingleTupleTableSlot(TupleDesc tupdesc, TupleTableSlotType st)
{
	TupleTableSlot *slot = MakeTupleTableSlot(tupdesc, st);

	return slot;
}

/* --------------------------------
 *		ExecDropSingleTupleTableSlot
 *
 *		Release a TupleTableSlot made with MakeSingleTupleTableSlot.
 *		DON'T use this on a slot that's part of a tuple table list!
 * --------------------------------
 */
void
ExecDropSingleTupleTableSlot(TupleTableSlot *slot)
{
	/*
	 * TODO: do we need this? Why is not this called from
	 * ExecResetTupleTable().
	 */
	slot->tts_cb->release(slot);

	/* This should match ExecResetTupleTable's processing of one slot */
	Assert(IsA(slot, TupleTableSlot));
	ExecClearTuple(slot);
	if (slot->tts_tupleDescriptor)
		ReleaseTupleDesc(slot->tts_tupleDescriptor);
	if (!IS_TTS_FIXED(slot))
	{
		if (slot->tts_values)
			pfree(slot->tts_values);
		if (slot->tts_isnull)
			pfree(slot->tts_isnull);
	}
	pfree(slot);
}


/* ----------------------------------------------------------------
 *				  tuple table slot accessor functions
 * ----------------------------------------------------------------
 */

/* --------------------------------
 *		ExecSetSlotDescriptor
 *
 *		This function is used to set the tuple descriptor associated
 *		with the slot's tuple.  The passed descriptor must have lifespan
 *		at least equal to the slot's.  If it is a reference-counted descriptor
 *		then the reference count is incremented for as long as the slot holds
 *		a reference.
 * --------------------------------
 */
void
ExecSetSlotDescriptor(TupleTableSlot *slot, /* slot to change */
					  TupleDesc tupdesc)	/* new tuple descriptor */
{
	Assert(!IS_TTS_FIXED(slot));

	/* For safety, make sure slot is empty before changing it */
	ExecClearTuple(slot);

	/*
	 * Release any old descriptor.  Also release old Datum/isnull arrays if
	 * present (we don't bother to check if they could be re-used).
	 */
	if (slot->tts_tupleDescriptor)
		ReleaseTupleDesc(slot->tts_tupleDescriptor);

	if (slot->tts_values)
		pfree(slot->tts_values);
	if (slot->tts_isnull)
		pfree(slot->tts_isnull);

	/*
	 * Install the new descriptor; if it's refcounted, bump its refcount.
	 */
	slot->tts_tupleDescriptor = tupdesc;
	PinTupleDesc(tupdesc);

	/*
	 * Allocate Datum/isnull arrays of the appropriate size.  These must have
	 * the same lifetime as the slot, so allocate in the slot's own context.
	 */
	slot->tts_values = (Datum *)
		MemoryContextAlloc(slot->tts_mcxt, tupdesc->natts * sizeof(Datum));
	slot->tts_isnull = (bool *)
		MemoryContextAlloc(slot->tts_mcxt, tupdesc->natts * sizeof(bool));
}

/* --------------------------------
 *		ExecStoreTuple
 *
 *		This function is used to store a physical tuple into a specified
 *		slot in the tuple table.
 *
 *		tuple:	tuple to store
 *		slot:	slot to store it in
 *		buffer: disk buffer if tuple is in a disk page, else InvalidBuffer
 *		shouldFree: true if ExecClearTuple should pfree() the tuple
 *					when done with it
 *
 * If 'buffer' is not InvalidBuffer, the tuple table code acquires a pin
 * on the buffer which is held until the slot is cleared, so that the tuple
 * won't go away on us.
 *
 * shouldFree is normally set 'true' for tuples constructed on-the-fly.
 * It must always be 'false' for tuples that are stored in disk pages,
 * since we don't want to try to pfree those.
 *
 * Another case where it is 'false' is when the referenced tuple is held in a
 * tuple table slot belonging to a lower-level executor Proc node.  In this
 * case the lower-level slot retains ownership and responsibility for
 * eventually releasing the tuple.  When this method is used, we must be
 * certain that the upper-level Proc node will lose interest in the tuple
 * sooner than the lower-level one does!  If you're not certain, copy the
 * lower-level tuple with heap_copytuple or with ExecCopySlotTuple() and let
 * the upper-level table slot assume ownership of the copy!
 *
 * Return value is just the passed-in slot pointer.
 *
 * NOTE: before PostgreSQL 8.1, this function would accept a NULL tuple
 * pointer and effectively behave like ExecClearTuple (though you could
 * still specify a buffer to pin, which would be an odd combination).
 * This saved a couple lines of code in a few places, but seemed more likely
 * to mask logic errors than to be really useful, so it's now disallowed.
 * --------------------------------
 *
 *  TODO: In the light of introducing new pluggable storage formats, should we
 *  rename this function as ExecStoreHeapTuple(), like ExecStoreMinimalTuple,
 *  to be more clear as to what kind if tuple is being stored. This applies to
 *  other function like ExecFetchSlotTuple, ExecFetchSlotTupleDatum,
 *
 *  TODO: Should we split the function into 1. ExecStoreBufferHeapTuple() to
 *  store an on disk heap tuple which will take Buffer as an argument but not
 *  shouldFree and 2. ExecStoreHeapTuple() to store an in-memory heap tuple
 *  which will not have Buffer as an argument and ShouldFree will be an
 *  argument. The callers of this function fall into one or the other category.
 *  There is no caller which falls in both the categories.
 */
TupleTableSlot *
ExecStoreTuple(HeapTuple tuple,
			   TupleTableSlot *slot,
			   Buffer buffer,
			   bool shouldFree)
{
	/*
	 * sanity checks
	 */
	Assert(tuple != NULL);
	Assert(slot != NULL);
	Assert(slot->tts_tupleDescriptor != NULL);
	/* passing shouldFree=true for a tuple on a disk page is not sane */
	Assert(BufferIsValid(buffer) ? (!shouldFree) : true);

	if (BufferIsValid(buffer))
	{
		if (!TTS_IS_BUFFERTUPLE(slot))
			elog(ERROR, "trying to store an on-disk heap tuple into wrong type of slot");
		tts_buffer_store_tuple(slot, tuple, buffer);
	}
	else
	{
		if (!TTS_IS_HEAPTUPLE(slot))
			elog(ERROR, "trying to store a heap tuple into wrong type of slot");
		tts_heap_store_tuple(slot, tuple, shouldFree);

	}

	return slot;
}


/* --------------------------------
 *		ExecStoreMinimalTuple
 *
 *		Like ExecStoreTuple, but insert a "minimal" tuple into the slot.
 *
 * No 'buffer' parameter since minimal tuples are never stored in relations.
 * --------------------------------
 */
TupleTableSlot *
ExecStoreMinimalTuple(MinimalTuple mtup,
					  TupleTableSlot *slot,
					  bool shouldFree)
{
	/*
	 * sanity checks
	 */
	Assert(mtup != NULL);
	Assert(slot != NULL);
	Assert(slot->tts_tupleDescriptor != NULL);

	if (!TTS_IS_MINIMALTUPLE(slot))
		elog(ERROR, "trying to store a minimal tuple into wrong type of slot");
	tts_minimal_store_tuple(slot, mtup, shouldFree);

	return slot;
}

/* --------------------------------
 *		ExecStoreVirtualTuple
 *			Mark a slot as containing a virtual tuple.
 *
 * The protocol for loading a slot with virtual tuple data is:
 *		* Call ExecClearTuple to mark the slot empty.
 *		* Store data into the Datum/isnull arrays.
 *		* Call ExecStoreVirtualTuple to mark the slot valid.
 * This is a bit unclean but it avoids one round of data copying.
 *
 * TODO: Now that we have VirtualTupleTableSlot as a separate slot type, name
 * of this function is misleading. We could follow the protocol above with
 * every TupleTableSlot type. So the name of the function should be something
 * like ExecFinishLoadSlot() or something like that.
 * --------------------------------
 */
TupleTableSlot *
ExecStoreVirtualTuple(TupleTableSlot *slot)
{
	/*
	 * sanity checks
	 */
	Assert(slot != NULL);
	Assert(slot->tts_tupleDescriptor != NULL);
	Assert(IS_TTS_EMPTY(slot));

	RESET_TTS_EMPTY(slot);
	slot->tts_nvalid = slot->tts_tupleDescriptor->natts;

	return slot;
}

/* --------------------------------
 *		ExecStoreAllNullTuple
 *			Set up the slot to contain a null in every column.
 *
 * At first glance this might sound just like ExecClearTuple, but it's
 * entirely different: the slot ends up full, not empty.
 * --------------------------------
 */
TupleTableSlot *
ExecStoreAllNullTuple(TupleTableSlot *slot)
{
	/*
	 * sanity checks
	 */
	Assert(slot != NULL);
	Assert(slot->tts_tupleDescriptor != NULL);

	/* Clear any old contents */
	ExecClearTuple(slot);

	/*
	 * Fill all the columns of the virtual tuple with nulls
	 */
	MemSet(slot->tts_values, 0,
		   slot->tts_tupleDescriptor->natts * sizeof(Datum));
	memset(slot->tts_isnull, true,
		   slot->tts_tupleDescriptor->natts * sizeof(bool));

	return ExecStoreVirtualTuple(slot);
}

/* --------------------------------
 *		ExecFetchSlotTuple
 *			Fetch the slot's regular physical tuple.
 *
 *		This is a thin wrapper around TupleTableSlotType specific
 *		get_heap_tuple callback. The callback is expected to return a heap
 *		tuple as is if it holds one and continue to have ownership of the heap
 *		tuple. Or it is expected to convert the internal representation into a
 *		heap tuple, if required, and return it. In this case it may or may not
 *		retain the heap tuple ownership.
 *
 * The main difference between this and ExecMaterializeSlot() is that this
 * does not guarantee that the contained tuple is local storage.
 * Hence, the result must be treated as read-only.
 *
 * TODO: should we move these thin wrappers to tuptable.h and make those
 * inline.
 * --------------------------------
 */
HeapTuple
ExecFetchSlotTuple(TupleTableSlot *slot)
{
	/*
	 * sanity checks
	 */
	Assert(slot != NULL);
	Assert(!IS_TTS_EMPTY(slot));

	return slot->tts_cb->get_heap_tuple(slot);
}

/* --------------------------------
 *		ExecFetchSlotMinimalTuple
 *			Fetch the slot's minimal physical tuple.
 *
 *		This is a thin wrapper around TupleTableSlotType specific
 *		get_heap_tuple callback. The callback is expected to return a heap
 *		tuple as is if it holds one and continue to have ownership of the heap
 *		tuple. Or it is expected to convert the internal representation into a
 *		heap tuple, if required, and return it. In this case it may or may not
 *		retain the heap tuple ownership.
 *
 * As above, the result must be treated as read-only.
 * --------------------------------
 */
MinimalTuple
ExecFetchSlotMinimalTuple(TupleTableSlot *slot)
{
	/*
	 * sanity checks
	 */
	Assert(slot != NULL);
	Assert(!IS_TTS_EMPTY(slot));

	return slot->tts_cb->get_minimal_tuple(slot);
}

/* --------------------------------
 *		ExecFetchSlotTupleDatum
 *			Fetch the slot's tuple as a composite-type Datum.
 *
 *		The result is always freshly palloc'd in the caller's memory context.
 * --------------------------------
 */
Datum
ExecFetchSlotTupleDatum(TupleTableSlot *slot)
{
	HeapTuple	tup;
	TupleDesc	tupdesc;

	/* Fetch slot's contents in regular-physical-tuple form */
	tup = ExecFetchSlotTuple(slot);
	tupdesc = slot->tts_tupleDescriptor;

	/* Convert to Datum form */
	return heap_copy_tuple_as_datum(tup, tupdesc);
}

/* --------------------------------
 *		ExecCopySlot
 *			Copy the source slot's contents into the destination slot.
 *
 *		The destination acquires a private copy that will not go away
 *		if the source is cleared.
 *
 *		The caller must ensure the slots have compatible tupdescs.
 * --------------------------------
 */
TupleTableSlot *
ExecCopySlot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	MemoryContext oldcontext;

	ExecClearTuple(dstslot);

	/*
	 * TODO: We could eliminate the if .. else if .. conditions below, by
	 * having a callback copyslot() called on the dstslot. The callback would
	 * decide how to fetch the data from the srcslot so that it can be saved
	 * into the destination slot's representation.
	 */
	if (TTS_IS_VIRTUAL(dstslot))
	{
		FormData_pg_attribute *attrs = srcslot->tts_tupleDescriptor->attrs;
		int			cnt;
		int			natts = dstslot->tts_tupleDescriptor->natts;

		/*
		 * Copy the datums from the source slot to the destination slot. If the
		 * destination slot is not a virtual tuple table slot, first deform the
		 * tuple inside it.
		 */
		if (!TTS_IS_VIRTUAL(srcslot));
		{
			slot_getallattrs(srcslot);

			/*
			 * Number of attributes in the source tuple should match those in
			 * the destination tuple descriptor.
			 */
			Assert(srcslot->tts_nvalid == natts);
		}

		oldcontext = MemoryContextSwitchTo(dstslot->tts_mcxt);
		memcpy(dstslot->tts_isnull, srcslot->tts_isnull, sizeof(bool) * natts);
		for (cnt = 0; cnt < natts; cnt++)
		{
			if (!dstslot->tts_isnull[cnt])
				dstslot->tts_values[cnt] = datumCopy(srcslot->tts_values[cnt],
													 attrs[cnt].attbyval,
													 attrs[cnt].attlen);
		}
		MemoryContextSwitchTo(oldcontext);

		ExecStoreVirtualTuple(dstslot);
	}
	else if (TTS_IS_HEAPTUPLE(dstslot) || TTS_IS_BUFFERTUPLE(dstslot))
	{
		HeapTuple tuple;

		oldcontext = MemoryContextSwitchTo(dstslot->tts_mcxt);
		tuple = ExecCopySlotTuple(srcslot);
		MemoryContextSwitchTo(oldcontext);

		ExecStoreTuple(tuple, dstslot, InvalidBuffer, true);
	}
	else if (TTS_IS_MINIMALTUPLE(dstslot))
	{
		MinimalTuple mintuple;

		oldcontext = MemoryContextSwitchTo(dstslot->tts_mcxt);
		mintuple = ExecCopySlotMinimalTuple(srcslot);
		MemoryContextSwitchTo(oldcontext);

		ExecStoreMinimalTuple(mintuple, dstslot, true);
	}

	return dstslot;
}


/* ----------------------------------------------------------------
 *				convenience initialization routines
 * ----------------------------------------------------------------
 */

/* --------------------------------
 *		ExecInit{Result,Scan,Extra}TupleSlot[TL]
 *
 *		These are convenience routines to initialize the specified slot
 *		in nodes inheriting the appropriate state.  ExecInitExtraTupleSlot
 *		is used for initializing special-purpose slots.
 * --------------------------------
 */

/* ----------------
 *		ExecInitResultTupleSlotTL
 *
 *		Initialize result tuple slot, using the plan node's targetlist.
 * ----------------
 */
void
ExecInitResultTupleSlotTL(EState *estate, PlanState *planstate,
						  TupleTableSlotType st)
{
	bool		hasoid;
	TupleDesc	tupDesc;

	if (ExecContextForcesOids(planstate, &hasoid))
	{
		/* context forces OID choice; hasoid is now set correctly */
	}
	else
	{
		/* given free choice, don't leave space for OIDs in result tuples */
		hasoid = false;
	}

	tupDesc = ExecTypeFromTL(planstate->plan->targetlist, hasoid);

	planstate->ps_ResultTupleSlot =
		ExecAllocTableSlot(&estate->es_tupleTable, tupDesc, st);
}

/* ----------------
 *		ExecInitScanTupleSlot
 * ----------------
 */
void
ExecInitScanTupleSlot(EState *estate, ScanState *scanstate,
					  TupleDesc tupledesc, TupleTableSlotType st)
{
	scanstate->ss_ScanTupleSlot = ExecAllocTableSlot(&estate->es_tupleTable,
													 tupledesc, st);
	scanstate->ps.scandesc = tupledesc;
}

/* ----------------
 *		ExecInitExtraTupleSlot
 *
 * Return a newly created slot. If tupledesc is non-NULL the slot will have
 * that as its fixed tupledesc. Otherwise the caller needs to use
 * ExecSetSlotDescriptor() to set the descriptor before use.
 * ----------------
 */
TupleTableSlot *
ExecInitExtraTupleSlot(EState *estate, TupleDesc tupledesc, TupleTableSlotType st)
{
	return ExecAllocTableSlot(&estate->es_tupleTable, tupledesc, st);
}

/* ----------------
 *		ExecInitNullTupleSlot
 *
 * Build a slot containing an all-nulls tuple of the given type.
 * This is used as a substitute for an input tuple when performing an
 * outer join.
 * ----------------
 */
TupleTableSlot *
ExecInitNullTupleSlot(EState *estate, TupleDesc tupType, TupleTableSlotType st)
{
	TupleTableSlot *slot = ExecInitExtraTupleSlot(estate, tupType, st);

	return ExecStoreAllNullTuple(slot);
}

/* ----------------------------------------------------------------
 *		ExecTypeFromTL
 *
 *		Generate a tuple descriptor for the result tuple of a targetlist.
 *		(A parse/plan tlist must be passed, not an ExprState tlist.)
 *		Note that resjunk columns, if any, are included in the result.
 *
 *		Currently there are about 4 different places where we create
 *		TupleDescriptors.  They should all be merged, or perhaps
 *		be rewritten to call BuildDesc().
 * ----------------------------------------------------------------
 */
TupleDesc
ExecTypeFromTL(List *targetList, bool hasoid)
{
	return ExecTypeFromTLInternal(targetList, hasoid, false);
}

/* ----------------------------------------------------------------
 *		ExecCleanTypeFromTL
 *
 *		Same as above, but resjunk columns are omitted from the result.
 * ----------------------------------------------------------------
 */
TupleDesc
ExecCleanTypeFromTL(List *targetList, bool hasoid)
{
	return ExecTypeFromTLInternal(targetList, hasoid, true);
}

static TupleDesc
ExecTypeFromTLInternal(List *targetList, bool hasoid, bool skipjunk)
{
	TupleDesc	typeInfo;
	ListCell   *l;
	int			len;
	int			cur_resno = 1;

	if (skipjunk)
		len = ExecCleanTargetListLength(targetList);
	else
		len = ExecTargetListLength(targetList);
	typeInfo = CreateTemplateTupleDesc(len, hasoid);

	foreach(l, targetList)
	{
		TargetEntry *tle = lfirst(l);

		if (skipjunk && tle->resjunk)
			continue;
		TupleDescInitEntry(typeInfo,
						   cur_resno,
						   tle->resname,
						   exprType((Node *) tle->expr),
						   exprTypmod((Node *) tle->expr),
						   0);
		TupleDescInitEntryCollation(typeInfo,
									cur_resno,
									exprCollation((Node *) tle->expr));
		cur_resno++;
	}

	return typeInfo;
}

/*
 * ExecTypeFromExprList - build a tuple descriptor from a list of Exprs
 *
 * This is roughly like ExecTypeFromTL, but we work from bare expressions
 * not TargetEntrys.  No names are attached to the tupledesc's columns.
 */
TupleDesc
ExecTypeFromExprList(List *exprList)
{
	TupleDesc	typeInfo;
	ListCell   *lc;
	int			cur_resno = 1;

	typeInfo = CreateTemplateTupleDesc(list_length(exprList), false);

	foreach(lc, exprList)
	{
		Node	   *e = lfirst(lc);

		TupleDescInitEntry(typeInfo,
						   cur_resno,
						   NULL,
						   exprType(e),
						   exprTypmod(e),
						   0);
		TupleDescInitEntryCollation(typeInfo,
									cur_resno,
									exprCollation(e));
		cur_resno++;
	}

	return typeInfo;
}

/*
 * ExecTypeSetColNames - set column names in a TupleDesc
 *
 * Column names must be provided as an alias list (list of String nodes).
 *
 * For some callers, the supplied tupdesc has a named rowtype (not RECORD)
 * and it is moderately likely that the alias list matches the column names
 * already present in the tupdesc.  If we do change any column names then
 * we must reset the tupdesc's type to anonymous RECORD; but we avoid doing
 * so if no names change.
 */
void
ExecTypeSetColNames(TupleDesc typeInfo, List *namesList)
{
	bool		modified = false;
	int			colno = 0;
	ListCell   *lc;

	foreach(lc, namesList)
	{
		char	   *cname = strVal(lfirst(lc));
		Form_pg_attribute attr;

		/* Guard against too-long names list */
		if (colno >= typeInfo->natts)
			break;
		attr = TupleDescAttr(typeInfo, colno);
		colno++;

		/* Ignore empty aliases (these must be for dropped columns) */
		if (cname[0] == '\0')
			continue;

		/* Change tupdesc only if alias is actually different */
		if (strcmp(cname, NameStr(attr->attname)) != 0)
		{
			namestrcpy(&(attr->attname), cname);
			modified = true;
		}
	}

	/* If we modified the tupdesc, it's now a new record type */
	if (modified)
	{
		typeInfo->tdtypeid = RECORDOID;
		typeInfo->tdtypmod = -1;
	}
}

/*
 * BlessTupleDesc - make a completed tuple descriptor useful for SRFs
 *
 * Rowtype Datums returned by a function must contain valid type information.
 * This happens "for free" if the tupdesc came from a relcache entry, but
 * not if we have manufactured a tupdesc for a transient RECORD datatype.
 * In that case we have to notify typcache.c of the existence of the type.
 */
TupleDesc
BlessTupleDesc(TupleDesc tupdesc)
{
	if (tupdesc->tdtypeid == RECORDOID &&
		tupdesc->tdtypmod < 0)
		assign_record_type_typmod(tupdesc);

	return tupdesc;				/* just for notational convenience */
}

/*
 * TupleDescGetAttInMetadata - Build an AttInMetadata structure based on the
 * supplied TupleDesc. AttInMetadata can be used in conjunction with C strings
 * to produce a properly formed tuple.
 */
AttInMetadata *
TupleDescGetAttInMetadata(TupleDesc tupdesc)
{
	int			natts = tupdesc->natts;
	int			i;
	Oid			atttypeid;
	Oid			attinfuncid;
	FmgrInfo   *attinfuncinfo;
	Oid		   *attioparams;
	int32	   *atttypmods;
	AttInMetadata *attinmeta;

	attinmeta = (AttInMetadata *) palloc(sizeof(AttInMetadata));

	/* "Bless" the tupledesc so that we can make rowtype datums with it */
	attinmeta->tupdesc = BlessTupleDesc(tupdesc);

	/*
	 * Gather info needed later to call the "in" function for each attribute
	 */
	attinfuncinfo = (FmgrInfo *) palloc0(natts * sizeof(FmgrInfo));
	attioparams = (Oid *) palloc0(natts * sizeof(Oid));
	atttypmods = (int32 *) palloc0(natts * sizeof(int32));

	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		/* Ignore dropped attributes */
		if (!att->attisdropped)
		{
			atttypeid = att->atttypid;
			getTypeInputInfo(atttypeid, &attinfuncid, &attioparams[i]);
			fmgr_info(attinfuncid, &attinfuncinfo[i]);
			atttypmods[i] = att->atttypmod;
		}
	}
	attinmeta->attinfuncs = attinfuncinfo;
	attinmeta->attioparams = attioparams;
	attinmeta->atttypmods = atttypmods;

	return attinmeta;
}

/*
 * BuildTupleFromCStrings - build a HeapTuple given user data in C string form.
 * values is an array of C strings, one for each attribute of the return tuple.
 * A NULL string pointer indicates we want to create a NULL field.
 */
HeapTuple
BuildTupleFromCStrings(AttInMetadata *attinmeta, char **values)
{
	TupleDesc	tupdesc = attinmeta->tupdesc;
	int			natts = tupdesc->natts;
	Datum	   *dvalues;
	bool	   *nulls;
	int			i;
	HeapTuple	tuple;

	dvalues = (Datum *) palloc(natts * sizeof(Datum));
	nulls = (bool *) palloc(natts * sizeof(bool));

	/*
	 * Call the "in" function for each non-dropped attribute, even for nulls,
	 * to support domains.
	 */
	for (i = 0; i < natts; i++)
	{
		if (!TupleDescAttr(tupdesc, i)->attisdropped)
		{
			/* Non-dropped attributes */
			dvalues[i] = InputFunctionCall(&attinmeta->attinfuncs[i],
										   values[i],
										   attinmeta->attioparams[i],
										   attinmeta->atttypmods[i]);
			if (values[i] != NULL)
				nulls[i] = false;
			else
				nulls[i] = true;
		}
		else
		{
			/* Handle dropped attributes by setting to NULL */
			dvalues[i] = (Datum) 0;
			nulls[i] = true;
		}
	}

	/*
	 * Form a tuple
	 */
	tuple = heap_form_tuple(tupdesc, dvalues, nulls);

	/*
	 * Release locally palloc'd space.  XXX would probably be good to pfree
	 * values of pass-by-reference datums, as well.
	 */
	pfree(dvalues);
	pfree(nulls);

	return tuple;
}

/*
 * HeapTupleHeaderGetDatum - convert a HeapTupleHeader pointer to a Datum.
 *
 * This must *not* get applied to an on-disk tuple; the tuple should be
 * freshly made by heap_form_tuple or some wrapper routine for it (such as
 * BuildTupleFromCStrings).  Be sure also that the tupledesc used to build
 * the tuple has a properly "blessed" rowtype.
 *
 * Formerly this was a macro equivalent to PointerGetDatum, relying on the
 * fact that heap_form_tuple fills in the appropriate tuple header fields
 * for a composite Datum.  However, we now require that composite Datums not
 * contain any external TOAST pointers.  We do not want heap_form_tuple itself
 * to enforce that; more specifically, the rule applies only to actual Datums
 * and not to HeapTuple structures.  Therefore, HeapTupleHeaderGetDatum is
 * now a function that detects whether there are externally-toasted fields
 * and constructs a new tuple with inlined fields if so.  We still need
 * heap_form_tuple to insert the Datum header fields, because otherwise this
 * code would have no way to obtain a tupledesc for the tuple.
 *
 * Note that if we do build a new tuple, it's palloc'd in the current
 * memory context.  Beware of code that changes context between the initial
 * heap_form_tuple/etc call and calling HeapTuple(Header)GetDatum.
 *
 * For performance-critical callers, it could be worthwhile to take extra
 * steps to ensure that there aren't TOAST pointers in the output of
 * heap_form_tuple to begin with.  It's likely however that the costs of the
 * typcache lookup and tuple disassembly/reassembly are swamped by TOAST
 * dereference costs, so that the benefits of such extra effort would be
 * minimal.
 *
 * XXX it would likely be better to create wrapper functions that produce
 * a composite Datum from the field values in one step.  However, there's
 * enough code using the existing APIs that we couldn't get rid of this
 * hack anytime soon.
 */
Datum
HeapTupleHeaderGetDatum(HeapTupleHeader tuple)
{
	Datum		result;
	TupleDesc	tupDesc;

	/* No work if there are no external TOAST pointers in the tuple */
	if (!HeapTupleHeaderHasExternal(tuple))
		return PointerGetDatum(tuple);

	/* Use the type data saved by heap_form_tuple to look up the rowtype */
	tupDesc = lookup_rowtype_tupdesc(HeapTupleHeaderGetTypeId(tuple),
									 HeapTupleHeaderGetTypMod(tuple));

	/* And do the flattening */
	result = toast_flatten_tuple_to_datum(tuple,
										  HeapTupleHeaderGetDatumLength(tuple),
										  tupDesc);

	ReleaseTupleDesc(tupDesc);

	return result;
}


/*
 * Functions for sending tuples to the frontend (or other specified destination)
 * as though it is a SELECT result. These are used by utility commands that
 * need to project directly to the destination and don't need or want full
 * table function capability. Currently used by EXPLAIN and SHOW ALL.
 */
TupOutputState *
begin_tup_output_tupdesc(DestReceiver *dest, TupleDesc tupdesc)
{
	TupOutputState *tstate;

	tstate = (TupOutputState *) palloc(sizeof(TupOutputState));

	tstate->slot = MakeSingleTupleTableSlot(tupdesc, TTS_TYPE_VIRTUAL);
	tstate->dest = dest;

	tstate->dest->rStartup(tstate->dest, (int) CMD_SELECT, tupdesc);

	return tstate;
}

/*
 * write a single tuple
 */
void
do_tup_output(TupOutputState *tstate, Datum *values, bool *isnull)
{
	TupleTableSlot *slot = tstate->slot;
	int			natts = slot->tts_tupleDescriptor->natts;

	/* make sure the slot is clear */
	ExecClearTuple(slot);

	/* insert data */
	memcpy(slot->tts_values, values, natts * sizeof(Datum));
	memcpy(slot->tts_isnull, isnull, natts * sizeof(bool));

	/* mark slot as containing a virtual tuple */
	ExecStoreVirtualTuple(slot);

	/* send the tuple to the receiver */
	(void) tstate->dest->receiveSlot(slot, tstate->dest);

	/* clean up */
	ExecClearTuple(slot);
}

/*
 * write a chunk of text, breaking at newline characters
 *
 * Should only be used with a single-TEXT-attribute tupdesc.
 */
void
do_text_output_multiline(TupOutputState *tstate, const char *txt)
{
	Datum		values[1];
	bool		isnull[1] = {false};

	while (*txt)
	{
		const char *eol;
		int			len;

		eol = strchr(txt, '\n');
		if (eol)
		{
			len = eol - txt;
			eol++;
		}
		else
		{
			len = strlen(txt);
			eol = txt + len;
		}

		values[0] = PointerGetDatum(cstring_to_text_with_len(txt, len));
		do_tup_output(tstate, values, isnull);
		pfree(DatumGetPointer(values[0]));
		txt = eol;
	}
}

void
end_tup_output(TupOutputState *tstate)
{
	tstate->dest->rShutdown(tstate->dest);
	/* note that destroying the dest is not ours to do */
	ExecDropSingleTupleTableSlot(tstate->slot);
	pfree(tstate);
}
