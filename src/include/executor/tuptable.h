/*-------------------------------------------------------------------------
 *
 * tuptable.h
 *	  tuple table support stuff
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/tuptable.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TUPTABLE_H
#define TUPTABLE_H

#include "access/htup.h"
#include "access/tupdesc.h"
#include "storage/buf.h"

/*----------
 * The executor stores tuples in a "tuple table" which is a List of
 * independent TupleTableSlots.  There are several cases we need to handle:
 *		1. physical tuple in a disk buffer page
 *		2. physical tuple constructed in palloc'ed memory
 *		3. "minimal" physical tuple constructed in palloc'ed memory
 *		4. "virtual" tuple consisting of Datum/isnull arrays
 *
 * The first two cases are similar in that they both deal with "materialized"
 * tuples, but resource management is different.  For a tuple in a disk page
 * we need to hold a pin on the buffer until the TupleTableSlot's reference
 * to the tuple is dropped; while for a palloc'd tuple we usually want the
 * tuple pfree'd when the TupleTableSlot's reference is dropped.
 *
 * A "minimal" tuple is handled similarly to a palloc'd regular tuple.
 * At present, minimal tuples never are stored in buffers, so there is no
 * parallel to case 1.  Note that a minimal tuple has no "system columns".
 * (Actually, it could have an OID, but we have no need to access the OID.)
 *
 * A "virtual" tuple is an optimization used to minimize physical data
 * copying in a nest of plan nodes.  Any pass-by-reference Datums in the
 * tuple point to storage that is not directly associated with the
 * TupleTableSlot; generally they will point to part of a tuple stored in
 * a lower plan node's output TupleTableSlot, or to a function result
 * constructed in a plan node's per-tuple econtext.  It is the responsibility
 * of the generating plan node to be sure these resources are not released
 * for as long as the virtual tuple needs to be valid.  We only use virtual
 * tuples in the result slots of plan nodes --- tuples to be copied anywhere
 * else need to be "materialized" into physical tuples.  Note also that a
 * virtual tuple does not have any "system columns".
 *
 * It is also possible for a TupleTableSlot to hold both physical and minimal
 * copies of a tuple.  This is done when the slot is requested to provide
 * the format other than the one it currently holds.  (Originally we attempted
 * to handle such requests by replacing one format with the other, but that
 * had the fatal defect of invalidating any pass-by-reference Datums pointing
 * into the existing slot contents.)  Both copies must contain identical data
 * payloads when this is the case.
 *
 * The Datum/isnull arrays of a TupleTableSlot serve double duty.  When the
 * slot contains a virtual tuple, they are the authoritative data.  When the
 * slot contains a physical tuple, the arrays contain data extracted from
 * the tuple.  (In this state, any pass-by-reference Datums point into
 * the physical tuple.)  The extracted information is built "lazily",
 * ie, only as needed.  This serves to avoid repeated extraction of data
 * from the physical tuple.
 *
 * A TupleTableSlot can also be "empty", indicated by flag TTS_EMPTY set in
 * tts_flags, holding no valid data.  This is the only valid state for a
 * freshly-created slot that has not yet had a tuple descriptor assigned to it.
 * In this state, TTS_SHOULDFREE should not be set in tts_flag, tts_tuple must
 * be NULL, tts_buffer InvalidBuffer, and tts_nvalid zero.
 *
 * The tupleDescriptor is simply referenced, not copied, by the TupleTableSlot
 * code.  The caller of ExecSetSlotDescriptor() is responsible for providing
 * a descriptor that will live as long as the slot does.  (Typically, both
 * slots and descriptors are in per-query memory and are freed by memory
 * context deallocation at query end; so it's not worth providing any extra
 * mechanism to do more.  However, the slot will increment the tupdesc
 * reference count if a reference-counted tupdesc is supplied.)
 *
 * When TTS_SHOULDFREE is set in tts_flags, the physical tuple is "owned" by
 * the slot and should be freed when the slot's reference to the tuple is
 * dropped.
 *
 * If tts_buffer is not InvalidBuffer, then the slot is holding a pin
 * on the indicated buffer page; drop the pin when we release the
 * slot's reference to that buffer.  (tts_shouldFree should always be
 * false in such a case, since presumably tts_tuple is pointing at the
 * buffer page.)
 *
 * tts_nvalid indicates the number of valid columns in the tts_values/isnull
 * arrays.  When the slot is holding a "virtual" tuple this must be equal
 * to the descriptor's natts.  When the slot is holding a physical tuple
 * this is equal to the number of columns we have extracted (we always
 * extract columns from left to right, so there are no holes).
 *
 * tts_values/tts_isnull are allocated when a descriptor is assigned to the
 * slot; they are of length equal to the descriptor's natts.
 *
 * tts_mintuple must always be NULL if the slot does not hold a "minimal"
 * tuple.  When it does, tts_mintuple points to the actual MinimalTupleData
 * object (the thing to be pfree'd if tts_shouldFreeMin is true).  If the slot
 * has only a minimal and not also a regular physical tuple, then tts_tuple
 * points at tts_minhdr and the fields of that struct are set correctly
 * for access to the minimal tuple; in particular, tts_minhdr.t_data points
 * MINIMAL_TUPLE_OFFSET bytes before tts_mintuple.  This allows column
 * extraction to treat the case identically to regular physical tuples.
 *
 * TTS_SLOW flag in tts_flags and tts_off are saved state for
 * slot_deform_tuple, and should not be touched by any other code.
 *----------
 */

/* true = slot is empty */
#define			TTS_ISEMPTY			(1 << 1)
#define IS_TTS_EMPTY(slot)	((slot)->tts_flags & TTS_ISEMPTY)
#define SET_TTS_EMPTY(slot) ((slot)->tts_flags |= TTS_ISEMPTY)
#define RESET_TTS_EMPTY(slot) ((slot)->tts_flags &= ~TTS_ISEMPTY)

/* should pfree tts_tuple? */
#define			TTS_SHOULDFREE		(1 << 2)
#define IS_TTS_SHOULDFREE(slot) ((slot)->tts_flags & TTS_SHOULDFREE)
#define SET_TTS_SHOULDFREE(slot) ((slot)->tts_flags |= TTS_SHOULDFREE)
#define RESET_TTS_SHOULDFREE(slot) ((slot)->tts_flags &= ~TTS_SHOULDFREE)

/* should pfree tts_mintuple? */
#define			TTS_SHOULDFREEMIN	(1 << 3)
#define IS_TTS_SHOULDFREEMIN(slot) ((slot)->tts_flags & TTS_SHOULDFREEMIN)
#define SET_TTS_SHOULDFREEMIN(slot) ((slot)->tts_flags |= TTS_SHOULDFREEMIN)
#define RESET_TTS_SHOULDFREEMIN(slot) ((slot)->tts_flags &= ~TTS_SHOULDFREEMIN)

/* saved state for slot_deform_tuple */
#define			TTS_SLOW			(1 << 4)
#define IS_TTS_SLOW(slot) ((slot)->tts_flags & TTS_SLOW)
#define SET_TTS_SLOW(slot) ((slot)->tts_flags |= TTS_SLOW)
#define RESET_TTS_SLOW(slot) ((slot)->tts_flags &= ~TTS_SLOW)

/* fixed tuple descriptor */
#define			TTS_FIXED			(1 << 5)
#define IS_TTS_FIXED(slot) ((slot)->tts_flags & TTS_FIXED)
#define SET_TTS_FIXED(slot) ((slot)->tts_flags |= TTS_FIXED)
#define RESET_TTS_FIXED(slot) ((slot)->tts_flags &= ~TTS_FIXED)

typedef enum TupleTableSlotType
{
	TTS_TYPE_VIRTUAL,
	TTS_TYPE_HEAPTUPLE,
	TTS_TYPE_MINIMALTUPLE,
	TTS_TYPE_BUFFER
} TupleTableSlotType;

struct TupleTableSlot;
typedef struct TupleTableSlot TupleTableSlot;

/* TupleTableSlotType specific routines */
typedef struct TupleTableSlotOps
{
	/* Initialization. */
	void (*init)(TupleTableSlot *slot);

	/* Destruction. */
	void (*release)(TupleTableSlot *slot);

	/*
	 * Clear the contents of the slot. Only the contents are expected to be
	 * cleared and not the tuple descriptor. Typically an implementation of
	 * this callback should free the memory allocated for the tuple contained
	 * in the slot.
	 */
	void (*clear)(TupleTableSlot *slot);

	/*
	 * Fill up first natts entries of tts_values and tts_isnull arrays with
	 * values from the tuple contained in the slot.
	 */
	void (*getsomeattrs)(TupleTableSlot *slot, int natts);

	/*
	 * Save the contents of the slot into its own memory context.
	 *
	 * TODO:
	 * The name of this callback may be changed to something different once
	 * we finalize the name for ExecSaveSlot().
	 */
	void (*materialize)(TupleTableSlot *slot);

	/*
	 * Return a heap tuple representing the contents of the slot. The returned
	 * heap tuple need not be writable.
	 */
	HeapTuple (*get_heap_tuple)(TupleTableSlot *slot);

	/*
	 * Return a minimal tuple representing the contents of the slot. The
	 * returned minimal tuple need not be writable.
	 */
	MinimalTuple (*get_minimal_tuple)(TupleTableSlot *slot);

	/*
	 * Return a copy of heap tuple representing the contents of the slot. The
	 * returned heap tuple should be writable. The copy should be palloc'd in
	 * the current memory context. The slot itself is expected to remain
	 * undisturbed. It is *not* expected to have meaningful "system columns" in
	 * the copy.
	 */
	HeapTuple (*copy_heap_tuple)(TupleTableSlot *slot);

	/*
	 * Return a copy of minimal tuple representing the contents of the slot.
	 * The returned minimal tuple should be writable. The copy should be
	 * palloc'd in the current memory context. The slot itself is expected to
	 * remain undisturbed.
	 */
	MinimalTuple (*copy_minimal_tuple)(TupleTableSlot *slot);

} TupleTableSlotOps;

/*
 * Predefined TupleTableSlotOps for various types of TupleTableSlotOps. The
 * same are used to identify the type of a given slot.
 */
extern PGDLLIMPORT const TupleTableSlotOps TTSOpsVirtual;
extern PGDLLIMPORT const TupleTableSlotOps TTSOpsHeapTuple;
extern PGDLLIMPORT const TupleTableSlotOps TTSOpsMinimalTuple;
extern PGDLLIMPORT const TupleTableSlotOps TTSOpsBufferTuple;

#define TTS_IS_VIRTUAL(slot) ((slot)->tts_cb == &TTSOpsVirtual)
#define TTS_IS_HEAPTUPLE(slot) ((slot)->tts_cb == &TTSOpsHeapTuple)
#define TTS_IS_MINIMALTUPLE(slot) ((slot)->tts_cb == &TTSOpsMinimalTuple)
#define TTS_IS_BUFFERTUPLE(slot) ((slot)->tts_cb == &TTSOpsBufferTuple)

extern Datum ExecFetchSlotTupleDatum(TupleTableSlot *slot);

extern TupleTableSlot *ExecCopySlot(TupleTableSlot *dstslot,
			 TupleTableSlot *srcslot);

/* virtual or base type */
/*
 * TODO: tts_type is added for convenience of debugging. Most of the code
 * doesn't use it. Instead tts_cb is used to identify the type of tuple table
 * slot.  We might want to create new NodeTag for the type of tuple, but then
 * users won't be able to add new tuple table types dynamically.
 */
struct TupleTableSlot
{
	NodeTag		type;
	TupleTableSlotType tts_type;	/* Type of TupleTableSlot. */
	uint16		tts_flags;
	AttrNumber	tts_nvalid;		/* # of valid values in tts_values */

	const TupleTableSlotOps *const tts_cb;
	TupleDesc	tts_tupleDescriptor;	/* slot's tuple descriptor */
	Datum	   *tts_values;		/* current per-attribute values */
	bool	   *tts_isnull;		/* current per-attribute isnull flags */

	/* can we optimize away? */
	MemoryContext tts_mcxt;		/* slot itself is in this context */
};

typedef struct HeapTupleTableSlot
{
	TupleTableSlot base;
	HeapTuple	tuple;		/* physical tuple */
	uint32		off;		/* saved state for slot_deform_tuple */
} HeapTupleTableSlot;

/* heap tuple residing in a buffer */
typedef struct BufferHeapTupleTableSlot
{
	HeapTupleTableSlot base;
	Buffer		buffer;		/* tuple's buffer, or InvalidBuffer */
} BufferHeapTupleTableSlot;

typedef struct MinimalTupleTableSlot
{
	TupleTableSlot base;
	HeapTuple	tuple;		/* tuple wrapper */
	MinimalTuple mintuple;	/* minimal tuple, or NULL if none */
	HeapTupleData minhdr;	/* workspace for minimal-tuple-only case */
	uint32		off;		/* saved state for slot_deform_tuple */
} MinimalTupleTableSlot;

/*
 * TupIsNull -- is a TupleTableSlot empty?
 */
#define TupIsNull(slot) \
	((slot) == NULL || IS_TTS_EMPTY(slot))

/* in executor/execTuples.c */
extern TupleTableSlot *MakeTupleTableSlot(TupleDesc tupleDesc, TupleTableSlotType st);
extern TupleTableSlot *ExecAllocTableSlot(List **tupleTable, TupleDesc desc, TupleTableSlotType st);
extern void ExecResetTupleTable(List *tupleTable, bool shouldFree);
extern TupleTableSlot *MakeSingleTupleTableSlot(TupleDesc tupdesc, TupleTableSlotType st);
extern void ExecDropSingleTupleTableSlot(TupleTableSlot *slot);
extern void ExecSetSlotDescriptor(TupleTableSlot *slot, TupleDesc tupdesc);
extern TupleTableSlot *ExecStoreTuple(HeapTuple tuple,
			   TupleTableSlot *slot,
			   Buffer buffer,
			   bool shouldFree);
extern TupleTableSlot *ExecStoreMinimalTuple(MinimalTuple mtup,
					  TupleTableSlot *slot,
					  bool shouldFree);

extern TupleTableSlot *ExecStoreVirtualTuple(TupleTableSlot *slot);
extern TupleTableSlot *ExecStoreAllNullTuple(TupleTableSlot *slot);

extern HeapTuple ExecFetchSlotTuple(TupleTableSlot *slot);
extern MinimalTuple ExecFetchSlotMinimalTuple(TupleTableSlot *slot);
extern Datum ExecFetchSlotTupleDatum(TupleTableSlot *slot);

extern TupleTableSlot *ExecCopySlot(TupleTableSlot *dstslot,
			 TupleTableSlot *srcslot);

/*
 * ExecClearTuple
 *
 * A thin wrapper around calling TupleTableSlotType specific clear() method.
 */
static inline TupleTableSlot *
ExecClearTuple(TupleTableSlot *slot)
{
	slot->tts_cb->clear(slot);
	return slot;
}

/*
 * TODO: What used to be ExecMaterializeSlot() is split into two functions
 * ExecGetHeapTupleFromSlot() and this. The first one returns a heap tuple
 * representing the contents of the given slot. The second saves the contents
 * of the slot into its own memory context. The name of this function should be
 * changed, but I do not have any bright idea now.
 *
 * Eventually we want to replace all the calls to ExecMaterializeSlot() with
 * one
 * of the two functions. We will do this as and when needed. Once all the calls
 * are replaced, we should remove this function too.
 */
static inline HeapTuple
ExecMaterializeSlot(TupleTableSlot *slot)
{
	/*
	 * TODO: Most of the get_heap_tuple() implementations materialize a tuple
	 * if it's not available. So, a separate call for materialize looks
	 * unnecessary.
	 */
	slot->tts_cb->materialize(slot);
	return slot->tts_cb->get_heap_tuple(slot);
}

static inline HeapTuple
ExecGetHeapTupleFromSlot(TupleTableSlot *slot)
{
	return slot->tts_cb->get_heap_tuple(slot);
}

static inline void
ExecSaveSlot(TupleTableSlot *slot)
{
	slot->tts_cb->materialize(slot);
}

/*
 * ExecCopySlotTuple
 *
 * A thin wrapper around calling TupleTableSlotType specific copy_heap_tuple()
 * method.
 */
static inline HeapTuple
ExecCopySlotTuple(TupleTableSlot *slot)
{
	/*
	 * sanity checks
	 */
	Assert(slot != NULL);
	Assert(!IS_TTS_EMPTY(slot));
	return slot->tts_cb->copy_heap_tuple(slot);
}

/*
 * ExecCopySlotMinimalTuple
 *
 * A thin wrapper around calling TupleTableSlotType specific
 * copy_minimal_tuple() method.
 */
static inline MinimalTuple
ExecCopySlotMinimalTuple(TupleTableSlot *slot)
{
	return slot->tts_cb->copy_minimal_tuple(slot);
}

/* in access/common/heaptuple.c */

/*
 * slot_getattr
 *
 * This function fetches an attribute of the slot's current tuple.
 *
 * Only a heap tuple has system attributes. Hence the callers which want to get
 * the values of systen attributes are required to call heap_getsysattr()
 * directly on the heap tuple contained in HeapTupleTableSlot or
 * BufferHeapTupleTableSlot.
 */
static inline Datum
slot_getattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
	if (attnum <= 0)
		elog(ERROR, "cannot extract a system attribute from a tuple table slot");

	/*
	 * If the attribute is not available readily, get all the attributes upto
	 * the requested one.
	 */
	if (attnum > slot->tts_nvalid)
		slot->tts_cb->getsomeattrs(slot, attnum);

	/*
	 * We expect getsomeattrs() callback to make the requested attribute to be
	 * available.
	 */
	Assert(attnum <= slot->tts_nvalid);

	*isnull = slot->tts_isnull[attnum - 1];
	return slot->tts_values[attnum - 1];
}

/*
 * slot_getallattrs
 *
 * This function forces all the entries of the slot's Datum/isnull arrays to be
 * valid. The caller may then extract data directly from those arrays instead
 * of using slot_getattr.
 */
static inline void
slot_getallattrs(TupleTableSlot *slot)
{
	int			tdesc_natts = slot->tts_tupleDescriptor->natts;

	/* Quick out if we have 'em all already */
	if (slot->tts_nvalid == tdesc_natts)
		return;

	slot->tts_cb->getsomeattrs(slot, tdesc_natts);
}

/*
 * slot_getsomeattrs
 *
 * This function forces the entries of the slot's Datum/isnull arrays to be
 * valid at least up through the attnum'th entry. It's a thin wrapper around
 * TupleTableSlotType specific getsomeattrs() callback.
 */
static inline void
slot_getsomeattrs(TupleTableSlot *slot, int attnum)
{
	/* TODO: we're sometimes called with attnum == 0 which is wrong */
	Assert(attnum >= 0);

	if (attnum > slot->tts_nvalid)
		slot->tts_cb->getsomeattrs(slot, attnum);
}

/*
 * slot_attisnull
 *
 * Detect whether an attribute of the slot is null, without actually fetching
 * it.
 *
 * TODO: Depending upon the tuple representation we might be able to do
 * something better like heap_isnull(). For that we will require a separate
 * callback attisnull().
 */
static inline bool
slot_attisnull(TupleTableSlot *slot, int attnum)
{
	bool isnull;

	slot_getattr(slot, attnum, &isnull);

	return isnull;
}

extern void slot_getmissingattrs(TupleTableSlot *slot, int startAttNum, int lastAttNum);

#endif							/* TUPTABLE_H */
