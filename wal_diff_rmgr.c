#include "wal_diff_rmgr.h"

/*
 * Redo is just a noop for this module, because we aren't testing recovery of
 * any real structure.
 */

// XLogRecord + XLogRecordDataHeaderShort[Long] + [HeapTupleHeaderData + tuple_data] = main_data 

static void 
redo_insert(XLogReaderState *record)
{
	Buffer 				buffer;
	Page				page;
	XLogRedoAction 		action;
	XLogRecPtr			lsn = record->EndRecPtr;
	HeapTupleHeaderData *record_data = (HeapTupleHeaderData *) record->record->main_data;
	char				*main_data = record_data->t_bits;
	RelFileLocator 		target_locator = record->record.;
	ItemPointerData 	target_tid = record_data->t_ctid;
	xl_heap_delete 		*xlrec = (xl_heap_delete *) XLogRecGetData(record);
	uint32				newlen;

	if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
	{
		// тут пока bruh
	}

	/*
	 * If we inserted the first and only tuple on the page, re-initialize the
	 * page from scratch.
	 */
	if (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE)
	{
		buffer = XLogInitBufferForRedo(record, 0);
		page = BufferGetPage(buffer);
		PageInit(page, BufferGetPageSize(buffer), 0);
		action = BLK_NEEDS_REDO;
	}
	else
		action = XLogReadBufferForRedo(record, 0, &buffer);
	if (action == BLK_NEEDS_REDO)
	{
		Size		datalen;
		char	   *data;

		page = BufferGetPage(buffer);

		if (PageGetMaxOffsetNumber(page) + 1 < xlrec->offnum)
			elog(PANIC, "invalid max offset number");

		data = XLogRecGetBlockData(record, 0, &datalen);

		newlen = datalen - SizeOfHeapHeader;
		Assert(datalen > SizeOfHeapHeader && newlen <= MaxHeapTupleSize);
		memcpy((char *) &xlhdr, data, SizeOfHeapHeader);
		data += SizeOfHeapHeader;

		htup = &tbuf.hdr;
		MemSet((char *) htup, 0, SizeofHeapTupleHeader);
		/* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
		memcpy((char *) htup + SizeofHeapTupleHeader,
			   data,
			   newlen);
		newlen += SizeofHeapTupleHeader;
		htup->t_infomask2 = xlhdr.t_infomask2;
		htup->t_infomask = xlhdr.t_infomask;
		htup->t_hoff = xlhdr.t_hoff;
		HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
		HeapTupleHeaderSetCmin(htup, FirstCommandId);
		htup->t_ctid = target_tid;

		if (PageAddItem(page, (Item) htup, newlen, xlrec->offnum,
						true, true) == InvalidOffsetNumber)
			elog(PANIC, "failed to add tuple");

		freespace = PageGetHeapFreeSpace(page); /* needed to update FSM below */

		PageSetLSN(page, lsn);

		if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
			PageClearAllVisible(page);

		/* XLH_INSERT_ALL_FROZEN_SET implies that all tuples are visible */
		if (xlrec->flags & XLH_INSERT_ALL_FROZEN_SET)
			PageSetAllVisible(page);

		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);

	/*
	 * If the page is running low on free space, update the FSM as well.
	 * Arbitrarily, our definition of "low" is less than 20%. We can't do much
	 * better than that without knowing the fill-factor for the table.
	 *
	 * XXX: Don't do this if the page was restored from full page image. We
	 * don't bother to update the FSM in that case, it doesn't need to be
	 * totally accurate anyway.
	 */
	if (action == BLK_NEEDS_REDO && freespace < BLCKSZ / 5)
		XLogRecordPageWithFreeSpace(target_locator, blkno, freespace);
}

static void
redo_delete(XLogReaderState *record)
{
	Buffer				buffer;
	Page				page;
	ItemId				lp = NULL;
	XLogRecPtr			lsn = record->EndRecPtr;
	HeapTupleHeaderData *record_data = (HeapTupleHeaderData *) record->record->main_data;
	char				*main_data = record_data->t_bits;
	RelFileLocator 		target_locator = record->record.;
	ItemPointerData 	target_tid = record_data->t_ctid;
	xl_heap_delete 		*xlrec = (xl_heap_delete *) XLogRecGetData(record);

	if (xlrec->flags & XLH_DELETE_ALL_VISIBLE_CLEARED)
	{
		// тут пока bruh
	}

	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
	{
		page = BufferGetPage(buffer);
		if (PageGetMaxOffsetNumber(page) >= xlrec->offnum)
			lp = PageGetItemId(page, xlrec->offnum);	
		record_data->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
		record_data->t_infomask2 &= ~HEAP_KEYS_UPDATED;
		HeapTupleHeaderClearHotUpdated(record_data);
		fix_infomask_from_infobits(xlrec->infobits_set,
								   &record_data->t_infomask, &record_data->t_infomask2);
		if (!(xlrec->flags & XLH_DELETE_IS_SUPER))
			HeapTupleHeaderSetXmax(record_data, xlrec->xmax);
		else
			HeapTupleHeaderSetXmin(record_data, InvalidTransactionId);
		HeapTupleHeaderSetCmax(record_data, FirstCommandId, false);

		/* Mark the page as a candidate for pruning */
		PageSetPrunable(page, XLogRecGetXid(record));

		if (xlrec->flags & XLH_DELETE_ALL_VISIBLE_CLEARED)
			PageClearAllVisible(page);

		/* Make sure t_ctid is set correctly */
		if (xlrec->flags & XLH_DELETE_IS_PARTITION_MOVE)
			HeapTupleHeaderSetMovedPartitions(record_data);
		else
			record_data->t_ctid = target_tid;
		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

static void
redo_update(XLogReaderState *record)
{
	HeapTupleHeaderData *record_data = (HeapTupleHeaderData *) record->record->main_data;
}

void
waldiff_rmgr_redo(XLogReaderState *record)
{
	uint8		opcode = XLogRecGetInfo(record) & XLOG_HEAP_OPMASK;

	XLogRecord main_data = record->record->header; 

	switch (opcode)
	{
		case XLOG_HEAP_INSERT:
			redo_insert(record);
			break;
		case XLOG_HEAP_DELETE:
			redo_delete(record);
			break;
		case XLOG_HEAP_UPDATE:
			redo_update(record);
			break;
		default:
			elog(PANIC, "heap_redo: unknown op code %u", opcode);
	}
}

/*
 * No information on custom resource managers; just print the ID.
 */
void
waldiff_rmgr_desc(StringInfo buf, XLogReaderState *record)
{
	appendStringInfo(buf, "rmid: %d", XLogRecGetRmid(record));
}

/*
 * No information on custom resource managers; just return NULL and let the
 * caller handle it.
 */
const char *
waldiff_rmgr_identify(uint8 info)
{
	return NULL;
}
