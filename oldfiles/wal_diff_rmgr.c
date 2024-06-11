#include "wal_diff_rmgr.h"

/*
 * Redo is just a noop for this module, because we aren't testing recovery of
 * any real structure.
 */

// XLogRecord + XLogRecordDataHeaderShort[Long] + [HeapTupleHeaderData + tuple_data] = main_data 

static void 
redo_insert(XLogReaderState *record)
{
	xl_heap_insert *xlrec;
	Size		freespace = 0;
	XLogRecPtr	lsn = record->EndRecPtr;

	RelFileLocator 		target_locator;
	BlockNumber blkno;
	ItemPointerData 	target_tid;

	Buffer		buffer;
	Page		page;
	XLogRedoAction action;

	Size datalen = 0;
	char* data = XLogRecGetBlockData(record, 0, &datalen);
	HeapTupleHeaderData htup_data;

	HeapTupleHeader htup;
	Size htup_len;

	int bitmap_len;
	int user_data_len;

	memcpy((char*) &htup_data, data, SizeofHeapTupleHeader);
	xlrec = (xl_heap_insert*) (data + htup_data.t_hoff);

	bitmap_len = htup_data.t_hoff - SizeofHeapTupleHeader;
	user_data_len = datalen - SizeofHeapTupleHeader - bitmap_len - SizeOfHeapInsert;

	htup_len = bitmap_len + user_data_len + SizeofHeapTupleHeader;
	htup = palloc0(htup_len);
	memcpy((char*) htup, data, SizeofHeapTupleHeader);
	memcpy((char*) htup + SizeofHeapTupleHeader, (char*) data + SizeofHeapTupleHeader, bitmap_len);
	memcpy((char*) htup + SizeofHeapTupleHeader + bitmap_len, (char*) data + htup_data.t_hoff + SizeOfHeapInsert, user_data_len);
	htup->t_hoff = SizeofHeapTupleHeader + bitmap_len;

	XLogRecGetBlockTag(record, 0, &target_locator, NULL, &blkno);
	ItemPointerSetBlockNumber(&target_tid, blkno);
	ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);

	if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
	{
		Relation	reln = CreateFakeRelcacheEntry(target_locator);
		Buffer		vmbuffer = InvalidBuffer;

		visibilitymap_pin(reln, blkno, &vmbuffer);
		visibilitymap_clear(reln, blkno, vmbuffer, VISIBILITYMAP_VALID_BITS);
		ReleaseBuffer(vmbuffer);
		FreeFakeRelcacheEntry(reln);
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
		page = BufferGetPage(buffer);

		if (PageGetMaxOffsetNumber(page) + 1 < xlrec->offnum)
			elog(PANIC, "invalid max offset number");

		if (PageAddItem(page, (Item) htup, SizeofHeapTupleHeader, xlrec->offnum,
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
	// Buffer				buffer;
	// Page				page;
	// ItemId				lp = NULL;
	// XLogRecPtr			lsn = record->EndRecPtr;
	// HeapTupleHeaderData *record_data = (HeapTupleHeaderData *) record->record->main_data;
	// char				*main_data = record_data->t_bits;
	// RelFileLocator 		target_locator = record->record.;
	// ItemPointerData 	target_tid = record_data->t_ctid;
	// xl_heap_delete 		*xlrec = (xl_heap_delete *) XLogRecGetData(record);

	// if (xlrec->flags & XLH_DELETE_ALL_VISIBLE_CLEARED)
	// {
	// 	// тут пока bruh
	// }

	// if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
	// {
	// 	page = BufferGetPage(buffer);
	// 	if (PageGetMaxOffsetNumber(page) >= xlrec->offnum)
	// 		lp = PageGetItemId(page, xlrec->offnum);	
	// 	record_data->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
	// 	record_data->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	// 	HeapTupleHeaderClearHotUpdated(record_data);
	// 	fix_infomask_from_infobits(xlrec->infobits_set,
	// 							   &record_data->t_infomask, &record_data->t_infomask2);
	// 	if (!(xlrec->flags & XLH_DELETE_IS_SUPER))
	// 		HeapTupleHeaderSetXmax(record_data, xlrec->xmax);
	// 	else
	// 		HeapTupleHeaderSetXmin(record_data, InvalidTransactionId);
	// 	HeapTupleHeaderSetCmax(record_data, FirstCommandId, false);

	// 	/* Mark the page as a candidate for pruning */
	// 	PageSetPrunable(page, XLogRecGetXid(record));

	// 	if (xlrec->flags & XLH_DELETE_ALL_VISIBLE_CLEARED)
	// 		PageClearAllVisible(page);

	// 	/* Make sure t_ctid is set correctly */
	// 	if (xlrec->flags & XLH_DELETE_IS_PARTITION_MOVE)
	// 		HeapTupleHeaderSetMovedPartitions(record_data);
	// 	else
	// 		record_data->t_ctid = target_tid;
	// 	PageSetLSN(page, lsn);
	// 	MarkBufferDirty(buffer);
	// }
	// if (BufferIsValid(buffer))
	// 	UnlockReleaseBuffer(buffer);
}

static void
redo_update(XLogReaderState *record)
{
	// char* record_bytes = record->record->main_data;
	// char* user_data;
	// int offset = 0;
	// int user_data_offset = 0;
	// int user_data_len = 0;
	
	// RelFileLocator locator;
	// ItemPointerData old_ptr_data;
	// HeapTupleHeaderData htup_data;
	// xl_heap_update xl_hdr;
	// uint16 prefix_len;
	// uint16 suffix_len;
	// Buffer old_buff;
	// Buffer new_buff;

	// XLogRedoAction oldaction;
	// XLogRedoAction newaction;

	// Page page;
	// OffsetNumber offnum;
	// ItemId		lp = NULL;
	// HeapTupleHeader htup;

	// XLogRecPtr	lsn = record->EndRecPtr;

	// HeapTupleData oldtup;
	// /* initialize to keep the compiler quiet */
	// oldtup.t_data = NULL;
	// oldtup.t_len = 0;

	// memcpy((char*) &locator, record_bytes, sizeof(RelFileLocator));
	// offset += sizeof(RelFileLocator);

	// memcpy((char*) &old_buff, record_bytes, sizeof(Buffer));
	// offset += sizeof(Buffer);

	// memcpy((char*) &new_buff, record_bytes, sizeof(Buffer));
	// offset += sizeof(Buffer);

	// memcpy((char*) &old_ptr_data, record_bytes, sizeof(ItemPointerData));
	// offset += sizeof(ItemPointerData);

	// memcpy((char*) &htup_data, (char*) record_bytes + offset, SizeofHeapTupleHeader);
	// user_data = (char*) record_bytes + offset + htup_data.t_hoff;
	// offset += SizeofHeapTupleHeader;

	// memcpy((char*) &xl_hdr, user_data, SizeOfHeapUpdate);
	// user_data_offset += SizeOfHeapUpdate;

	// memcpy((char*) &prefix_len, (char*) user_data + user_data_offset, sizeof(uint16));
	// user_data_offset += sizeof(uint16);
	// memcpy((char*) &suffix_len, (char*) user_data + user_data_offset, sizeof(uint16));
	// user_data_offset += sizeof(uint16);

	// /*
	//  * The visibility map may need to be fixed even if the heap page is
	//  * already up-to-date.
	//  */
	// if (xl_hdr.flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED)
	// {
	// 	Relation	reln = CreateFakeRelcacheEntry(locator);
	// 	Buffer		vmbuffer = InvalidBuffer;

	// 	visibilitymap_pin(reln, old_ptr_data.ip_blkid, &vmbuffer);
	// 	visibilitymap_clear(reln, old_ptr_data.ip_blkid, vmbuffer, VISIBILITYMAP_VALID_BITS);
	// 	ReleaseBuffer(vmbuffer);
	// 	FreeFakeRelcacheEntry(reln);
	// }

	// // TODO костыльно предполагаем, что redo всегда нужен для chain записей
	// if (oldaction == BLK_NEEDS_REDO)
	// {
	// 	page = BufferGetPage(old_buff);
	// 	offnum = xl_hdr.old_offnum;
	// 	if (PageGetMaxOffsetNumber(page) >= offnum)
	// 		lp = PageGetItemId(page, offnum);

	// 	if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp))
	// 		elog(PANIC, "invalid lp");

	// 	htup = (HeapTupleHeader) PageGetItem(page, lp);

	// 	oldtup.t_data = htup;
	// 	oldtup.t_len = ItemIdGetLength(lp);

	// 	htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
	// 	htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;

	// 	HeapTupleHeaderClearHotUpdated(htup);

	// 	fix_infomask_from_infobits(xl_hdr.old_infobits_set, &htup->t_infomask,
	// 							   &htup->t_infomask2);
	// 	HeapTupleHeaderSetXmax(htup, xl_hdr.old_xmax);
	// 	HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
	// 	/* Set forward chain link in t_ctid */
	// 	htup->t_ctid = htup_data.t_ctid;

	// 	/* Mark the page as a candidate for pruning */
	// 	PageSetPrunable(page, XLogRecGetXid(record));

	// 	if (xl_hdr.flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED)
	// 		PageClearAllVisible(page);

	// 	PageSetLSN(page, lsn);
	// 	MarkBufferDirty(old_buff);
	// }
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
