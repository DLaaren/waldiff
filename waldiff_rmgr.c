#include "waldiff_rmgr.h"

static OffsetNumber
PageAddPlugExtended(Page page,
					waldiff_rmgr_plug *plug,
					Size size,
					OffsetNumber offsetNumber,
					int flags)
{
    PageHeader	phdr = (PageHeader) page;
	Size		alignedSize;
	int			lower;
	int			upper;
	ItemId		itemId;
	OffsetNumber limit;
	bool		needshuffle = false;

    /*
	 * Be wary about corrupted page pointers
	 */
	if (phdr->pd_lower < SizeOfPageHeaderData ||
		phdr->pd_lower > phdr->pd_upper ||
		phdr->pd_upper > phdr->pd_special ||
		phdr->pd_special > BLCKSZ)
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("corrupted page pointers: lower = %u, upper = %u, special = %u",
						phdr->pd_lower, phdr->pd_upper, phdr->pd_special)));

    /*
	 * Select offsetNumber to place the new item at
	 */
	limit = OffsetNumberNext(PageGetMaxOffsetNumber(page));

    /* was offsetNumber passed in? */
	if (OffsetNumberIsValid(offsetNumber))
	{
        if (offsetNumber < limit)
        {
            itemId = PageGetItemId(page, offsetNumber);
            if (ItemIdIsUsed(itemId) || ItemIdHasStorage(itemId))
            {
                elog(WARNING, "will not overwrite a used ItemId");
                return InvalidOffsetNumber;
            }
            needshuffle = true; /* need to move existing linp's */
        }
    }
	else
	{
        /* don't bother searching if hint says there's no free slot */
        offsetNumber = limit;
    }

    /* Reject placing items beyond the first unused line pointer */
	if (offsetNumber > limit)
	{
		elog(WARNING, "specified item offset is too large");
		return InvalidOffsetNumber;
	}

	/*
	 * Compute new lower and upper pointers for page, see if it'll fit.
	 *
	 * Note: do arithmetic as signed ints, to avoid mistakes if, say,
	 * alignedSize > pd_upper.
	 */
	if (offsetNumber == limit || needshuffle)
		lower = phdr->pd_lower + sizeof(ItemIdData);
	else
		lower = phdr->pd_lower;

	alignedSize = MAXALIGN(size);

	upper = (int) phdr->pd_upper - (int) alignedSize;

	if (lower > upper)
		return InvalidOffsetNumber;

    /*
	 * OK to insert the item.  First, shuffle the existing pointers if needed.
	 */
	itemId = PageGetItemId(page, offsetNumber);

	if (needshuffle)
		memmove(itemId + 1, itemId,
				(limit - offsetNumber) * sizeof(ItemIdData));

	itemId->lp_flags = flags;
	if (itemId->lp_flags == LP_REDIRECT)
	{
		itemId->lp_len = 0;
		itemId->lp_off = plug->offset;
	}
	else if (itemId->lp_flags == LP_NORMAL)
	{
		itemId->lp_len = 0;
		itemId->lp_off = 0;
	}

    /* adjust page header */
	phdr->pd_lower = (LocationIndex) lower;
	phdr->pd_upper = (LocationIndex) upper;

	return offsetNumber;
}

static void
waldiff_redo_plug(XLogReaderState *record)
{   
    Buffer            buffer;
    Page              page;
    XLogRecPtr	lsn = record->EndRecPtr;
    RelFileLocator    target_locator;
	BlockNumber       blkno;
	waldiff_rmgr_plug *block_data;
    Size              block_data_len;
	int 			  flag;
    ItemPointerData   target_tid;
	XLogRedoAction    action;
    Size		      freespace = 0;
    
    Assert(XLogRecGetInfo(record) & WALDIFF_RMGR_PLUG);

    XLogRecGetBlockTag(record, 0, &target_locator, NULL, &blkno);
    block_data = (waldiff_rmgr_plug *) XLogRecGetBlockData(record, 0, &block_data_len);
    Assert(block_data_len == sizeof(waldiff_rmgr_plug));

	ItemPointerSetBlockNumber(&target_tid, blkno);
	ItemPointerSetOffsetNumber(&target_tid, block_data->offset);
	flag = block_data->flag;

    action = XLogReadBufferForRedo(record, 0, &buffer);
    if (action == BLK_NEEDS_REDO)
	{
        page = BufferGetPage(buffer);

        if (PageGetMaxOffsetNumber(page) + 1 <  block_data->offset)
			elog(PANIC, "invalid max offset number");

		if (flag == PLUG_NORMAL)
		{
			if (PageAddPlugExtended(page, block_data, sizeof(waldiff_rmgr_plug), 
									block_data->offset, LP_NORMAL) == InvalidOffsetNumber)
				elog(PANIC, "failed to add tuple");
		}
		else if (flag == PLUG_REDIRECT)
		{
			if (PageAddPlugExtended(page, block_data, sizeof(waldiff_rmgr_plug), 
									block_data->offset, LP_REDIRECT) == InvalidOffsetNumber)
				elog(PANIC, "failed to add tuple");
		}


        PageSetLSN(page, lsn);

        MarkBufferDirty(buffer);
    }
    if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);

    /* XXX ??? */
    if (action == BLK_NEEDS_REDO && freespace < BLCKSZ / 5)
		XLogRecordPageWithFreeSpace(target_locator, blkno, freespace);
}

void
waldiff_rmgr_redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & XLR_RMGR_INFO_MASK;
    
    switch (info) 
    {
        case WALDIFF_RMGR_PLUG:
            waldiff_rmgr_plug(record);
            break;
        default:
            elog(PANIC, "waldiff_rmgr_redo: unknown op code %u", info);
    }
}

/*
 * No information on custom resource managers; just print the ID.
 */
void
waldiff_rmgr_desc(StringInfo buf, XLogReaderState *record)
{
	appendStringInfo(buf, "custrom rmid for waldiff extension: %d", XLogRecGetRmid(record));
}

/*
 * No information on custom resource managers; just return NULL and let the
 * caller handle it.
 */
const char *
waldiff_rmgr_identify(uint8 info)
{
	 char *id = NULL;

	switch (info & XLR_RMGR_INFO_MASK)
	{
        case WALDIFF_RMGR_PLUG:
            id = "WALDIFF_RMGR_PLUG";
			break;
    } 
    return id;
}