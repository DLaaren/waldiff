#include "waldiff_decoding_encoding.h"

static WaldiffRecord decode_insert(XLogRecord *WalRec, XLogRecPtr lsn);

static WaldiffRecord decode_hot_update(XLogRecord *WalRec, XLogRecPtr lsn);

// returns null if contains image
WaldiffRecord
WalDiffDecodeRecord(XLogRecord *WalRec, XLogRecPtr lsn)
{
    switch (WalRec->xl_info & XLOG_HEAP_OPMASK)
    {
        case XLOG_HEAP_INSERT:
            return decode_insert(WalRec, lsn);

        case XLOG_HEAP_HOT_UPDATE:
            return decode_hot_update(WalRec, lsn);

		default: 
			ereport(ERROR, 
					errmsg("WALDIFF: uprocessed type of record in WalDiffDecodeRecord"));
	}
	return NULL;
}

XLogRecord *
WalDiffEncodeRecord(WaldiffRecord WaldiffRecord)
{
	char *WalRec = palloc0(WaldiffRecord->rec_hdr.xl_tot_len);
	Offset currPos = 0;

	RelFileLocator relfileloc;
	BlockNumber blk_num;

	pg_crc32c	crc;

	Assert(WaldiffRecord->rec_hdr.xl_rmid == RM_HEAP_ID && 
		   (GetRecordType(WaldiffRecord) == XLOG_HEAP_INSERT || 
		    GetRecordType(WaldiffRecord) == XLOG_HEAP_HOT_UPDATE));

	memcpy(WalRec + currPos, &(WaldiffRecord->rec_hdr), SizeOfXLogRecord);
	currPos += SizeOfXLogRecord;
	
	for (int id = 0; id <= WaldiffRecord->max_block_id; id++)
	{
		memcpy(WalRec + currPos, &(WaldiffRecord->blocks[id].blk_hdr), SizeOfXLogRecordBlockHeader);
		currPos += SizeOfXLogRecordBlockHeader;

		Assert(!(WaldiffRecord->blocks[id].blk_hdr.fork_flags & BKPBLOCK_HAS_IMAGE));

		if (!(WaldiffRecord->blocks[id].blk_hdr.fork_flags & BKPBLOCK_SAME_REL))
		{
			relfileloc = WaldiffRecord->blocks[id].file_loc;

			memcpy(WalRec + currPos, &relfileloc, sizeof(RelFileLocator));
			currPos += sizeof(RelFileLocator);
		}

		blk_num = WaldiffRecord->blocks[id].blknum;

		memcpy(WalRec + currPos, &blk_num, sizeof(BlockNumber));
		currPos += sizeof(BlockNumber);
	}

	if (WaldiffRecord->main_data_len > 0)
	{
		if (WaldiffRecord->main_data_len < 256)
		{
			XLogRecordDataHeaderShort main_hdr;

			main_hdr.id = XLR_BLOCK_ID_DATA_SHORT;
			main_hdr.data_length = WaldiffRecord->main_data_len;

			memcpy(WalRec + currPos, &main_hdr, SizeOfXLogRecordDataHeaderShort);
			currPos += SizeOfXLogRecordDataHeaderShort;
		}
		else
		{
			XLogRecordDataHeaderLong main_hdr;

			main_hdr.id = XLR_BLOCK_ID_DATA_LONG;

			memcpy(WalRec + currPos, &main_hdr, sizeof(uint8));
			currPos += sizeof(uint8);

			memcpy(WalRec + currPos, &(WaldiffRecord->main_data_len), sizeof(uint32));
			currPos += sizeof(uint32);
		}
	}

	for (int block_id = 0; block_id <= WaldiffRecord->max_block_id; block_id++)
	{
		if (WaldiffRecord->blocks[block_id].has_data) {
			memcpy(WalRec + currPos, WaldiffRecord->blocks[block_id].block_data, WaldiffRecord->blocks[block_id].block_data_len);
			currPos += WaldiffRecord->blocks[block_id].block_data_len;
		}
	}
	
	memcpy(WalRec + currPos, WaldiffRecord->main_data, WaldiffRecord->main_data_len);
	currPos +=  WaldiffRecord->main_data_len;

	Assert(currPos == WaldiffRecord->rec_hdr.xl_tot_len);

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, WalRec + SizeOfXLogRecord, ((XLogRecord *) WalRec)->xl_tot_len - SizeOfXLogRecord);
	COMP_CRC32C(crc, WalRec, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(crc);
	((XLogRecord *) WalRec)->xl_crc = crc;

	return (XLogRecord *) WalRec;
}

/* This function returns palloced struct */
/* Here we reckon that insert does not contain full page image */
/* [XLogRecodr] + [BlockHeader] + [RelFileLocator] + [BlockNumber] + [MainDataHeader] + [block data] + [main data] */
static WaldiffRecord 
decode_insert(XLogRecord *WalRec, XLogRecPtr lsn)
{
	uint32					 curr_pos = 0;
	WaldiffRecord			 WaldiffRec;	
	uint8 					 main_data_hdr_id;	

	WaldiffRec = palloc(SizeOfWaldiffRecord + sizeof(WaldiffBlock));
	Assert(WaldiffRec != NULL);

	WaldiffRec->chain_length = 1;
	WaldiffRec->chain_start_lsn = lsn;
	WaldiffRec->has_main_data = true;
	WaldiffRec->max_block_id = 0;

	memcpy(&(WaldiffRec->rec_hdr), WalRec, SizeOfXLogRecord);
	curr_pos += SizeOfXLogRecord;

	WaldiffRec->t_xmin = WaldiffRec->rec_hdr.xl_xid;

	memcpy(&(WaldiffRec->blocks[0].blk_hdr), 
		   (char *) WalRec + curr_pos, 
		   SizeOfXLogRecordBlockHeader);
	curr_pos += SizeOfXLogRecordBlockHeader;

	Assert(WaldiffRec->blocks[0].blk_hdr.id == 0);
	Assert(!(WaldiffRec->blocks[0].blk_hdr.fork_flags & BKPBLOCK_HAS_IMAGE));
	Assert(WaldiffRec->blocks[0].blk_hdr.fork_flags & BKPBLOCK_HAS_DATA);
	WaldiffRec->blocks[0].has_data = true;
	WaldiffRec->blocks[0].block_data_len = WaldiffRec->blocks[0].blk_hdr.data_length;

	Assert(!(BKPBLOCK_SAME_REL & WaldiffRec->blocks[0].blk_hdr.fork_flags));
	memcpy(&(WaldiffRec->blocks[0].file_loc), 
		   (char *) WalRec + curr_pos,
		   sizeof(RelFileLocator));
	curr_pos += sizeof(RelFileLocator);

	memcpy(&(WaldiffRec->blocks[0].blknum), 
		   (char *) WalRec + curr_pos,
		   sizeof(BlockNumber));
	curr_pos += sizeof(BlockNumber);

	WaldiffRec->blocks[0].forknum = WaldiffRec->blocks[0].blk_hdr.fork_flags & BKPBLOCK_FORK_MASK;

	main_data_hdr_id = *((uint8 *)((char *) WalRec + curr_pos));
	curr_pos += sizeof(uint8);

	if (main_data_hdr_id == XLR_BLOCK_ID_DATA_SHORT)
	{
		WaldiffRec->main_data_len = *((uint8 *)((char *) WalRec + curr_pos));
		curr_pos += sizeof(uint8);
	}
	else if (main_data_hdr_id == XLR_BLOCK_ID_DATA_LONG)
	{
		WaldiffRec->main_data_len = *((uint32 *)((char *) WalRec + curr_pos));
		curr_pos += sizeof(uint32);
	}
	else 
	{
		ereport(ERROR, errmsg("WALDIFF: waiting for main data header but there is something else in record %08X/%08X", LSN_FORMAT_ARGS(lsn)));
	}

	WaldiffRec->blocks[0].block_data = palloc(WaldiffRec->blocks[0].block_data_len);
	memcpy(WaldiffRec->blocks[0].block_data, 
		   (char *) WalRec + curr_pos,
		   WaldiffRec->blocks[0].block_data_len);
	curr_pos += WaldiffRec->blocks[0].block_data_len;  

	WaldiffRec->main_data = palloc(WaldiffRec->main_data_len);
	memcpy(WaldiffRec->main_data, 
		   (char *) WalRec + curr_pos,
		   WaldiffRec->main_data_len);
	curr_pos += WaldiffRec->main_data_len;   


	ItemPointerSet(&(WaldiffRec->current_t_ctid), 
				   WaldiffRec->blocks[0].blknum, 
				   ((xl_heap_insert *)(WaldiffRec->main_data))->offnum);

	Assert(curr_pos == WaldiffRec->rec_hdr.xl_tot_len);

	return WaldiffRec;
}

/* This function returns palloced struct */
/* Here we reckon that insert does not contain full page image */
/* [XLogRecodr] + [BlockHeader] + [RelFileLocator] + [BlockNumber] + [MainDataHeader] + [block data] + [main data] */

/* If XLH_UPDATE_PREFIX_FROM_OLD or XLH_UPDATE_SUFFIX_FROM_OLD flags are set, the prefix and/or suffix come first, as one or two uint16s */

 /* The new tuple data doesn't include the prefix and suffix, which are copied from the old tuple on replay */
static WaldiffRecord 
decode_hot_update(XLogRecord *WalRec, XLogRecPtr lsn)
{
	uint32					 curr_pos = 0;
	WaldiffRecord			 WaldiffRec;	
	uint8 					 main_data_hdr_id;	

	WaldiffRec = palloc(SizeOfWaldiffRecord + sizeof(WaldiffBlock));
	Assert(WaldiffRec != NULL);

	WaldiffRec->chain_length = 1;
	WaldiffRec->chain_start_lsn = lsn;
	WaldiffRec->has_main_data = true;
	WaldiffRec->max_block_id = 0;

	memcpy(&(WaldiffRec->rec_hdr), WalRec, SizeOfXLogRecord);
	curr_pos += SizeOfXLogRecord;

	WaldiffRec->t_xmin = WaldiffRec->rec_hdr.xl_xid;

	memcpy(&(WaldiffRec->blocks[0].blk_hdr), 
		   (char *) WalRec + curr_pos, 
		   SizeOfXLogRecordBlockHeader);
	curr_pos += SizeOfXLogRecordBlockHeader;

	Assert(WaldiffRec->blocks[0].blk_hdr.id == 0);
	Assert(!(WaldiffRec->blocks[0].blk_hdr.fork_flags & BKPBLOCK_HAS_IMAGE));
	Assert(WaldiffRec->blocks[0].blk_hdr.fork_flags & BKPBLOCK_HAS_DATA);
	WaldiffRec->blocks[0].has_data = true;
	WaldiffRec->blocks[0].block_data_len = WaldiffRec->blocks[0].blk_hdr.data_length;

	Assert(!(BKPBLOCK_SAME_REL & WaldiffRec->blocks[0].blk_hdr.fork_flags));
	memcpy(&(WaldiffRec->blocks[0].file_loc), 
		   (char *) WalRec + curr_pos,
		   sizeof(RelFileLocator));
	curr_pos += sizeof(RelFileLocator);

	memcpy(&(WaldiffRec->blocks[0].blknum), 
		   (char *) WalRec + curr_pos,
		   sizeof(BlockNumber));
	curr_pos += sizeof(BlockNumber);

	WaldiffRec->blocks[0].forknum = WaldiffRec->blocks[0].blk_hdr.fork_flags & BKPBLOCK_FORK_MASK;

	main_data_hdr_id = *((uint8 *)((char *) WalRec + curr_pos));
	curr_pos += sizeof(uint8);

	if (main_data_hdr_id == XLR_BLOCK_ID_DATA_SHORT)
	{
		WaldiffRec->main_data_len = *((uint8 *)((char *) WalRec + curr_pos));
		curr_pos += sizeof(uint8);
	}
	else if (main_data_hdr_id == XLR_BLOCK_ID_DATA_LONG)
	{
		WaldiffRec->main_data_len = *((uint32 *)((char *) WalRec + curr_pos));
		curr_pos += sizeof(uint32);
	}
	else 
	{
		ereport(ERROR, errmsg("WALDIFF: waiting for main data header but there is something else in record %08X/%08X", LSN_FORMAT_ARGS(lsn)));
	}

	WaldiffRec->blocks[0].block_data = palloc(WaldiffRec->blocks[0].block_data_len);
	memcpy(WaldiffRec->blocks[0].block_data, 
		   (char *) WalRec + curr_pos,
		   WaldiffRec->blocks[0].block_data_len);
	curr_pos += WaldiffRec->blocks[0].block_data_len;  

	WaldiffRec->main_data = palloc(WaldiffRec->main_data_len);
	memcpy(WaldiffRec->main_data, 
		   (char *) WalRec + curr_pos,
		   WaldiffRec->main_data_len);
	curr_pos += WaldiffRec->main_data_len;   

	ItemPointerSet(&(WaldiffRec->current_t_ctid), 
				   WaldiffRec->blocks[0].blknum, 
				   ((xl_heap_update *)(WaldiffRec->main_data))->new_offnum);

	ItemPointerSet(&(WaldiffRec->prev_t_ctid), 
				   WaldiffRec->blocks[0].blknum, 
				   ((xl_heap_update *)(WaldiffRec->main_data))->old_offnum);

	WaldiffRec->t_xmax = ((xl_heap_update *)(WaldiffRec->main_data))->new_xmax;

	Assert(curr_pos == WaldiffRec->rec_hdr.xl_tot_len);

	return WaldiffRec;
}