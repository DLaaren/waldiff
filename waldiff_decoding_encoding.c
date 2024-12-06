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


//     XLogRecord   	 	  *WALRec = (XLogRecord *) record;
//     WALDIFFRecord 	  	   WDRec; 		// allocate after finding out the number of blocks
// 	WALDIFFRecordData  	   WDRecTemp;
// 	static WALDIFFBlock	 **blocks;

//     Offset      currPos = SizeOfXLogRecord;
//     uint32		remaining = WALRec->xl_tot_len - SizeOfXLogRecord;
// 	uint32		datatotal = 0;
//     uint8		block_id;
//     uint8       max_block_id = -1;

// 	if (blocks == NULL)
// 		blocks = palloc(sizeof(WALDIFFBlock *) * (XLR_MAX_BLOCK_ID + 4)); // let it be so for now

// 	WDRecTemp.has_main_data = false;

//     while (remaining > datatotal)
// 	{
// 		memcpy(&block_id, record + currPos, sizeof(uint8));

// 		if (block_id == XLR_BLOCK_ID_DATA_SHORT)
// 		{
// 			/* XLogRecordDataHeaderShort */
// 			uint8		main_data_len;

// 			memcpy(&main_data_len, record + currPos + sizeof(uint8), sizeof(uint8));
// 			currPos += sizeof(uint8);

// 			WDRecTemp.is_long_header = false;
// 			WDRecTemp.has_main_data = true;
// 			WDRecTemp.main_data_len = main_data_len;
// 			datatotal += main_data_len;
// 			break;				/* by convention, the main data fragment is
// 								 * always last */
// 		}
// 		else if (block_id == XLR_BLOCK_ID_DATA_LONG)
// 		{
// 			/* XLogRecordDataHeaderLong */
// 			uint32		main_data_len;

// 			memcpy(&main_data_len, record + currPos + sizeof(uint8), sizeof(uint32));
// 			currPos += sizeof(uint32);

// 			WDRecTemp.is_long_header = true;
// 			WDRecTemp.has_main_data = true;
// 			WDRecTemp.main_data_len = main_data_len;
// 			datatotal += main_data_len;
// 			break;				/* by convention, the main data fragment is
// 								 * always last */
// 		}

// 		// TODO забей пока
// 		// else if (block_id == XLR_BLOCK_ID_ORIGIN)
// 		// {
// 		// 	WALDIFFBlock *blk;

// 		// 	max_block_id += 1;
// 		// 	COPY_HEADER_FIELD(&decoded->record_origin, sizeof(RepOriginId));
// 		// }
// 		// else if (block_id == XLR_BLOCK_ID_TOPLEVEL_XID)
// 		// {
// 		// 	WALDIFFBlock *blk;

// 		// 	max_block_id += 1;
// 		// 	COPY_HEADER_FIELD(&decoded->toplevel_xid, sizeof(TransactionId));
// 		// }

// 		else if (block_id <= XLR_MAX_BLOCK_ID)
// 		{
// 			/* XLogRecordBlockHeader */
// 			WALDIFFBlock *blk = palloc0(sizeof(WALDIFFBlock));
// 			uint8		  fork_flags;

// 			max_block_id += 1;

// 			memcpy(&(blk->blk_hdr), record + currPos, SizeOfXLogRecordBlockHeader);
// 			currPos += SizeOfXLogRecordBlockHeader;

// 			// We do not process records with image
// 			if (blk->blk_hdr.fork_flags & BKPBLOCK_HAS_IMAGE)
// 				return NULL;

// 			blk->forknum = fork_flags & BKPBLOCK_FORK_MASK;
// 			blk->has_data = ((fork_flags & BKPBLOCK_HAS_DATA) != 0);

// 			memcpy(&(blk->block_data_len), record + currPos, sizeof(uint16));
// 			currPos += sizeof(uint16);

// 			/* cross-check that the HAS_DATA flag is set iff data_length > 0 */
// 			if (blk->has_data && blk->block_data_len == 0)
// 				ereport(ERROR, errmsg("BKPBLOCK_HAS_DATA set, but no data included"));

// 			if (!blk->has_data && blk->block_data_len != 0)
// 				ereport(ERROR, errmsg("BKPBLOCK_HAS_DATA not set, but data length is %u",
// 									  (unsigned int) blk->block_data_len));

// 			datatotal += blk->block_data_len;

// 			if (!(fork_flags & BKPBLOCK_SAME_REL))
// 			{
// 				memcpy(&(blk->file_loc), record + currPos, sizeof(RelFileLocator));
// 				currPos += sizeof(RelFileLocator);
// 			}

// 			memcpy(&(blk->blknum), record + currPos, sizeof(BlockNumber));
// 			currPos += sizeof(BlockNumber);

// 			blocks[max_block_id] = blk;
// 			datatotal += blk->blk_hdr.data_length;
// 		}
// 		else
// 			ereport(ERROR, errmsg("invalid block_id %u", block_id));
// 	}

// 	if (remaining != datatotal)
// 		ereport(ERROR, errmsg("error while decoding a record"));

// 	WDRecTemp.max_block_id = max_block_id;

// 	// All header are parsed, parse data blocks now
// 	for (int block_id; block_id < max_block_id; block_id++)
// 	{
// 		char *block_data;

// 		if (!blocks[block_id]->has_data)
// 			continue;

// 		block_data = palloc(blocks[block_id]->block_data_len);
// 		pfree(blocks[block_id]);
// 	}

// 	if (WDRecTemp.has_main_data) { 
// 		WDRecTemp.main_data = palloc(WDRecTemp.main_data_len);
// 		memcpy(WDRecTemp.main_data, record + currPos, WDRecTemp.main_data_len);
// 	}

// 	WDRec = palloc(sizeof(WALDIFFRecordData) +
// 				   (max_block_id + 1) * sizeof(WALDIFFBlock));

// 	// copy temp
// 	memcpy(WDRec, &WDRecTemp, sizeof(WALDIFFRecord));
// 	memcpy(WDRec + sizeof(WALDIFFRecord), blocks, (max_block_id + 1) * sizeof(WALDIFFBlock));

// 	return WDRec;
// }

// char *
// WalDiffEncodeRecord(WALDIFFRecord WaldiffRecord)
// {
    
// }


// char *
// EncodeWalRecord(DecodedXLogRecord *WalRecDecoded)
// {
// 	char *WalRec = palloc0(MAXALIGN(WalRecDecoded->header.xl_tot_len));
// 	Offset currPos = 0;

// 	memcpy(WalRec + currPos, &(WalRecDecoded->header), SizeOfXLogRecord);
// 	currPos += SizeOfXLogRecord;
	
// 	for (int block_id = 0; block_id <= WalRecDecoded->max_block_id; block_id++)
// 	{
// 		Assert(currPos < WalRecDecoded->header.xl_tot_len);

// 		XLogRecordBlockHeader block_hdr;
// 		block_hdr.id = block_id;
// 		block_hdr.fork_flags = WalRecDecoded->blocks[block_id].flags;
// 		block_hdr.data_length = WalRecDecoded->blocks[block_id].data_len;

// 		// if (writer->writeBufSize == 0x000000c0)
// 		// 	ereport(LOG, errmsg("\nBLOCK ID = %u\nFORK FLAGS = 0x%x\nDATA LEN = %u", block_hdr.id, block_hdr.fork_flags, block_hdr.data_length));

// 		memcpy(WalRec + currPos, &block_hdr, SizeOfXLogRecordBlockHeader);
// 		currPos += SizeOfXLogRecordBlockHeader;

// 		if (block_hdr.fork_flags & BKPBLOCK_HAS_IMAGE)
// 		{
// 			XLogRecordBlockImageHeader image_hdr;
// 			image_hdr.length = WalRecDecoded->blocks[block_id].bimg_len;
// 			image_hdr.hole_offset = WalRecDecoded->blocks[block_id].hole_offset;
// 			image_hdr.bimg_info = WalRecDecoded->blocks[block_id].bimg_info;

// 			// if (writer->writeBufSize == 0x000000c0)
// 			// 	ereport(LOG, errmsg("\nHAS IMAGE\nIMAGE LEN = %u\nHOLE OFFSET = %u\nBIMG INFO = 0x%x", image_hdr.length, image_hdr.hole_offset, image_hdr.bimg_info));

// 			memcpy(WalRec + currPos, &image_hdr, SizeOfXLogRecordBlockImageHeader);
// 			currPos += SizeOfXLogRecordBlockImageHeader;

// 			if (((image_hdr.bimg_info) & BKPIMAGE_HAS_HOLE) && (BKPIMAGE_COMPRESSED(image_hdr.bimg_info)))
// 			{
// 				XLogRecordBlockCompressHeader compress_hdr;
// 				compress_hdr.hole_length = WalRecDecoded->blocks[block_id].hole_length;

// 				// if (writer->writeBufSize == 0x000000c0)
// 				// 	ereport(LOG, errmsg("\nCOMPRESSED IMAGE\n = %u\nHOLE len = %u", compress_hdr.hole_length));

// 				memcpy(WalRec + currPos, &compress_hdr, SizeOfXLogRecordBlockCompressHeader);
// 				currPos += SizeOfXLogRecordBlockCompressHeader;
// 			}
// 		}

// 		if (!(block_hdr.fork_flags & BKPBLOCK_SAME_REL))
// 		{
// 			RelFileLocator relfileloc;
// 			relfileloc = WalRecDecoded->blocks[block_id].rlocator;
// 			memcpy(WalRec + currPos, &relfileloc, sizeof(RelFileLocator));
// 			currPos += sizeof(RelFileLocator);

// 			// if (writer->writeBufSize == 0x000000c0)
// 			// 		ereport(LOG, errmsg("\nHAS RELFILE LOC = 0x%x 0x%x 0x%x", relfileloc.spcOid, relfileloc.dbOid, relfileloc.relNumber));

// 		}

// 		BlockNumber blk_num = WalRecDecoded->blocks[block_id].blkno;
// 		memcpy(WalRec + currPos, &blk_num, sizeof(BlockNumber));
// 		currPos += sizeof(BlockNumber);

// 		// if (writer->writeBufSize == 0x000000c0)
// 		// 			ereport(LOG, errmsg("\nBLOCKNUM = %x", blk_num));
// 	}

// 	if (WalRecDecoded->main_data_len > 0)
// 	{
// 		if (WalRecDecoded->main_data_len < 256)
// 		{
// 			XLogRecordDataHeaderShort main_hdr;
// 			main_hdr.id = XLR_BLOCK_ID_DATA_SHORT;
// 			main_hdr.data_length = WalRecDecoded->main_data_len;

// 			// if (writer->writeBufSize == 0x000000c0)
// 			// 		ereport(LOG, errmsg("\nSHORT MAIN DATA HDR"));

// 			memcpy(WalRec + currPos, &main_hdr, SizeOfXLogRecordDataHeaderShort);
// 			currPos += SizeOfXLogRecordDataHeaderShort;
// 		}
// 		else
// 		{
// 			XLogRecordDataHeaderLong main_hdr;
// 			main_hdr.id = XLR_BLOCK_ID_DATA_LONG;

// 			// if (writer->writeBufSize == 0x000000c0)
// 			// 		ereport(LOG, errmsg("\nLONG MAIN DATA HDR"));

// 			memcpy(WalRec + currPos, &main_hdr, sizeof(uint8));
// 			currPos += sizeof(uint8);

// 			memcpy(WalRec + currPos, &(WalRecDecoded->main_data_len), sizeof(uint32));
// 			currPos += sizeof(uint32);
// 		}
// 	}

// 	for (int block_id = 0; block_id <= WalRecDecoded->max_block_id; block_id++)
// 	{
// 		Assert(currPos < WalRecDecoded->header.xl_tot_len);
// 		if (WalRecDecoded->blocks[block_id].has_data) {
// 			memcpy(WalRec + currPos, WalRecDecoded->blocks[block_id].data, WalRecDecoded->blocks[block_id].data_len);
// 			currPos += WalRecDecoded->blocks[block_id].data_len;
// 		}

// 		if (WalRecDecoded->blocks[block_id].has_image)
// 		{
// 			memcpy(WalRec + currPos, WalRecDecoded->blocks[block_id].bkp_image, WalRecDecoded->blocks[block_id].bimg_len);
// 			currPos += WalRecDecoded->blocks[block_id].bimg_len;
// 		}
// 	}
	
// 	memcpy(WalRec + currPos, WalRecDecoded->main_data, WalRecDecoded->main_data_len);
// 	currPos +=  WalRecDecoded->main_data_len;

// 	ereport(LOG, errmsg("LSN = %X/%X :: currPos = %u; tot_len = %u", LSN_FORMAT_ARGS(WalRecDecoded->lsn), currPos, WalRecDecoded->header.xl_tot_len));
// 	Assert(currPos == WalRecDecoded->header.xl_tot_len);

// 	return WalRec;
// }

// /*
//  * fetch_hot_update
//  * 
//  * Backup blk 0: new page
//  *
//  * If XLH_UPDATE_PREFIX_FROM_OLD or XLH_UPDATE_SUFFIX_FROM_OLD flags are set,
//  * the prefix and/or suffix come first, as one or two uint16s.
//  *
//  * After that, xl_heap_header and new tuple data follow.  The new tuple
//  * data doesn't include the prefix and suffix, which are copied from the
//  * old tuple on replay.
//  *
//  * If XLH_UPDATE_CONTAINS_NEW_TUPLE flag is given, the tuple data is
//  * included even if a full-page image was taken.
//  */
// static void 
// fetch_hot_update(WaldiffRecord *WaldiffRec)
// {
// 	DecodedXLogRecord 		*decoded_record = reader_state->record;
// 	RelFileLocator 			 rel_file_locator;
// 	ForkNumber 				 forknum;
// 	BlockNumber 			 blknum;
// 	xl_heap_update 			*main_data;
// 	XLogRecordBlockHeader 	 block_hdr;
// 	char 					*block_data;
// 	Size					 block_data_len;

// 	/* HEAP_UPDATE_HOT contains one block */
// 	Assert(decoded_record->max_block_id == 0);

// 	*WaldiffRec = palloc0(SizeOfWALDIFFRecord + sizeof(WALDIFFBlock) * (decoded_record->max_block_id + 1));
// 	Assert(*WaldiffRec != NULL);

// 	XLogRecGetBlockTag(reader_state, 0, &rel_file_locator, &forknum, &blknum);
// 	main_data = (xl_heap_update *) XLogRecGetData(reader_state);
// 	block_data = XLogRecGetBlockData(reader_state, 0, &block_data_len);
// 	block_hdr.id = 0;
// 	block_hdr.fork_flags = decoded_record->blocks[0].flags;
// 	block_hdr.data_length = block_data_len;
// 	Assert(XLogRecHasBlockData(reader_state, 0));

// 	(*WaldiffRec)->type = XLOG_HEAP_HOT_UPDATE;
// 	(*WaldiffRec)->lsn = decoded_record->lsn;
// 	(*WaldiffRec)->rec_hdr = decoded_record->header;
// 	(*WaldiffRec)->t_xmin = main_data->old_xmax;
// 	(*WaldiffRec)->t_xmax = main_data->new_xmax;
// 	/* 
// 	 * Copy tuple's version pointers
// 	 * At this step, t_ctid always will be point to itself,
// 	 * because we reckon this record as first
// 	 */
// 	ItemPointerSetBlockNumber(&((*WaldiffRec)->current_t_ctid), blknum);
//     ItemPointerSetOffsetNumber(&((*WaldiffRec)->current_t_ctid), main_data->new_offnum);
// 	ItemPointerSetBlockNumber(&((*WaldiffRec)->prev_t_ctid), blknum);
//     ItemPointerSetOffsetNumber(&((*WaldiffRec)->prev_t_ctid), main_data->old_offnum);
// 	/* Copy main data */
// 	(*WaldiffRec)->main_data = palloc0(SizeOfHeapUpdate);
// 	memcpy((*WaldiffRec)->main_data, main_data, SizeOfHeapUpdate);
// 	(*WaldiffRec)->main_data_len = SizeOfHeapUpdate;
// 	/* Copy 0th block */
// 	(*WaldiffRec)->max_block_id 				= 0;
// 	(*WaldiffRec)->blocks[0].blk_hdr 		= block_hdr;
// 	(*WaldiffRec)->blocks[0].file_loc 		= rel_file_locator;
// 	(*WaldiffRec)->blocks[0].forknum			= forknum;
// 	(*WaldiffRec)->blocks[0].blknum 			= blknum;
// 	(*WaldiffRec)->blocks[0].has_data 		= true;
// 	(*WaldiffRec)->blocks[0].block_data_len 	= block_data_len;
// 	(*WaldiffRec)->blocks[0].block_data 		= palloc0(block_data_len);
// 	memcpy((*WaldiffRec)->blocks[0].block_data, block_data, block_data_len);

// 	(*WaldiffRec)->chain_length = 0;
// }

// /*
//  * constructWALDIFF
//  * 
//  * Creates WALDIFF records according to data in hash table
//  * 
//  */
// XLogRecord * 
// constructWALDIFF(WaldiffRecord WaldiffRec)
// {
// 	XLogRecord *constructed_record = palloc0(WaldiffRec->rec_hdr.xl_tot_len);
// 	off_t 		curr_off = 0;
// 	pg_crc32c	crc;

// 	Assert(WaldiffRec->rec_hdr.xl_rmid == RM_HEAP_ID && 
// 		   (WaldiffRec->type == XLOG_HEAP_INSERT || 
// 		   WaldiffRec->type == XLOG_HEAP_HOT_UPDATE));

// 	/* XLogRecord */
// 	memcpy(constructed_record, &(WaldiffRec->rec_hdr), SizeOfXLogRecord);
// 	curr_off += SizeOfXLogRecord;

// 	/* XLogRecordBlockHeader */
// 	Assert(WaldiffRec->max_block_id == 0);
// 	memcpy(constructed_record + curr_off, &(WaldiffRec->blocks[0].blk_hdr), SizeOfXLogRecordBlockHeader);
// 	curr_off += SizeOfXLogRecordBlockHeader;

// 	/* RelFileLocator */
// 	if (!(WaldiffRec->blocks[0].blk_hdr.fork_flags & BKPBLOCK_SAME_REL))
// 	{
// 		memcpy(constructed_record + curr_off, &(WaldiffRec->blocks[0].file_loc), sizeof(RelFileLocator));
// 		curr_off += sizeof(RelFileLocator);
// 	}

// 	/* BlockNumber */
// 	memcpy(constructed_record + curr_off, &(WaldiffRec->blocks[0].blknum), sizeof(BlockNumber));

// 	/* XLogRecordDataHeader[Short|Long] */
// 	if (WaldiffRec->main_data_len < 256)
// 	{
// 		XLogRecordDataHeaderShort main_data_hdr = {XLR_BLOCK_ID_DATA_SHORT, WaldiffRec->main_data_len};

// 		memcpy(constructed_record + curr_off, &main_data_hdr, SizeOfXLogRecordDataHeaderShort);
// 		curr_off += SizeOfXLogRecordDataHeaderShort;
// 	}
// 	else
// 	{
// 		XLogRecordDataHeaderLong main_data_hdr_long = {XLR_BLOCK_ID_DATA_LONG};

// 		memcpy(constructed_record + curr_off, &main_data_hdr_long, sizeof(uint8));
// 		curr_off += sizeof(uint8);
// 		memcpy(constructed_record + curr_off, &(WaldiffRec->main_data_len), sizeof(uint32));
// 		curr_off += sizeof(uint32);
// 	}

// 	/* main data */
// 	memcpy(constructed_record + curr_off, WaldiffRec->main_data, WaldiffRec->main_data_len);
// 	curr_off += WaldiffRec->main_data_len;

// 	Assert(WaldiffRec->rec_hdr.xl_tot_len == curr_off);

// 	/* calculate CRC */
// 	INIT_CRC32C(crc);
// 	COMP_CRC32C(crc, ((char *) constructed_record) + SizeOfXLogRecord, constructed_record->xl_tot_len - SizeOfXLogRecord);
// 	COMP_CRC32C(crc, (char *) constructed_record, offsetof(XLogRecord, xl_crc));
// 	FIN_CRC32C(crc);
// 	constructed_record->xl_crc = crc;

// 	return constructed_record;
// }

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
static WaldiffRecord 
decode_hot_update(XLogRecord *WalRec, XLogRecPtr lsn)
{
    return NULL;
}