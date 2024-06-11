#include "waldiff.h"
#include "waldiff_writer.h"
#include "waldiff_test.h"

PG_MODULE_MAGIC;

/* GUC value's store */
static char *waldiff_dir;

/* Global data */
static MemoryContextStorage *memory_context_storage;
static WALDIFFWriterState   *writer_state;
static XLogReaderState		*reader_state;
static HTAB 				*hash_table;

typedef struct HTABElem
{
	uint32_t 	  key;
	WALDIFFRecord data;
} HTABElem;

typedef struct XLogReaderPrivate
{
	TimeLineID	timeline;
	XLogRecPtr	startptr;
	XLogRecPtr	endptr;
	bool		endptr_reached;
} XLogReaderPrivate;


/* Forward declaration */
static void waldiff_startup(ArchiveModuleState *state);
static bool waldiff_configured(ArchiveModuleState *state);
static bool waldiff_archive(ArchiveModuleState *state, const char *file, const char *path);
static void waldiff_shutdown(ArchiveModuleState *state);

/* Custom defined callbacks for WALDIFFReader and WALDIFFWriter */
void WalOpenSegment(XLogReaderState *state,
					XLogSegNo nextSegNo,
					TimeLineID *tli_p);
void WALDIFFOpenSegment(WALDIFFSegmentContext *segcxt, WALDIFFSegment *seg);
int WalReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
				XLogRecPtr targetPtr, char *readBuff);
void WalCloseSegment(XLogReaderState *state);				
void WALDIFFCloseSegment(WALDIFFSegment *seg);
WALDIFFRecordWriteResult WALDIFFWriteRecord(WALDIFFWriterState *waldiff_writer,
											XLogRecord *record);

/* This three fuctions returns palloced struct */
static WALDIFFRecord fetch_insert(XLogReaderState *record);
static WALDIFFRecord fetch_update(XLogReaderState *record);
static WALDIFFRecord fetch_delete(XLogReaderState *record);

static void overlay_update(WALDIFFRecord prev_tup, WALDIFFRecord curr_tup);

static int getWALsegsize(const char *WALpath);											 
static void constructWALDIFFs(void);
// do we need this?
// need to check how postgres deal with not full WAL segment
static void finishWALDIFFSegment(void);

#define GetHashKeyOfWALDIFFRecord(record) \
( \
	(uint32_t)((record)->blocks[0].file_loc.spcOid + \
			   (record)->blocks[0].file_loc.dbOid + \
			   (record)->blocks[0].file_loc.relNumber + \
			   (record)->current_t_ctid.ip_blkid.bi_hi + \
			   (record)->current_t_ctid.ip_blkid.bi_lo + \
			   (record)->current_t_ctid.ip_posid) \
)

#define GetHashKeyOfPrevWALDIFFRecord(record) \
( \
	(uint32_t)((record)->blocks[0].file_loc.spcOid + \
			   (record)->blocks[0].file_loc.dbOid + \
			   (record)->blocks[0].file_loc.relNumber + \
			   (record)->prev_t_ctid.ip_blkid.bi_hi + \
			   (record)->prev_t_ctid.ip_blkid.bi_lo + \
			   (record)->prev_t_ctid.ip_posid) \
)

/*
 * _PG_init
 *
 * Defines the module's GUC.
 *
 */
void 
_PG_init(void)
{
	DefineCustomStringVariable("waldiff.waldiff_dir",
							   "WALDIFF destination directory",
							   NULL,
							   &waldiff_dir,
							   NULL,
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	MarkGUCPrefixReserved("waldiff");
}

/* Module's archiving callbacks. */
static const ArchiveModuleCallbacks waldiff_callbacks = {
    .startup_cb 		 = waldiff_startup,
	.check_configured_cb = waldiff_configured,
	.archive_file_cb 	 = waldiff_archive,
	.shutdown_cb 		 = waldiff_shutdown
};

/*
 * _PG_archive_module_init
 *
 * Returns the module's archiving callbacks.
 */
const ArchiveModuleCallbacks *
_PG_archive_module_init(void)
{
	return &waldiff_callbacks;
}

/*
 * waldiff_startup
 *
 * Creates the module's memory context, WALDIFFWriter and WALDIFFReader.
 */
void 
waldiff_startup(ArchiveModuleState *state)
{
	/* The value should be a power of 2 */
	enum {hash_table_initial_size = 128};
    HASHCTL hash_ctl;

    /* First, allocating the archive module's memory context */
	if (memory_context_storage == NULL)
		memory_context_storage = (MemoryContextStorage *) MemoryContextAllocZero(TopMemoryContext, 
		  																		 sizeof(MemoryContextStorage));
	Assert(memory_context_storage != NULL);

	memory_context_storage->current = AllocSetContextCreate(TopMemoryContext,
										                    "waldiff",
										                    ALLOCSET_DEFAULT_SIZES);
    Assert(memory_context_storage->current != NULL);

	memory_context_storage->old = MemoryContextSwitchTo(memory_context_storage->current);    
	Assert(memory_context_storage->old != NULL);

	ereport(LOG, errmsg("Memory contextes were allocated successfully"));

    /* Secondly, allocating the hash table */
    hash_ctl.keysize    = sizeof(uint32_t);
	hash_ctl.entrysize 	= sizeof(HTABElem);
	hash_ctl.hash 		= &tag_hash;
	/* It is said hash table must have its own memory context */
	// hash_ctl.hcxt 		= tmemory_conext_storage->current;      
	hash_ctl.hcxt = AllocSetContextCreate(memory_context_storage->current,
										  "WALDIFF_HTAB",
										  ALLOCSET_DEFAULT_SIZES);  
	Assert(hash_ctl.hcxt != NULL);								  

    hash_table = hash_create("WALDIFFHashTable", hash_table_initial_size,
                             &hash_ctl, HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);
	Assert(hash_table != NULL);

	ereport(LOG, errmsg("Hash tables was allocated successfully"));
}

/*
 * waldiff_configured
 *
 * Checks that the provided archive directory exists. If it does not, then
 * use default archive directory name.
 */
bool 
waldiff_configured(ArchiveModuleState *state)
{
    struct stat st;

	if (waldiff_dir == NULL || waldiff_dir[0] == '\0')
	{
		GUC_check_errmsg("WALDIFF archive directory name is not set or blank");
		return false;
	}
	if (strlen(waldiff_dir) >= MAXPGPATH)
	{
		GUC_check_errmsg("WALDIFF archive directory name is too long");
		return false;
	}	
	if (stat(waldiff_dir, &st) != 0 || !S_ISDIR(st.st_mode))
	{
		GUC_check_errmsg("Specified WALDIFF archive directory does not exist: %m");
        GUC_check_errmsg("Creating WALDIFF archive directory");
		if (pg_mkdir_p(waldiff_dir, 0700) != 0)
		{
			GUC_check_errmsg("Could not allocate specified WALDIFF directory: %m");
			return false;
		}
	}
	return true;
}

/*
 * waldiff_archive
 *
 * Creates and archives one WALDIFF segment.
 * 
 * file - just name of the WAL file 
 * path - the full path including the WAL file name
 * 
 */
bool 
waldiff_archive(ArchiveModuleState *state, const char *WALfile, const char *WALpath)
{
	/* Preparations */
	static int wal_segment_size = 0;
	XLogReaderPrivate reader_private = {0};

	if (wal_segment_size == 0)
		wal_segment_size = getWALsegsize(WALpath);
	Assert(IsValidWalSegSize(wal_segment_size));

	ereport(LOG, errmsg("WAL segment size is: %d", wal_segment_size));

	/* Determines tli, segno and startPtr values of archived WAL segment
	 * and future WALDIFF segment
	 */
	{
		XLogSegNo segno;

		reader_private.timeline = 1;
		reader_private.startptr = InvalidXLogRecPtr;
		reader_private.endptr = InvalidXLogRecPtr;
		reader_private.endptr_reached = false;

		XLogFromFileName(WALfile, &reader_private.timeline, &segno, wal_segment_size);
		XLogSegNoOffsetToRecPtr(segno, 0, wal_segment_size, reader_private.startptr);
		XLogSegNoOffsetToRecPtr(segno + 1, 0, wal_segment_size, reader_private.endptr);
		Assert(reader_private.startptr != InvalidXLogRecPtr);

		WALDIFFBeginWrite(writer_state, reader_private.startptr, segno, reader_private.timeline);

		ereport(LOG, errmsg("segNo is: %lu", segno));
		ereport(LOG, errmsg("tli is: %u", reader_private.timeline));
		ereport(LOG, errmsg("startptr is: %lu", reader_private.startptr));
		ereport(LOG, errmsg("endptr is: %lu", reader_private.endptr));
	}


	if (reader_state == NULL)
		reader_state = XLogReaderAllocate(wal_segment_size, waldiff_dir,
										  XL_ROUTINE(.page_read = WalReadPage,
													 .segment_open = WalOpenSegment,
													 .segment_close = WalCloseSegment),
										  &reader_private);
	Assert(reader_state != NULL);

	ereport(LOG, errmsg("WALDIFFReader was allocated successfully"));

    if (writer_state == NULL)
		writer_state = WALDIFFWriterAllocate(wal_segment_size, XLOGDIR, 
											 WALDIFFWRITER_ROUTINE(.write_records = WALDIFFWriteRecord,
									  							   .segment_open = WALDIFFOpenSegment,
									  							   .segment_close = WALDIFFCloseSegment));
	Assert(writer_state != NULL);

	ereport(LOG, errmsg("WALDIFFWriter was allocated successfully"));

	/* Main work */
	ereport(LOG, errmsg("archiving WAL file: %s", WALpath));

	{
		XLogPageHeader page_hdr;
		XLogRecPtr 	   first_record = XLogFindNextRecord(reader_state, reader_private.startptr);
		
		if (first_record == InvalidXLogRecPtr)
		{
			ereport(FATAL, 
					errmsg("could not find a valid record after %X/%X", 
							LSN_FORMAT_ARGS(reader_private.startptr)));
			return false;
		}

		page_hdr = (XLogPageHeader) reader_state->readBuf;

		// This cases we should consider later 
		if (page_hdr->xlp_rem_len)
			ereport(LOG, 
					errmsg("got some remaining data from a previous page : %d", page_hdr->xlp_rem_len));

		if (first_record != reader_private.startptr && 
			XLogSegmentOffset(reader_private.startptr, wal_segment_size) != 0)
			ereport(LOG, 
					errmsg("skipping over %u bytes", (uint32) (first_record - reader_private.startptr)));
	}

	/* Main reading & constructing & writing loop */
	for (;;)
	{
		WALDIFFRecord WDRec;
		XLogRecord 	  *WALRec;
		char 		  *errormsg;

		WALRec = XLogReadRecord(reader_state, &errormsg);
		if (WALRec == InvalidXLogRecPtr) {
			if (reader_private.endptr_reached)
				break;

            ereport(ERROR, 
					errmsg("XLogReadRecord failed to read WAL record: %s", errormsg));
        }

		/* Now we're processing only several HEAP type WAL records and without image*/
		if (XLogRecGetRmid(reader_state)== RM_HEAP_ID && !XLogRecHasBlockImage(reader_state, 0))
		{
			uint32_t prev_hash_key;
			uint32_t hash_key;
			HTABElem *entry;
			bool is_found;
			uint8 xlog_type = XLogRecGetInfo(reader_state) & XLOG_HEAP_OPMASK;

			switch(xlog_type)
			{
				case XLOG_HEAP_INSERT:
					WDRec = fetch_insert(reader_state);
					if (WDRec == NULL)
						ereport(ERROR, errmsg("fetch_insert failed"));

					hash_key = GetHashKeyOfWALDIFFRecord(WDRec);
					hash_search(hash_table, (void*) &hash_key, HASH_FIND, &is_found);
					if (is_found)
						entry = (HTABElem *) hash_search(hash_table, (void*) &hash_key, HASH_REMOVE, NULL);

					entry = (HTABElem *) hash_search(hash_table, (void*) &hash_key, HASH_ENTER, NULL);
					entry->data = WDRec;
					Assert(entry->key == hash_key);
					
					break;

				case XLOG_HEAP_UPDATE:
					WDRec = fetch_update(reader_state);
					if (WDRec == NULL)
						ereport(ERROR, errmsg("fetch_update failed"));

					prev_hash_key = GetHashKeyOfPrevWALDIFFRecord(WDRec);
					hash_key = GetHashKeyOfWALDIFFRecord(WDRec);

					entry = (HTABElem *) hash_search(hash_table, (void*) &prev_hash_key, HASH_FIND, &is_found);
					if (is_found)
					{
						uint8 prev_xlog_type = entry->data->rec_hdr.xl_info & XLOG_HEAP_OPMASK;
						bool is_insert_WDrec = (prev_xlog_type == XLOG_HEAP_INSERT);
						overlay_update(entry->data, WDRec);

						if (!is_insert_WDrec) 
						{
							entry = (HTABElem *) hash_search(hash_table, (void*) &prev_hash_key, HASH_REMOVE, NULL);
							entry = (HTABElem *) hash_search(hash_table, (void*) &hash_key, HASH_ENTER, NULL);
							entry->data = WDRec;
							Assert(entry->key == hash_key);
						}
					}
					else 
					{
						entry = (HTABElem *) hash_search(hash_table, (void*) &hash_key, HASH_ENTER, NULL);
						entry->data = WDRec;
						Assert(entry->key == hash_key);
					}

					break;

				case XLOG_HEAP_DELETE:
					WDRec = fetch_delete(reader_state);
					if (WDRec == NULL)
						ereport(ERROR, errmsg("fetch_delete failed"));

					prev_hash_key = GetHashKeyOfPrevWALDIFFRecord(WDRec);
					hash_key = GetHashKeyOfWALDIFFRecord(WDRec);

					entry = hash_search(hash_table, (void*) &prev_hash_key, HASH_FIND, &is_found);
					if (is_found)
					{
						/* if prev WDRec is not presented in the HTAB then it's not in WALDIFF segment */
						entry = (HTABElem *) hash_search(hash_table, (void*) &prev_hash_key, HASH_REMOVE, NULL);
						Assert(entry == NULL);
					}

					/* insert/update (the prev record) is in another WAL segment */
					else 
					{
						entry = (HTABElem *) hash_search(hash_table, (void*) &hash_key, HASH_ENTER, NULL);
						entry->data = WDRec;
						Assert(entry->key == hash_key);
					}

					break;

				/* unprocessed record type */
				default:
					WALDIFFRecordWriteResult res = WALDIFFWriteRecord(writer_state, WALRec);
					if (res == WALDIFFWRITE_FAIL) 
						ereport(ERROR, errmsg("error during writing WALDIFF records in waldiff_archive"));
					break;
			}
		} 
		
		else 
		{
			WALDIFFRecordWriteResult res = WALDIFFWriteRecord(writer_state, WALRec);
			if (res == WALDIFFWRITE_FAIL) 
				ereport(ERROR, errmsg("error during writing WALDIFF records in waldiff_archive"));
		}
	}

	Assert(((writer_state->seg.segno + 1) * writer_state->segcxt.segsize) - 
			writer_state->segoff > 0);

	ereport(LOG, errmsg("start constructing WALDIFFs"));

	/* Constructing and writing WALDIFFs to WALDIFF segment */
	constructWALDIFFs();

	ereport(LOG, errmsg("finishing WALDIFF segment"));
	
	/* Questionable */
	/* End the segment with SWITCH record */
	finishWALDIFFSegment();

	ereport(LOG, errmsg("archived WAL file: %s", WALpath));

	return true;
}

/*
 * walldiff_shutdown
 *
 * Frees all allocated reaources.
 */
void 
waldiff_shutdown(ArchiveModuleState *state)
{
	close(writer_state->seg.fd);

	/* Must have 'case this function closes fd */
	XLogReaderFree(reader_state);

	hash_destroy(hash_table);

	MemoryContextSwitchTo(memory_context_storage->old);
	Assert(CurrentMemoryContext != memory_context_storage->current);
	MemoryContextDelete(memory_context_storage->current);
}

void 
WalOpenSegment(XLogReaderState *state,
			   XLogSegNo nextSegNo,
			   TimeLineID *tli_p)
{
	TimeLineID tli = *tli_p;
    char fname[XLOG_FNAME_LEN];
	char fpath[MAXPGPATH];

    XLogFileName(fname, tli, nextSegNo, state->segcxt.ws_segsize);

	if (snprintf(fpath, MAXPGPATH, "%s/%s", state->segcxt.ws_dir, fname) == -1)
		ereport(ERROR,
				errmsg("error during reading WAL absolute path : %s/%s", state->segcxt.ws_dir, fname));

	state->seg.ws_file = OpenTransientFile(fpath, PG_BINARY | O_RDONLY);
	if (state->seg.ws_file == -1)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open WAL segment \"%s\": %m", fpath)));
}

int 
WalReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
			XLogRecPtr targetPtr, char *readBuff)
{
	XLogReaderPrivate *private = state->private_data;
	int				  count = XLOG_BLCKSZ;
	WALReadError 	  errinfo;

	if (private->endptr != InvalidXLogRecPtr)
	{
		if (targetPagePtr + XLOG_BLCKSZ <= private->endptr)
			count = XLOG_BLCKSZ;
		else if (targetPagePtr + reqLen <= private->endptr)
			count = private->endptr - targetPagePtr;
		else
		{
			private->endptr_reached = true;
			return -1;
		}
	}

	if (!WALRead(state, readBuff, targetPagePtr, count, private->timeline,
				 &errinfo))
	{
		WALOpenSegment *seg = &errinfo.wre_seg;
		char		   fname[MAXPGPATH];

		XLogFileName(fname, seg->ws_tli, seg->ws_segno,
					 state->segcxt.ws_segsize);

		if (errinfo.wre_errno != 0)
		{
			errno = errinfo.wre_errno;
			ereport(ERROR, 
					errmsg("could not read from file %s, offset %d: %m",
					fname, errinfo.wre_off));
		}
		else
			ereport(ERROR,
					errmsg("could not read from file %s, offset %d: read %d of %d",
					fname, errinfo.wre_off, errinfo.wre_read,
					errinfo.wre_req));
	}

	return count;
}

/*
 * WALDIFFOpenSegment
 *
 * Opens new WAL segment
 */
void 
WALDIFFOpenSegment(WALDIFFSegmentContext *segcxt, WALDIFFSegment *seg)
{
	char fname[XLOG_FNAME_LEN];
	char fpath[MAXPGPATH];

	XLogFileName(fname, seg->tli, seg->segno, segcxt->segsize);

	if (snprintf(fpath, MAXPGPATH, "%s/%s", segcxt->dir, fname) == -1)
		ereport(ERROR,
				errmsg("error during reading WALDIFF absolute path: %s/%s", segcxt->dir, fname));
	
	seg->fd = OpenTransientFile(fpath, PG_BINARY | O_RDWR | O_CREAT);
	if (seg->fd == -1)
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("could not open WALDIFF segment \"%s\": %m", fpath)));
}


void 
WalCloseSegment(XLogReaderState *state)
{
	close(state->seg.ws_file);
	state->seg.ws_file = -1;
}

void 
WALDIFFCloseSegment(WALDIFFSegment *seg)
{
	Assert(seg->fd != -1);
	close(seg->fd);

	seg->fd = -1;
}

/* 
 * WALDIFFWriteRecord
 * 
 * Accumulate records in buffer, then writes them all at once.
 */
WALDIFFRecordWriteResult 
WALDIFFWriteRecord(WALDIFFWriterState *waldiff_writer,
				   XLogRecord *record)
{

	return WALDIFFWRITE_SUCCESS;
}

/*
 * fetch_insert
 * 
 */
WALDIFFRecord 
fetch_insert(XLogReaderState *record)
{
	WALDIFFRecord WDRec = (WALDIFFRecord) palloc0(SizeOfWALDIFFRecord);
	XLogRecordBlockHeader blk_hdr;
	char *block_data;
	Size block_data_len;
	RelFileLocator rel_file_locator;
	ForkNumber forknum;
	BlockNumber blknum;
	xl_heap_insert *main_data;

	/* Copy lsn */
	WDRec->lsn = record->record->lsn;

	/* Copy XLogRecord aka header */
	WDRec->rec_hdr = record->record->header;

	/* Copy some info */
	WDRec->t_xmin = XLogRecGetXid(record);
	WDRec->t_xmax = 0;
	WDRec->t_cid = 0;
	
	/* 
	 * Copy tuple's version pointers
	 * At this step, t_ctid always will be point to itself,
	 * because we reckon this record as first
	 */
	ItemPointerSetBlockNumber(&(WDRec->current_t_ctid), blknum);
    ItemPointerSetOffsetNumber(&(WDRec->current_t_ctid), main_data->offnum);
	WDRec->prev_t_ctid = WDRec->current_t_ctid;

	/* Copy main data */
	/* check do we need to allocate space for main data? 
	 * 'cause there is a huge ring buffer for all records(?) */
	main_data = (xl_heap_insert *) XLogRecGetData(record);
	WDRec->main_data = main_data;
	WDRec->main_data = SizeOfHeapInsert;

	/* Copy block data */
	XLogRecGetBlockTag(record, 0, &rel_file_locator, &forknum, &blknum);
	block_data = XLogRecGetBlockData(record, 0, &block_data_len);

	WDRec->max_block_id = 0;

	blk_hdr.id = 0;
	blk_hdr.fork_flags = record->record->blocks[0].flags;
	blk_hdr.data_length = block_data_len;

	WDRec->blocks[0].blk_hdr = blk_hdr;
	WDRec->blocks[0].file_loc = rel_file_locator;
	WDRec->blocks[0].forknum = forknum;
	WDRec->blocks[0].blknum = blknum;
	WDRec->blocks[0].has_data = true;
	Assert(XLogRecHasBlockData(record, 0));
	WDRec->blocks[0].block_data = block_data;
	WDRec->blocks[0].block_data_len = block_data_len;

	WDRec->t_hoff = SizeOfXLogRecord + SizeOfXLogRecordBlockHeader + \
					sizeof(RelFileLocator) + sizeof(BlockNumber) + SizeOfHeapHeader;

	return WDRec;
}

/*
 * fetch_update
 * 
 */
WALDIFFRecord 
fetch_update(XLogReaderState *record)
{
	WALDIFFRecord WDRec;
	xl_heap_update *main_data = (xl_heap_update *) XLogRecGetData(record);

	return WDRec;
}

/*
 * fetch_delete
 * 
 */
WALDIFFRecord 
fetch_delete(XLogReaderState *record)
{
	WALDIFFRecord WDRec = (WALDIFFRecord) palloc0(SizeOfWALDIFFRecord);
	XLogRecordBlockHeader blk_hdr;
	RelFileLocator rel_file_locator;
	ForkNumber forknum;
	BlockNumber blknum;
	xl_heap_delete *main_data;

	/* Copy lsn */
	WDRec->lsn = record->record->lsn;

	/* Copy XLogRecord aka header */
	WDRec->rec_hdr = record->record->header;

	/* Copy some info */
	WDRec->t_xmin = 0;
	WDRec->t_xmax = main_data->xmax;
	WDRec->t_cid = 0;
	
	/* 
	 * Copy tuple's version pointers
	 * At this step, t_ctid always will be point to itself,
	 * because we reckon this record as first
	 */
	ItemPointerSetBlockNumber(&(WDRec->current_t_ctid), blknum);
    ItemPointerSetOffsetNumber(&(WDRec->current_t_ctid), main_data->offnum);
	WDRec->prev_t_ctid = WDRec->current_t_ctid;

	/* Copy main data */
	/* check do we need to allocate space for main data? 
	 * 'cause there is a huge ring buffer for all records(?) */
	main_data = (xl_heap_insert *) XLogRecGetData(record);
	WDRec->main_data = main_data;
	WDRec->main_data = SizeOfHeapInsert;

	/* Copy block data */
	XLogRecGetBlockTag(record, 0, &rel_file_locator, &forknum, &blknum);

	WDRec->max_block_id = 0;

	blk_hdr.id = 0;
	blk_hdr.fork_flags = record->record->blocks[0].flags;
	blk_hdr.data_length = 0;

	WDRec->blocks[0].blk_hdr = blk_hdr;
	WDRec->blocks[0].file_loc = rel_file_locator;
	WDRec->blocks[0].forknum = forknum;
	WDRec->blocks[0].blknum = blknum;
	WDRec->blocks[0].has_data = false;
	Assert(!XLogRecHasBlockData(record, 0));

	main_data = (xl_heap_delete *) XLogRecGetData(record);

	return WDRec;
}

void 
overlay_update(WALDIFFRecord prev_tup, WALDIFFRecord curr_tup)
{
	
}

/*
 * getWALsegsize
 * 
 * Gets WAL segment size from the very first segment's header
 */
int 
getWALsegsize(const char *WALpath)
{
	int fd;
	int read_bytes;
	XLogLongPageHeader page_hdr;
	int wal_segment_size;

	fd = OpenTransientFile(WALpath, O_RDONLY | PG_BINARY);
	if (fd == -1)
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("could not open file \"%s\": %m", WALpath)));

	read_bytes = read(fd, (void *)(&page_hdr), sizeof(XLogLongPageHeader));
	if (read_bytes != sizeof(XLogLongPageHeader))
		ereport(ERROR,
				errmsg("could not read XLogLongPageHeader of WAL segment\"%s\": %m", 
					   WALpath));

	wal_segment_size = page_hdr->xlp_seg_size;
	Assert(IsValidWalSegSize(wal_segment_size));

	close(fd);
	return wal_segment_size;
}

/*
 * constructWALDIFFs
 * 
 * Creates WALDIFF records according to data in hash table 
 * and writes them into WALDIFF segment
 * 
 * [ I reckon We should remain RMGR records' structure ]
 */
void 
constructWALDIFFs(void)
{
	HTABElem *entry;
	HASH_SEQ_STATUS status;
	WALDIFFRecordWriteResult res;

	hash_seq_init(&status, hash_table);
	while ((entry = (HTABElem *) hash_seq_search(&status)) != NULL)
	{
		WALDIFFRecord WDRec = entry->data;
		static XLogRecord *record;
		if (record == NULL) 
			record = palloc0(XLogRecordMaxSize);

		if (WDRec->rec_hdr.xl_rmid == RM_HEAP_ID)
		{
			uint8 xlog_type = WDRec->rec_hdr.xl_info & XLOG_HEAP_OPMASK;

			/* The records have the same header */
			memcpy(record, &(WDRec->rec_hdr), SizeOfXLogRecord);
			record->xl_tot_len += SizeOfXLogRecord;

			switch(xlog_type)
			{
				/* 
				 * Contains XLogRecord + XLogRecordBlockHeader_0 +  
				 * RelFileLocator_0 + BlockNumber_0 + 
				 * block_data_0(xl_heap_header + tuple data) + main_data(xl_heap_insert)
				 */
				case XLOG_HEAP_INSERT:
				{
					WALDIFFBlock block_0;

					Assert(WDRec->max_block_id >= 0);
					block_0 = WDRec->blocks[0];

					/* XLogRecordBlockHeader */
					memcpy(record, &(block_0.blk_hdr), SizeOfXLogRecordBlockHeader);
					record->xl_tot_len += SizeOfXLogRecordBlockHeader;

					/* RelFileLocator */
					memcpy(record, &(block_0.file_loc), sizeof(RelFileLocator));
					record->xl_tot_len +=  sizeof(RelFileLocator);

					/* BlockNumber */
					memcpy(record, &(block_0.blknum), sizeof(BlockNumber));
					record->xl_tot_len += sizeof(BlockNumber);

					/* block data */
					memcpy(record, &(block_0.block_data), block_0.block_data_len);
					record->xl_tot_len += block_0.block_data_len;

					break;
				}
				
				/* 
				 * Contains XLogRecord + XLogRecordBlockHeader_0 + 
				 * RelFileLocator_0 + BlockNumber_0 + block_data_0 
				 * ((If XLH_UPDATE_PREFIX_FROM_OLD or XLH_UPDATE_SUFFIX_FROM_OLD flags are set)
 				 * prefix + suffix + xl_heap_header + tuple data) +
				 * XLogRecordBlockHeader_1 + BlockNumber_1 +
				 * main_data(xl_heap_update)
				 * 
				 * block 0: new page
				 * block 1: old page, if different. (no data, just a reference to the blk)
				 * [ As I got it, block 1 = header1 + blockNo1, and main_data contains offset to
				 * old tuple in this blockNo1 ]
				 */
				case XLOG_HEAP_UPDATE:
				{
					WALDIFFBlock block_0;
					WALDIFFBlock block_1;
					
					Assert(WDRec->max_block_id >= 1);
					block_0 = WDRec->blocks[0];
					block_1 = WDRec->blocks[1];

					/* XLogRecordBlockHeader 0 */
					memcpy(record, &(block_0.blk_hdr), SizeOfXLogRecordBlockHeader);
					record->xl_tot_len += SizeOfXLogRecordBlockHeader;

					/* RelFileLocator 0 */
					memcpy(record, &(block_0.file_loc), sizeof(RelFileLocator));
					record->xl_tot_len +=  sizeof(RelFileLocator);

					/* BlockNumber 0 */
					memcpy(record, &(block_0.blknum), sizeof(BlockNumber));
					record->xl_tot_len += sizeof(BlockNumber);

					/* block data 0 */
					memcpy(record, &(block_0.block_data), block_0.block_data_len);
					record->xl_tot_len += block_0.block_data_len;

					/* XLogRecordBlockHeader 1 */
					memcpy(record, &(block_1.blk_hdr), SizeOfXLogRecordBlockHeader);
					record->xl_tot_len += SizeOfXLogRecordBlockHeader;

					/* BlockNumber 1 */
					memcpy(record, &(block_1.blknum), sizeof(BlockNumber));
					record->xl_tot_len += sizeof(BlockNumber);

					break;
				}

				/* 
				 * Contains XLogRecord + XLogRecordBlockHeader_0 + 
				 * RelFileLocator_0 + BlockNumber_0 + main_data(xl_heap_delete)
				 */
				case XLOG_HEAP_DELETE:
				{
					WALDIFFBlock block_0;

					Assert(WDRec->max_block_id >= 0);
					block_0 = WDRec->blocks[0];

					/* XLogRecordBlockHeader */
					memcpy(record, &(block_0.blk_hdr), SizeOfXLogRecordBlockHeader);
					record->xl_tot_len += SizeOfXLogRecordBlockHeader;

					/* RelFileLocator */
					memcpy(record, &(block_0.file_loc), sizeof(RelFileLocator));
					record->xl_tot_len +=  sizeof(RelFileLocator);

					/* BlockNumber */
					memcpy(record, &(block_0.blknum), sizeof(BlockNumber));
					record->xl_tot_len += sizeof(BlockNumber);

					break;
				}

				default:
					ereport(ERROR, errmsg("unproccessed XLOG_HEAP type"));
			}

			/* main data */
			memcpy(record, WDRec->main_data, WDRec->main_data_len);
			record->xl_tot_len += WDRec->main_data_len;

			/* calculate and insert record's crc */
			{
				pg_crc32c crc;

				INIT_CRC32C(crc);
				COMP_CRC32C(crc, ((char *) record) + SizeOfXLogRecord, record->xl_tot_len - SizeOfXLogRecord);
				COMP_CRC32C(crc, (char *) record, offsetof(XLogRecord, xl_crc));
				FIN_CRC32C(crc);

				memcpy(((char *) record) + SizeOfXLogRecord - sizeof(pg_crc32c), &crc, sizeof(pg_crc32c));
			}
		}
		else 
			ereport(ERROR, errmsg("WALDIFF cannot contains not XLOG_HEAP types"));

		res = WALDIFFWriteRecord(writer_state, record);
		if (res == WALDIFFWRITE_FAIL) 
			ereport(ERROR, errmsg("error during writing WALDIFF records in constructWALDIFFs"));
	}
}

void 
finishWALDIFFSegment(void)
{

}