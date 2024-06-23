#include "waldiff.h"
#include "waldiff_writer.h"
#include "waldiff_test.h"

PG_MODULE_MAGIC;

/* GUC value's store */
static char *waldiff_dir;

/* Global data */
static MemoryContextStorage *memory_context_storage;
static WALDIFFWriterState   *writer;
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
static void waldiff_startup(ArchiveModuleState *reader);
static bool waldiff_configured(ArchiveModuleState *reader);
static bool waldiff_archive(ArchiveModuleState *reader, const char *file, const char *path);
static void waldiff_shutdown(ArchiveModuleState *reader);

/* Custom defined callbacks for WALDIFFReader and WALDIFFWriter */
void WalOpenSegment(XLogReaderState *reader,
					XLogSegNo nextSegNo,
					TimeLineID *tli_p);
void WALDIFFOpenSegment(WALDIFFSegmentContext *segcxt, WALDIFFSegment *seg);
int WalReadPage(XLogReaderState *reader, XLogRecPtr targetPagePtr, int reqLen,
				XLogRecPtr targetPtr, char *readBuff);
void WalCloseSegment(XLogReaderState *reader);				
void WALDIFFCloseSegment(WALDIFFSegment *seg);
WALDIFFRecordWriteResult WALDIFFWriteRecord(WALDIFFWriterState *writer,
											XLogRecord *record);

/* This three fuctions returns palloced struct */
static WALDIFFRecord fetch_insert(XLogReaderState *record);
static WALDIFFRecord fetch_update(XLogReaderState *record);
static WALDIFFRecord fetch_delete(XLogReaderState *record);

static void free_waldiff_record(WALDIFFRecord record);
static void copy_waldiff_record(WALDIFFRecord dest, WALDIFFRecord src);

static int overlay_update(WALDIFFRecord prev_tup, WALDIFFRecord curr_tup);

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
waldiff_startup(ArchiveModuleState *reader)
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
waldiff_configured(ArchiveModuleState *reader)
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
waldiff_archive(ArchiveModuleState *reader, const char *WALfile, const char *WALpath)
{
	/* Preparations */
	static int wal_segment_size = 0;
	XLogReaderPrivate reader_private = {0};
	XLogSegNo segno;

	ereport(LOG, errmsg("WAL segment is being archived"));

	if (wal_segment_size == 0)
		wal_segment_size = getWALsegsize(WALpath);

	Assert(IsValidWalSegSize(wal_segment_size));

	ereport(LOG, errmsg("WAL segment size is: %d", wal_segment_size));

	/* Determines tli, segno and startPtr values of archived WAL segment
	 * and future WALDIFF segment
	 */
	reader_private.timeline = 1;
	reader_private.startptr = InvalidXLogRecPtr;
	reader_private.endptr = InvalidXLogRecPtr;
	reader_private.endptr_reached = false;

	XLogFromFileName(WALfile, &reader_private.timeline, &segno, wal_segment_size);
	/* ? */
	XLogSegNoOffsetToRecPtr(segno, 0, wal_segment_size, reader_private.startptr);
	XLogSegNoOffsetToRecPtr(segno + 1, 0, wal_segment_size, reader_private.endptr);
	/* ? */
	Assert(!XLogRecPtrIsInvalid(reader_private.startptr));
	Assert(!XLogRecPtrIsInvalid(reader_private.endptr));

	ereport(LOG, errmsg("segNo: %lu; tli: %u; startptr: %lu; endptr: %lu",
						segno,reader_private.timeline, reader_private.startptr,
						reader_private.endptr));


	if (reader_state == NULL)
		reader_state = XLogReaderAllocate(wal_segment_size, XLOGDIR,
										  XL_ROUTINE(.page_read = WalReadPage,
													 .segment_open = WalOpenSegment,
													 .segment_close = WalCloseSegment),
										  &reader_private);
	Assert(reader_state != NULL);

	ereport(LOG, errmsg("XLogReader was allocated successfully"));

    if (writer == NULL)
		writer = WALDIFFWriterAllocate(wal_segment_size, waldiff_dir, 
											 WALDIFFWRITER_ROUTINE(.write_records = WALDIFFWriteRecord,
									  							   .segment_open = WALDIFFOpenSegment,
									  							   .segment_close = WALDIFFCloseSegment));
	Assert(writer != NULL);

	ereport(LOG, errmsg("WALDIFFWriter was allocated successfully"));

	WALDIFFBeginWrite(writer, reader_private.startptr, segno, reader_private.timeline);

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
		WALDIFFRecord WDrec;
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
			int overlay_result;
			uint8 xlog_type = XLogRecGetInfo(reader_state) & XLOG_HEAP_OPMASK;

			entry->data = NULL;

			switch(xlog_type)
			{
				case XLOG_HEAP_INSERT:
					WDrec = fetch_insert(reader_state);
					if (WDrec == NULL)
						ereport(ERROR, errmsg("fetch_insert failed"));

					hash_key = GetHashKeyOfWALDIFFRecord(WDrec);
					hash_search(hash_table, (void*) &hash_key, HASH_FIND, &is_found);
					if (is_found)
					{
						entry = (HTABElem *) hash_search(hash_table, (void*) &hash_key, HASH_REMOVE, NULL);
						free_waldiff_record(entry->data);
					}

					entry = (HTABElem *) hash_search(hash_table, (void*) &hash_key, HASH_ENTER, NULL);
					copy_waldiff_record(entry->data, WDrec);
					free_waldiff_record(WDrec);
					Assert(entry->key == hash_key);
					
					break;

				case XLOG_HEAP_UPDATE:
					WDrec = fetch_update(reader_state);
					if (WDrec == NULL)
						ereport(ERROR, errmsg("fetch_update failed"));

					prev_hash_key = GetHashKeyOfPrevWALDIFFRecord(WDrec);
					hash_key = GetHashKeyOfWALDIFFRecord(WDrec);

					entry = (HTABElem *) hash_search(hash_table, (void*) &prev_hash_key, HASH_FIND, &is_found);
					if (is_found)
					{
						uint8 prev_xlog_type = entry->data->rec_hdr.xl_info & XLOG_HEAP_OPMASK;
						overlay_result = overlay_update(entry->data, WDrec);

						if (overlay_result == -1)
						{
							/*
							 * Overlaying failed - we must store both records
							 */
							entry = (HTABElem *) hash_search(hash_table, (void*) &hash_key, HASH_ENTER, NULL);
							copy_waldiff_record(entry->data, WDrec);
							free_waldiff_record(WDrec);
							Assert(entry->key == hash_key);
						}
						else
						{
							WALDIFFRecord tmp = entry->data;
							entry = (HTABElem *) hash_search(hash_table, (void*) &prev_hash_key, HASH_REMOVE, NULL);
							entry = (HTABElem *) hash_search(hash_table, (void*) &hash_key, HASH_ENTER, NULL);

							/*
							 * We can do this without copying, because tmp dont refer to local variable
							 */
							entry->data = tmp;
							Assert(entry->key == hash_key);
						
							free_waldiff_record(WDrec);
						}
					}
					else 
					{
						entry = (HTABElem *) hash_search(hash_table, (void*) &hash_key, HASH_ENTER, NULL);
						copy_waldiff_record(entry->data, WDrec);
						free_waldiff_record(WDrec);
						Assert(entry->key == hash_key);
					}

					break;

				case XLOG_HEAP_DELETE:
					WDrec = fetch_delete(reader_state);
					if (WDrec == NULL)
						ereport(ERROR, errmsg("fetch_delete failed"));

					prev_hash_key = GetHashKeyOfPrevWALDIFFRecord(WDrec);
					hash_key = GetHashKeyOfWALDIFFRecord(WDrec);

					entry = hash_search(hash_table, (void*) &prev_hash_key, HASH_FIND, &is_found);
					if (is_found)
					{
						/* if prev WDrec is not presented in the HTAB then it's not in WALDIFF segment */
						entry = (HTABElem *) hash_search(hash_table, (void*) &prev_hash_key, HASH_REMOVE, NULL);
						free_waldiff_record(entry->data);
						Assert(entry == NULL);
					}

					/* insert/update (the prev record) is in another WAL segment */
					else 
					{
						entry = (HTABElem *) hash_search(hash_table, (void*) &hash_key, HASH_ENTER, NULL);
						copy_waldiff_record(entry->data, WDrec);
						free_waldiff_record(WDrec);
						Assert(entry->key == hash_key);
					}

					break;

				/* unprocessed record type */
				default:
					WALDIFFRecordWriteResult res = WALDIFFWriteRecord(writer, WALRec);
					if (res == WALDIFFWRITE_FAIL) 
						ereport(ERROR, errmsg("error during writing WALDIFF records in waldiff_archive: %s", 
											  WALDIFFWriterGetErrMsg(writer)));
					break;
			}
		} 
		
		else 
		{
			WALDIFFRecordWriteResult res = WALDIFFWriteRecord(writer, WALRec);
			if (res == WALDIFFWRITE_FAIL) 
				ereport(ERROR, errmsg("error during writing WALDIFF records in waldiff_archive %s", 
									  WALDIFFWriterGetErrMsg(writer)));
		}
	}

	Assert(((writer->seg.segno + 1) * writer->segcxt.segsize) - 
			writer->segoff > 0);

	ereport(LOG, errmsg("constructing WALDIFFs"));

	/* Constructing and writing WALDIFFs to WALDIFF segment */
	constructWALDIFFs();

	ereport(LOG, errmsg("finishing WALDIFF segment"));
	
	/* Questionable */
	/* End the segment with SWITCH record */
	finishWALDIFFSegment();

	if (writer->writeBufSize > 0)
		WALDIFFFlushBuffer(writer);

	ereport(LOG, errmsg("archived WAL file: %s", WALpath));

	return true;
}

/*
 * walldiff_shutdown
 *
 * Frees all allocated reaources.
 */
void 
waldiff_shutdown(ArchiveModuleState *reader)
{
	CloseTransientFile(writer->seg.fd);

	/* Must have 'case this function closes fd */
	XLogReaderFree(reader_state);
	WALDIFFWriterFree(writer);

	hash_destroy(hash_table);

	MemoryContextSwitchTo(memory_context_storage->old);
	Assert(CurrentMemoryContext != memory_context_storage->current);
	MemoryContextDelete(memory_context_storage->current);
}

void 
WalOpenSegment(XLogReaderState *reader,
			   XLogSegNo nextSegNo,
			   TimeLineID *tli_p)
{
	TimeLineID tli = *tli_p;
    char fname[XLOG_FNAME_LEN];
	char fpath[MAXPGPATH];

    XLogFileName(fname, tli, nextSegNo, reader->segcxt.ws_segsize);

	if (snprintf(fpath, MAXPGPATH, "%s/%s", reader->segcxt.ws_dir, fname) == -1)
		ereport(ERROR,
				errmsg("error during reading WAL absolute path : %s/%s", reader->segcxt.ws_dir, fname));

	reader->seg.ws_file = OpenTransientFile(fpath, PG_BINARY | O_RDONLY);
	if (reader->seg.ws_file == -1)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open WAL segment \"%s\": %m", fpath)));
}

int 
WalReadPage(XLogReaderState *reader, XLogRecPtr targetPagePtr, int reqLen,
			XLogRecPtr targetPtr, char *readBuff)
{
	XLogReaderPrivate *private = reader->private_data;
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

	if (!WALRead(reader, readBuff, targetPagePtr, count, private->timeline,
				 &errinfo))
	{
		WALOpenSegment *seg = &errinfo.wre_seg;
		char		   fname[MAXPGPATH];

		XLogFileName(fname, seg->ws_tli, seg->ws_segno,
					 reader->segcxt.ws_segsize);

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
WalCloseSegment(XLogReaderState *reader)
{
	Assert(reader->seg.ws_file != -1);
	CloseTransientFile(reader->seg.ws_file);
	reader->seg.ws_file = -1;
}

void 
WALDIFFCloseSegment(WALDIFFSegment *seg)
{
	Assert(seg->fd != -1);
	CloseTransientFile(seg->fd);
	seg->fd = -1;
}

/* 
 * WALDIFFWriteRecord
 * 
 * Accumulate records in WALDIFFWriter's buffer, then writes them all at once.
 */
WALDIFFRecordWriteResult 
WALDIFFWriteRecord(WALDIFFWriterState *waldiff_writer,
				   XLogRecord *record)
{
	/* 
	 * Every segments starts with page header.
	 * If it is the very first WALDIFF segment then page header has additional fields.
	 */
	XLogRecPtr startptr = InvalidXLogRecPtr;
	XLogSegNo first_segno = 1;
	XLogSegNoOffsetToRecPtr(first_segno, 0, writer->segcxt.segsize, startptr);

	Assert(!XLogRecPtrIsInvalid(startptr));

	if (writer->EndRecPtr == startptr && writer->seg.segno == first_segno &&
		WALDIFFWriterGetBufSize(writer) == 0)
	{
		XLogLongPageHeaderData long_page_hdr = {0};
		XLogPageHeaderData page_hdr = {0};

		ereport(LOG, errmsg("Putting long page header of the very first WALDIFF segment"));

		page_hdr.xlp_magic = XLOG_PAGE_MAGIC;		
		page_hdr.xlp_info |= XLP_LONG_HEADER;
		page_hdr.xlp_tli = writer->seg.tli;		
		page_hdr.xlp_pageaddr = writer->EndRecPtr;	
		page_hdr.xlp_rem_len = 0;

		long_page_hdr.std = page_hdr;
		long_page_hdr.xlp_sysid = writer->system_identifier;
		long_page_hdr.xlp_seg_size = writer->segcxt.segsize;
		long_page_hdr.xlp_xlog_blcksz = XLOG_BLCKSZ;

		/* Write to buffer */
		memcpy(WALDIFFWriterGetBuf(writer), &long_page_hdr, SizeOfXLogLongPHD);
		WALDIFFWriterGetBufSize(writer) += SizeOfXLogLongPHD;
	}
	else if (writer->EndRecPtr % writer->segcxt.segsize == 0 &&
			 WALDIFFWriterGetBufSize(writer) == 0)
	{
		XLogPageHeaderData page_hdr = {0};

		ereport(LOG, errmsg("Putting page header of a new WALDIFF segment"));
		
		page_hdr.xlp_magic = XLOG_PAGE_MAGIC;		
		page_hdr.xlp_info = 0;
		page_hdr.xlp_tli = writer->seg.tli;		
		page_hdr.xlp_pageaddr = writer->EndRecPtr;	
		page_hdr.xlp_rem_len = 0;

		/* Write to buffer */
		memcpy(WALDIFFWriterGetBuf(writer), &page_hdr, SizeOfXLogShortPHD);
		WALDIFFWriterGetBufSize(writer) += SizeOfXLogShortPHD;
	}

	/* Flush the buffer if there is no space for the next record */
	if (record->xl_tot_len > WALDIFFWriterGetRestOfBufSize(writer))
	{
		if (WALDIFFFlushBuffer(waldiff_writer) == WALDIFFWRITE_FAIL)
			return WALDIFFWRITE_FAIL;
	}

	memcpy(WALDIFFWriterGetBuf(writer) + WALDIFFWriterGetBufSize(writer), 
		   record, record->xl_tot_len);
	WALDIFFWriterGetBufSize(writer) += record->xl_tot_len;

	/* flush the full buffer, so we don't need to do it in the next time */
	if (record->xl_tot_len == WALDIFFWriterGetRestOfBufSize(writer))
		return WALDIFFFlushBuffer(waldiff_writer);

	return WALDIFFWRITE_SUCCESS;
}

/*
 * fetch_insert
 * 
 */
WALDIFFRecord 
fetch_insert(XLogReaderState *record)
{
	/* HEAP_INSERT contains one block */
	WALDIFFRecord WDrec = (WALDIFFRecord) palloc0(SizeOfWALDIFFRecord + sizeof(WALDIFFBlock));
	XLogRecordBlockHeader blk_hdr;
	char *block_data;
	Size block_data_len;
	RelFileLocator rel_file_locator;
	ForkNumber forknum;
	BlockNumber blknum;
	xl_heap_insert *main_data = (xl_heap_insert *) XLogRecGetData(record);

	WDrec->type = XLOG_HEAP_INSERT;

	/* Copy lsn */
	WDrec->lsn = record->record->lsn;

	/* Copy XLogRecord aka header */
	WDrec->rec_hdr = record->record->header;

	/* Copy some info */
	WDrec->t_xmin = XLogRecGetXid(record);
	WDrec->t_xmax = 0;
	WDrec->t_cid = FirstCommandId;

	XLogRecGetBlockTag(record, 0, &rel_file_locator, &forknum, &blknum);
	
	/* 
	 * Copy tuple's version pointers
	 * At this step, t_ctid always will be point to itself,
	 * because we reckon this record as first
	 */
	ItemPointerSetBlockNumber(&(WDrec->current_t_ctid), blknum);
    ItemPointerSetOffsetNumber(&(WDrec->current_t_ctid), main_data->offnum);
	WDrec->prev_t_ctid = WDrec->current_t_ctid;

	/* Copy main data */
	/* check do we need to allocate space for main data? 
	 * 'cause there is a huge ring buffer for all records(?) */
	WDrec->main_data = (char*) palloc0(SizeOfHeapInsert);
	memcpy(WDrec->main_data, main_data, SizeOfHeapInsert);
	WDrec->main_data_len = SizeOfHeapInsert;

	/* Copy block data */
	block_data = XLogRecGetBlockData(record, 0, &block_data_len);

	WDrec->max_block_id = 0;

	blk_hdr.id = 0;
	blk_hdr.fork_flags = record->record->blocks[0].flags;
	blk_hdr.data_length = block_data_len;

	WDrec->blocks[0].blk_hdr = blk_hdr;
	WDrec->blocks[0].file_loc = rel_file_locator;
	WDrec->blocks[0].forknum = forknum;
	WDrec->blocks[0].blknum = blknum;
	WDrec->blocks[0].has_data = true;

	Assert(XLogRecHasBlockData(record, 0));
	WDrec->blocks[0].block_data = (char*) palloc0(block_data_len);
	memcpy(WDrec->blocks[0].block_data, block_data, block_data_len);
	WDrec->blocks[0].block_data_len = block_data_len;

	return WDrec;
}

/*
 * fetch_update
 * 
 * Backup blk 0: new page
 *
 * If XLH_UPDATE_PREFIX_FROM_OLD or XLH_UPDATE_SUFFIX_FROM_OLD flags are set,
 * the prefix and/or suffix come first, as one or two uint16s.
 *
 * After that, xl_heap_header and new tuple data follow.  The new tuple
 * data doesn't include the prefix and suffix, which are copied from the
 * old tuple on replay.
 *
 * If XLH_UPDATE_CONTAINS_NEW_TUPLE flag is given, the tuple data is
 * included even if a full-page image was taken.
 *
 * Backup blk 1: old page, if different. (no data, just a reference to the blk)
 */
WALDIFFRecord 
fetch_update(XLogReaderState *record)
{
	/* HEAP_UPDATE contains two blocks */
	WALDIFFRecord WDrec = (WALDIFFRecord) palloc0(SizeOfWALDIFFRecord + 2 * sizeof(WALDIFFBlock));
	XLogRecordBlockHeader blk_hdr;
	char *block_data;
	Size block_data_len;
	RelFileLocator rel_file_locator;
	ForkNumber forknum;
	BlockNumber new_blknum,
				old_blknum;
	xl_heap_update *main_data = (xl_heap_update *) XLogRecGetData(record);

	WDrec->type = XLOG_HEAP_UPDATE;

	/* Copy lsn */
	WDrec->lsn = record->record->lsn;

	/* Copy XLogRecord aka header */
	WDrec->rec_hdr = record->record->header;

	/* Copy some info */
	WDrec->t_xmin = XLogRecGetXid(record);
	WDrec->t_xmax = main_data->new_xmax;
	WDrec->t_cid = FirstCommandId;

	XLogRecGetBlockTag(record, 0, &rel_file_locator, &forknum, &new_blknum);
	if (!XLogRecGetBlockTagExtended(record, 1, NULL, NULL, &old_blknum, NULL))
		old_blknum = new_blknum;

	/* 
	 * Copy tuple's version pointers
	 */
	ItemPointerSetBlockNumber(&(WDrec->current_t_ctid), new_blknum);
    ItemPointerSetOffsetNumber(&(WDrec->current_t_ctid), main_data->new_offnum);
	ItemPointerSetBlockNumber(&(WDrec->prev_t_ctid), old_blknum);
    ItemPointerSetOffsetNumber(&(WDrec->prev_t_ctid), main_data->old_offnum);

	/* Copy main data */
	/* check do we need to allocate space for main data? 
	 * 'cause there is a huge ring buffer for all records(?) */
	WDrec->main_data = (char*) palloc0(SizeOfHeapUpdate);
	memcpy(WDrec->main_data, main_data, SizeOfHeapUpdate);
	WDrec->main_data_len = SizeOfHeapUpdate;

	/* Copy block data */
	block_data = XLogRecGetBlockData(record, 0, &block_data_len);

	WDrec->max_block_id = 1;

	/* Block 0 */
	blk_hdr.id = 0;
	blk_hdr.fork_flags = record->record->blocks[0].flags;
	blk_hdr.data_length = block_data_len;

	WDrec->blocks[0].blk_hdr = blk_hdr;
	WDrec->blocks[0].file_loc = rel_file_locator;
	WDrec->blocks[0].forknum = forknum;
	WDrec->blocks[0].blknum = new_blknum;
	WDrec->blocks[0].has_data = true;

	Assert(XLogRecHasBlockData(record, 0));
	WDrec->blocks[0].block_data = (char*) palloc0(block_data_len);
	memcpy(WDrec->blocks[0].block_data, block_data, block_data_len);
	WDrec->blocks[0].block_data_len = block_data_len;

	/* Block 1 */
	memset((void*)&blk_hdr, 0, SizeOfXLogRecordBlockHeader);
	blk_hdr.id = 1;
	blk_hdr.fork_flags = record->record->blocks[1].flags;
	blk_hdr.data_length = 0;

	WDrec->blocks[1].blk_hdr = blk_hdr;
	WDrec->blocks[1].blknum = old_blknum;
	WDrec->blocks[1].has_data = false;
	Assert(!XLogRecHasBlockData(record, 1));

	if (main_data->flags & XLH_UPDATE_PREFIX_FROM_OLD || main_data->flags & XLH_UPDATE_SUFFIX_FROM_OLD)
		Assert(new_blknum == old_blknum);

	return WDrec;
}

/*
 * fetch_delete
 * 
 */
WALDIFFRecord 
fetch_delete(XLogReaderState *record)
{
	/* HEAP_DELETE contains one block */
	WALDIFFRecord WDrec = (WALDIFFRecord) palloc0(SizeOfWALDIFFRecord + sizeof(WALDIFFBlock));
	XLogRecordBlockHeader blk_hdr;
	RelFileLocator rel_file_locator;
	ForkNumber forknum;
	BlockNumber blknum;
	xl_heap_delete *main_data = (xl_heap_delete *) XLogRecGetData(record);

	WDrec->type = XLOG_HEAP_DELETE;

	/* Copy lsn */
	WDrec->lsn = record->record->lsn;

	/* Copy XLogRecord aka header */
	WDrec->rec_hdr = record->record->header;

	/* Copy some info */
	WDrec->t_xmin = 0;
	WDrec->t_xmax = main_data->xmax;
	WDrec->t_cid = FirstCommandId;

	XLogRecGetBlockTag(record, 0, &rel_file_locator, &forknum, &blknum);
	
	/* 
	 * Copy tuple's version pointers
	 * At this step, t_ctid always will be point to itself,
	 * because we reckon this record as first
	 */
	ItemPointerSetBlockNumber(&(WDrec->current_t_ctid), blknum);
    ItemPointerSetOffsetNumber(&(WDrec->current_t_ctid), main_data->offnum);
	WDrec->prev_t_ctid = WDrec->current_t_ctid;

	/* Copy main data */
	/* check do we need to allocate space for main data? 
	 * 'cause there is a huge ring buffer for all records(?) */
	WDrec->main_data = (char*) palloc0(SizeOfHeapDelete);
	memcpy(WDrec->main_data, main_data, SizeOfHeapDelete);
	WDrec->main_data_len = SizeOfHeapDelete;

	/* Copy block data */
	WDrec->max_block_id = 0;

	blk_hdr.id = 0;
	blk_hdr.fork_flags = record->record->blocks[0].flags;
	blk_hdr.data_length = 0;

	WDrec->blocks[0].blk_hdr = blk_hdr;
	WDrec->blocks[0].file_loc = rel_file_locator;
	WDrec->blocks[0].forknum = forknum;
	WDrec->blocks[0].blknum = blknum;
	WDrec->blocks[0].has_data = false;
	Assert(!XLogRecHasBlockData(record, 0));

	return WDrec;
}

static void free_waldiff_record(WALDIFFRecord record)
{
	if (record->type == XLOG_HEAP_INSERT || record->type == XLOG_HEAP_UPDATE)
		pfree(record->blocks[0].block_data);

	pfree(record->main_data);
	pfree(record);
}

/*
 * After using this function you can call free_waldiff_record for src,
 * because this function copies all arrays via memcpy
 * dest should be NULL pointer
 */
static void copy_waldiff_record(WALDIFFRecord dest, WALDIFFRecord src)
{
	int num_blocks = 0;

	Assert(dest == NULL);
	Assert(src != NULL);

	if (src->type == XLOG_HEAP_INSERT || src->type == XLOG_HEAP_DELETE)
		num_blocks = 1;
	else if (src->type == XLOG_HEAP_UPDATE)
		num_blocks = 2;

	Assert(num_blocks != 0);

	dest = (WALDIFFRecord) palloc0(SizeOfWALDIFFRecord + (sizeof(WALDIFFBlock) * num_blocks));
	memcpy(dest, src, SizeOfWALDIFFRecord);

	dest->main_data = (char*) palloc0(sizeof(char) * src->main_data_len);
	memcpy(dest->main_data, src->main_data, src->main_data_len);

	for (int i = 0; i < num_blocks; i++)
	{
		memcpy((void*)&dest->blocks[i], (void*)&src->blocks[i], sizeof(WALDIFFBlock));
		
		if (src->type == XLOG_HEAP_DELETE || 
			src->type == XLOG_HEAP_UPDATE && i > 0)
			continue;

		dest->blocks[i].block_data = (char*) palloc0(sizeof(char) * src->blocks[i].block_data_len);
		memcpy(dest->blocks[i].block_data, src->blocks[i].block_data, src->blocks[i].block_data_len);
	}
}

/*
 * Possible cases :
 * 1) prev_tup is insert record, so we write data from curr_tup (update record) on top of prev_tup data.
 * 2) prev tup is update record, so we form update record, that contain both changes. 
 * 	  2.1) If some updated fields (from insert record) matches in both records, we put curr_tup data on top of prev_tup data. 
 * 	  2.2) If one record contains a field that is not in another record, this field will be saved unchanged.
 * 	  2.3) If records has some fields "between" each other, we cannot overlay them, because those fields will be lost.
 *		   Function returns -1 in this case
 * 
 * After this function, curr_tup can be deallocated (if return value is 0)
 */
int
overlay_update(WALDIFFRecord prev_tup, WALDIFFRecord curr_tup)
{
	uint16 			curr_tup_prefix_len  	  = 0;
	uint16 			curr_tup_suffix_len 	  = 0;
	Size 			curr_tup_bitmap_len 	  = 0;

	/*
	 * 'user data' is just content of tuple that user is working with
	 */
	Size 			curr_tup_offset2user_data = 0;
	Size 			curr_tup_user_data_len 	  = 0;

	xl_heap_header* curr_tup_xl_hdr;
	xl_heap_update* curr_tup_update_hdr 	  = (xl_heap_update*) curr_tup->main_data;
	char* 			curr_tup_block_data 	  = (char*) curr_tup->blocks[0].block_data;

	Size 			prev_tup_bitmap_len 	  = 0;
	char* 			prev_tup_block_data 	  = (char*) prev_tup->blocks[0].block_data;
	xl_heap_header* prev_tup_xl_hdr;

	Assert(curr_tup->type == XLOG_HEAP_UPDATE);
	Assert(prev_tup->type == XLOG_HEAP_UPDATE || prev_tup->type == XLOG_HEAP_INSERT);

	/*
	 * block 0 data of update tuple looks like this : 
	 * [prefix len] + [suffix len] + xl_heap_header + bitmap + padding + user data
	 */
	if (curr_tup_update_hdr->flags & XLH_UPDATE_PREFIX_FROM_OLD)
	{
		memcpy((void*)&curr_tup_prefix_len, curr_tup_block_data, sizeof(uint16));
		curr_tup_block_data += sizeof(uint16);
		curr_tup_offset2user_data += sizeof(uint16);
	}
	if (curr_tup_update_hdr->flags & XLH_UPDATE_SUFFIX_FROM_OLD)
	{
		memcpy((void*)&curr_tup_suffix_len, curr_tup_block_data, sizeof(uint16));
		curr_tup_block_data += sizeof(uint16);
		curr_tup_offset2user_data += sizeof(uint16);
	}

	curr_tup_xl_hdr = (xl_heap_header*) curr_tup_block_data;
	/*
	 * t_hoff filed is just a copy of t_hoff from HeapTupleHeaderData, so
	 * we must calculate bitmap length using SizeofHeapTupleHeader
	 */
	curr_tup_bitmap_len = curr_tup_xl_hdr->t_hoff - SizeofHeapTupleHeader;

	/*
	 * Now curr_tup_block_data will be point to user_data of update record
	 */
	curr_tup_block_data += (SizeOfHeapHeader + curr_tup_bitmap_len);

	curr_tup_offset2user_data += (SizeOfHeapHeader + curr_tup_bitmap_len);
	curr_tup_user_data_len = curr_tup->blocks[0].block_data_len - curr_tup_offset2user_data;
	
	if (prev_tup->type == XLOG_HEAP_INSERT)
	{
		
		prev_tup_xl_hdr = (xl_heap_header*) prev_tup_block_data;

		/*
		 * Same logic as in curr_tup_bitmap_len calculation
		 */
		prev_tup_bitmap_len = prev_tup_xl_hdr->t_hoff - SizeofHeapTupleHeader;

		/*
		 * Now prev_tup_block_data will be point to user_data of insert record
		 */
		prev_tup_block_data += (SizeOfHeapHeader + prev_tup_bitmap_len);

		memcpy(prev_tup_block_data + curr_tup_prefix_len, curr_tup_block_data, curr_tup_user_data_len);
	}
	else if (prev_tup->type == XLOG_HEAP_UPDATE)
	{
		uint16 			prev_tup_prefix_len  	  = 0;
		uint16 			prev_tup_suffix_len 	  = 0;
		Size 			prev_tup_offset2user_data = 0;
		Size 			prev_tup_user_data_len 	  = 0;
		xl_heap_update* prev_tup_update_hdr 	  = (xl_heap_update*) prev_tup->main_data;

		/*
		 * Same logic as in curr_tup prefix, suffix and user_data_len calculation
		 */
		if (prev_tup_update_hdr->flags & XLH_UPDATE_PREFIX_FROM_OLD)
		{
			memcpy((void*)&prev_tup_prefix_len, prev_tup_block_data, sizeof(uint16));
			prev_tup_block_data += sizeof(uint16);
			prev_tup_offset2user_data += sizeof(uint16);
		}
		if (prev_tup_update_hdr->flags & XLH_UPDATE_SUFFIX_FROM_OLD)
		{
			memcpy((void*)&prev_tup_suffix_len, prev_tup_block_data, sizeof(uint16));
			prev_tup_block_data += sizeof(uint16);
			prev_tup_offset2user_data += sizeof(uint16);
		}

		prev_tup_xl_hdr = (xl_heap_header*) prev_tup_block_data;
		prev_tup_bitmap_len = prev_tup_xl_hdr->t_hoff - SizeofHeapTupleHeader;

		/*
		* Now curr_tup_block_data will be point to user_data of update record
		*/
		prev_tup_block_data += (SizeOfHeapHeader + prev_tup_bitmap_len);

		prev_tup_offset2user_data += (SizeOfHeapHeader + curr_tup_bitmap_len);
		prev_tup_user_data_len = prev_tup->blocks[0].block_data_len - prev_tup_offset2user_data;

		/*
		 * There may be a situation, when updated parts of tuples are not intersects.
		 * For example, original tuple consists of 6 fields : [1, 2, 3, 4, 5, 6], 
		 * prev_tup updates 2 and 3 field, curr_tup updates 5 and 6 field.
		 * In this case we cannot overlay updates on each other, because we will lost 4 field in future
		 */

		if (curr_tup_prefix_len > (prev_tup_prefix_len + prev_tup_user_data_len) ||
			curr_tup_suffix_len > (prev_tup_suffix_len + prev_tup_user_data_len))
			return -1;

		/*
		 * OK, now we are shure, that we can overlay updates. Now we must
		 * consider all possible "layouts" of our updates
		 */

		if (prev_tup_suffix_len < curr_tup_suffix_len &&
			prev_tup_prefix_len < curr_tup_prefix_len)
		{
			/*
			 * Second update completely "lies in" first update :
			 * [#####] - first update
			 *   [##] - second update
			 */	

			Size left_offset = (curr_tup_prefix_len - prev_tup_prefix_len);
			memcpy((void*) (prev_tup_block_data + left_offset), curr_tup_block_data, curr_tup_user_data_len);
		}
		else if (prev_tup_prefix_len == (curr_tup_prefix_len + curr_tup_user_data_len))
		{
			/*
			 * Second update "touch" first update on the left edge :
			 * first update - [#####][##] - second update
			 */

			char prev_tup_user_data_copy[prev_tup_user_data_len];
			memcpy(prev_tup_user_data_copy, prev_tup_block_data, prev_tup_user_data_len);

			prev_tup->blocks[0].block_data_len += curr_tup_user_data_len;
			prev_tup->blocks[0].block_data = (char*) repalloc(prev_tup->blocks[0].block_data, prev_tup->blocks[0].block_data_len);
			prev_tup_block_data = prev_tup->blocks[0].block_data;

			memcpy(prev_tup_block_data, curr_tup_block_data, curr_tup_user_data_len);
			memcpy((void*) (prev_tup_block_data + curr_tup_user_data_len), prev_tup_user_data_copy, prev_tup_user_data_len);
		}
		else if (prev_tup_suffix_len == (curr_tup_suffix_len + curr_tup_user_data_len))
		{
			/*
			 * Second update "touch" first update on the right edge :
			 * second update - [##][#####] - first update
			 */

			prev_tup->blocks[0].block_data_len += curr_tup_user_data_len;
			prev_tup->blocks[0].block_data = (char*) repalloc(prev_tup->blocks[0].block_data, prev_tup->blocks[0].block_data_len);
			prev_tup_block_data = prev_tup->blocks[0].block_data;

			memcpy((void*) (prev_tup_block_data + prev_tup_user_data_len), curr_tup_block_data, curr_tup_user_data_len);
		}
		else
		{
			if (prev_tup_prefix_len > curr_tup_prefix_len)
			{
				/*
				 * Updates "intersects" like this :
				 * 	  [###] - first update
				 *  [###] - second update
				 */

				Size new_part_len = prev_tup_prefix_len - curr_tup_prefix_len; // unique to second update data length (new to first update)
				Size common_part_len = prev_tup_user_data_len - (curr_tup_suffix_len - prev_tup_suffix_len);
				Size unique_part_len = prev_tup_user_data_len - common_part_len; // unique to first update data length
				char prev_tup_user_data_copy[unique_part_len]; // copy of unique to first update data

				memcpy(prev_tup_user_data_copy, (void*) (prev_tup_block_data + common_part_len), unique_part_len);

				prev_tup->blocks[0].block_data_len += new_part_len;
				prev_tup->blocks[0].block_data = (char*) repalloc(prev_tup->blocks[0].block_data, prev_tup->blocks[0].block_data_len);
				prev_tup_block_data = prev_tup->blocks[0].block_data;

				memcpy(prev_tup_block_data, curr_tup_block_data, curr_tup_user_data_len);
				memcpy((void*) (prev_tup_block_data + curr_tup_user_data_len), prev_tup_user_data_copy, unique_part_len);
			}
			else
			{
				/*
				 * Updates "intersects" like this :
				 * [###] - first update
				 *   [###] - second update
				 */

				Size new_part_len = prev_tup_suffix_len - curr_tup_suffix_len; // unique to second update data length (new to first update)
				Size common_part_len = prev_tup_user_data_len - (curr_tup_prefix_len - prev_tup_prefix_len);
				Size unique_part_len = prev_tup_user_data_len - common_part_len; // unique to first update data length

				// memcpy(prev_tup_user_data_copy, (void*) (prev_tup_block_data + common_part_len), unique_part_len);

				prev_tup->blocks[0].block_data_len += new_part_len;
				prev_tup->blocks[0].block_data = (char*) repalloc(prev_tup->blocks[0].block_data, prev_tup->blocks[0].block_data_len);
				prev_tup_block_data = prev_tup->blocks[0].block_data;

				memcpy((void*) prev_tup_block_data + unique_part_len, curr_tup_block_data, curr_tup_user_data_len);
			}
		}
	}

	return 0;
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
	XLogLongPageHeader page_hdr = (XLogLongPageHeader) palloc0(SizeOfXLogLongPHD);
	int wal_segment_size;

	fd = OpenTransientFile(WALpath, O_RDONLY | PG_BINARY);
	if (fd == -1)
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("could not open file \"%s\": %m", WALpath)));

	read_bytes = read(fd, page_hdr, SizeOfXLogLongPHD);
	if (read_bytes != SizeOfXLogLongPHD)
		ereport(ERROR,
				errmsg("could not read XLogLongPageHeader of WAL segment\"%s\": %m", 
					   WALpath));

	wal_segment_size = page_hdr->xlp_seg_size;
	Assert(IsValidWalSegSize(wal_segment_size));

	pfree(page_hdr);
	CloseTransientFile(fd);

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

	// ereport(LOG, errmsg("Before loop"));

	while ((entry = (HTABElem *) hash_seq_search(&status)) != NULL)
	{
		WALDIFFRecord WDrec = entry->data;
		static XLogRecord *record;
		if (record == NULL) 
			record = palloc0(XLogRecordMaxSize);
		else
			memset(record, 0, XLogRecordMaxSize);

		if (WDrec->rec_hdr.xl_rmid == RM_HEAP_ID)
		{
			uint8 xlog_type = WDrec->rec_hdr.xl_info & XLOG_HEAP_OPMASK;
			uint32 rec_tot_len = 0;

			// ereport(LOG, errmsg("Got XLOG_HEAP"));

			/* The records have the same header */
			memcpy(record, &(WDrec->rec_hdr), SizeOfXLogRecord);
			rec_tot_len += SizeOfXLogRecord;

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

					// ereport(LOG, errmsg("Got XLOG_HEAP_INSERT"));

					Assert(WDrec->max_block_id >= 0);
					block_0 = WDrec->blocks[0];

					/* XLogRecordBlockHeader */
					memcpy(record + rec_tot_len, &(block_0.blk_hdr), SizeOfXLogRecordBlockHeader);
					rec_tot_len += SizeOfXLogRecordBlockHeader;

					/* RelFileLocator */
					memcpy(record + rec_tot_len, &(block_0.file_loc), sizeof(RelFileLocator));
					rec_tot_len +=  sizeof(RelFileLocator);

					/* BlockNumber */
					memcpy(record + rec_tot_len, &(block_0.blknum), sizeof(BlockNumber));
					rec_tot_len += sizeof(BlockNumber);

					/* block data */
					memcpy(record + rec_tot_len, &(block_0.block_data), block_0.block_data_len);
					rec_tot_len += block_0.block_data_len;

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

					// ereport(LOG, errmsg("Got XLOG_UPDATE"));
					
					Assert(WDrec->max_block_id >= 1);
					block_0 = WDrec->blocks[0];
					block_1 = WDrec->blocks[1];

					/* XLogRecordBlockHeader 0 */
					memcpy(record + rec_tot_len, &(block_0.blk_hdr), SizeOfXLogRecordBlockHeader);
					rec_tot_len += SizeOfXLogRecordBlockHeader;

					/* RelFileLocator 0 */
					memcpy(record + rec_tot_len, &(block_0.file_loc), sizeof(RelFileLocator));
					rec_tot_len +=  sizeof(RelFileLocator);

					/* BlockNumber 0 */
					memcpy(record + rec_tot_len, &(block_0.blknum), sizeof(BlockNumber));
					rec_tot_len += sizeof(BlockNumber);

					/* block data 0 */
					memcpy(record + rec_tot_len, &(block_0.block_data), block_0.block_data_len);
					rec_tot_len += block_0.block_data_len;

					/* XLogRecordBlockHeader 1 */
					memcpy(record + rec_tot_len, &(block_1.blk_hdr), SizeOfXLogRecordBlockHeader);
					rec_tot_len += SizeOfXLogRecordBlockHeader;

					/* BlockNumber 1 */
					memcpy(record + rec_tot_len, &(block_1.blknum), sizeof(BlockNumber));
					rec_tot_len += sizeof(BlockNumber);

					break;
				}

				/* 
				 * Contains XLogRecord + XLogRecordBlockHeader_0 + 
				 * RelFileLocator_0 + BlockNumber_0 + main_data(xl_heap_delete)
				 */
				case XLOG_HEAP_DELETE:
				{
					WALDIFFBlock block_0;

					// ereport(LOG, errmsg("Got XLOG_DELETE"));

					Assert(WDrec->max_block_id >= 0);
					block_0 = WDrec->blocks[0];

					/* XLogRecordBlockHeader */
					memcpy(record + rec_tot_len, &(block_0.blk_hdr), SizeOfXLogRecordBlockHeader);
					rec_tot_len += SizeOfXLogRecordBlockHeader;

					/* RelFileLocator */
					memcpy(record + rec_tot_len, &(block_0.file_loc), sizeof(RelFileLocator));
					rec_tot_len +=  sizeof(RelFileLocator);

					/* BlockNumber */
					memcpy(record + rec_tot_len, &(block_0.blknum), sizeof(BlockNumber));
					rec_tot_len += sizeof(BlockNumber);

					break;
				}

				default:
					ereport(ERROR, errmsg("unproccessed XLOG_HEAP type"));
			}

			// ereport(LOG, errmsg("Copying main data in the end of the record"));

			/* main data */
			memcpy(record + rec_tot_len, WDrec->main_data, WDrec->main_data_len);
			rec_tot_len += WDrec->main_data_len;

			/* finally copy record's total length */
			record->xl_tot_len = rec_tot_len;
			Assert(record->xl_tot_len >= SizeOfXLogRecord);

			/* calculate and insert record's crc */
			// ereport(LOG, errmsg("Calculating crc"));
			{
				pg_crc32c crc;

				INIT_CRC32C(crc);
				COMP_CRC32C(crc, ((char *) record) + SizeOfXLogRecord, record->xl_tot_len - SizeOfXLogRecord);
				/* include the record's header last */
				COMP_CRC32C(crc, (char *) record, offsetof(XLogRecord, xl_crc));
				FIN_CRC32C(crc);

				record->xl_crc = crc;
			}
		}
		else 
			ereport(ERROR, errmsg("WALDIFF cannot contains not XLOG_HEAP types"));

		ereport(LOG, errmsg("Writing to WALDIFF segment"));
		res = WALDIFFWriteRecord(writer, record);
		if (res == WALDIFFWRITE_FAIL) 
			ereport(ERROR, errmsg("error during writing WALDIFF records in constructWALDIFFs"));
	}
}

void 
finishWALDIFFSegment(void)
{

}