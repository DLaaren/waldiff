#include "waldiff.h"
#include "waldiff_writer.h"
#include "wal_raw_reader.h"
#include "waldiff_test.h"

PG_MODULE_MAGIC;

/* GUC value's store */
static char *waldiff_dir;

/* Global data */
static MemoryContextStorage *memory_context_storage;
static WALDIFFWriterState   *writer = NULL;
static WALRawReaderState 	*raw_reader = NULL;		
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

/* Custom defined callbacks for WALReader and WALDIFFWriter */
void WalOpenSegment(XLogReaderState *reader,
					XLogSegNo nextSegNo,
					TimeLineID *tli_p);
void WALDIFFOpenSegment(WALSegment *seg, int flags);
int WalReadPage(XLogReaderState *reader, XLogRecPtr targetPagePtr, int reqLen,
				XLogRecPtr targetPtr, char *readBuff);
void WalCloseSegment(XLogReaderState *reader);				
void WALDIFFCloseSegment(WALSegment *seg);
WALDIFFRecordWriteResult WALDIFFWriteRecord(WALDIFFWriterState *writer, char *record);

WALRawRecordReadResult WALReadRawRecord(WALRawReaderState *wal_raw_reader, XLogRecord *record);
WALRawRecordSkipResult WALSkipRawRecord(WALRawReaderState *raw_reader, XLogRecord *target);

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
 * Creates the module's memory context
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

	if (waldiff_dir == NULL)
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
		writer = WALDIFFWriterAllocate(wal_segment_size, 
									   waldiff_dir,
									   WALDIFFWRITER_ROUTINE(.write_record  = WALDIFFWriteRecord,
															 .segment_open  = WALDIFFOpenSegment,
															 .segment_close = WALDIFFCloseSegment),
									   XLogRecordMaxSize + SizeOfXLogLongPHD);
	Assert(writer != NULL);

	ereport(LOG, errmsg("WALDIFFWriter was allocated successfully"));

	WALDIFFBeginWrite(writer, segno, reader_private.timeline);

	if (raw_reader == NULL)
		raw_reader = WALRawReaderAllocate(wal_segment_size, 
										  XLOGDIR, 
										  WALRAWREADER_ROUTINE(.read_record   = WALReadRawRecord,
										  					   .skip_record  = WALSkipRawRecord,
															   .segment_open  = WALDIFFOpenSegment,
															   .segment_close = WALDIFFCloseSegment), 
										  XLogRecordMaxSize);

	Assert(raw_reader != NULL);

	ereport(LOG, errmsg("WALRawReader was allocated successfully"));

	WALRawBeginRead(raw_reader, segno, reader_private.timeline);

	/* Main work */
	ereport(LOG, errmsg("archiving WAL file: %s", WALpath));

	{
		XLogLongPageHeader page_hdr;
		XLogRecPtr 	   first_record = XLogFindNextRecord(reader_state, reader_private.startptr);
		
		if (first_record == InvalidXLogRecPtr)
		{
			ereport(FATAL, 
					errmsg("could not find a valid record after %X/%X", 
							LSN_FORMAT_ARGS(reader_private.startptr)));
			return false;
		}

		page_hdr = (XLogLongPageHeader) reader_state->readBuf;

		// This cases we should consider later 
		if (page_hdr->std.xlp_rem_len)
			ereport(LOG, 
					errmsg("got some remaining data from a previous page : %d", page_hdr->std.xlp_rem_len));

		if (first_record != reader_private.startptr && 
			XLogSegmentOffset(reader_private.startptr, wal_segment_size) != 0)
			ereport(LOG, 
					errmsg("skipping over %u bytes", (uint32) (first_record - reader_private.startptr)));
		
		writer->first_page_addr = raw_reader->first_page_addr = page_hdr->std.xlp_pageaddr;
		writer->system_identifier = raw_reader->system_identifier = page_hdr->xlp_sysid;
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
					entry->data = NULL;
					copy_waldiff_record(entry->data, WDrec);
					free_waldiff_record(WDrec);
					Assert(entry->key == hash_key);
					
					raw_reader->routine.skip_record(raw_reader, WALRec);
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
						overlay_result = overlay_update(entry->data, WDrec);

						if (overlay_result == -1)
						{
							/*
							 * Overlaying failed - we must store both records
							 */
							entry = (HTABElem *) hash_search(hash_table, (void*) &hash_key, HASH_ENTER, NULL);
							entry->data = NULL;
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
						entry->data = NULL;
						copy_waldiff_record(entry->data, WDrec);
						free_waldiff_record(WDrec);
						Assert(entry->key == hash_key);
					}

					raw_reader->routine.skip_record(raw_reader, WALRec);
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

					raw_reader->routine.skip_record(raw_reader, WALRec);
					break;

				/* unprocessed record type */
				default:
					WALDIFFRecordWriteResult write_res;
					WALRawRecordReadResult   read_res;

					reset_buff(raw_reader);
					reset_tmp_buff(raw_reader);
					
					read_res = raw_reader->routine.read_record(raw_reader, WALRec);
					if (read_res == WALREAD_FAIL)
						ereport(ERROR, errmsg("error during reading raw record from WAL segment: %s", 
											  WALDIFFWriterGetErrMsg(writer)));
					
					write_res = writer->routine.write_record(writer, WALRawReaderGetLastRecordRead(raw_reader));
					if (write_res == WALDIFFWRITE_FAIL) 
						ereport(ERROR, errmsg("error during writing WALDIFF records in waldiff_archive: %s", 
											  WALDIFFWriterGetErrMsg(writer)));
					break;
			}
		} 
		
		else 
		{
			WALDIFFRecordWriteResult write_res;
			WALRawRecordReadResult   read_res;

			reset_buff(raw_reader);
			reset_tmp_buff(raw_reader);

			read_res = raw_reader->routine.read_record(raw_reader, WALRec);
			if (read_res == WALREAD_FAIL)
				ereport(ERROR, errmsg("error during reading raw record from WAL segment: %s", 
										WALDIFFWriterGetErrMsg(writer)));
			
			write_res = writer->routine.write_record(writer, WALRawReaderGetLastRecordRead(raw_reader));
			if (write_res == WALDIFFWRITE_FAIL) 
				ereport(ERROR, errmsg("error during writing WALDIFF records in waldiff_archive: %s", 
										WALDIFFWriterGetErrMsg(writer)));
		}
	}

	ereport(LOG, errmsg("constructing WALDIFFs"));

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
waldiff_shutdown(ArchiveModuleState *reader)
{
	/* Must have 'case this function closes fd */
	XLogReaderFree(reader_state);
	WALDIFFWriterFree(writer);
	WALRawReaderFree(raw_reader);

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
 * Opens new WAL/WALDIFF segment file with specified flags
 */
void 
WALDIFFOpenSegment(WALSegment *seg, int flags)
{
	char fname[XLOG_FNAME_LEN];
	char fpath[MAXPGPATH];

	XLogFileName(fname, seg->tli, seg->segno, seg->segsize);

	if (snprintf(fpath, MAXPGPATH, "%s/%s", seg->dir, fname) == -1)
		ereport(ERROR,
				errmsg("error during reading WALDIFF absolute path: %s/%s", seg->dir, fname));
	
	seg->fd = OpenTransientFile(fpath, flags);
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
WALDIFFCloseSegment(WALSegment *seg)
{
	Assert(seg->fd != -1);
	CloseTransientFile(seg->fd);
	seg->fd = -1;
}

/*
 * Skip next record in WAL segment. You must reset raw_reader's buffers before 
 * calling this function. raw_reader's buffers will not contain skipped record
 */
WALRawRecordSkipResult
WALSkipRawRecord(WALRawReaderState *raw_reader, XLogRecord *target)
{
	int	   nbytes = 0;

	while (true)
	{
		/* check whether we are in start of block */
		if ((raw_reader->already_read) % BLCKSZ == 0)
		{
			XLogPageHeader hdr;
			nbytes = append_to_tmp_buff(raw_reader, SizeOfXLogShortPHD); /* skip short header */
			if (nbytes == 0)
				return WALSKIP_EOF;
			
			/*
			 * Check whether we are in start of segment and skip rest of header if needed
			 */
			hdr = (XLogPageHeader) raw_reader->tmp_buffer;
			if (XLogPageHeaderSize(hdr) > SizeOfXLogShortPHD)
			{
				lseek(raw_reader->wal_seg.fd, SizeOfXLogLongPHD - SizeOfXLogShortPHD, SEEK_CUR);
				raw_reader->already_read += SizeOfXLogLongPHD - SizeOfXLogShortPHD;
			}
			
			if (hdr->xlp_rem_len > 0)
			{
				/*
				 * Even rest of record may be so large that it  will not fit on the page. 
				 * data_len - part of record that is in the current page
				 */
				uint64 data_len = 0;

				if (! XlogRecFitsOnPage(raw_reader->already_read, hdr->xlp_rem_len))
					data_len = BLCKSZ * (1 + (raw_reader->already_read / BLCKSZ)) - raw_reader->already_read; /* rest of current page */
				else
					data_len = hdr->xlp_rem_len;
				
				lseek(raw_reader->wal_seg.fd, data_len, SEEK_CUR);
				raw_reader->already_read += data_len;

				if (data_len == hdr->xlp_rem_len) /* if record remains fit on the current page */
				{
					lseek(raw_reader->wal_seg.fd, MAXALIGN(hdr->xlp_rem_len) - data_len, SEEK_CUR); /* Records and maxaligned, so skip padding */
					raw_reader->already_read += MAXALIGN(hdr->xlp_rem_len) - data_len;

					return WALSKIP_SUCCESS; /* full record skipped, so return success */
				}
				else /* remaining part of record will be skipped in next iterations */
					continue;
			}
		}

		/*
		 * We are not in the start of the page, so try read record header. 
		 * We take into account that header may not fit on the rest of the current page
		 */

		if (! XlogRecHdrFitsOnPage(raw_reader->already_read))
		{
			uint64 data_len = BLCKSZ * (1 + (raw_reader->already_read) / BLCKSZ) - raw_reader->already_read; /* rest of current page */

			/*
			 * Skip as much as we can. Remaining part of record will be read in next iterations
			 */
			lseek(raw_reader->wal_seg.fd, data_len, SEEK_CUR);
			raw_reader->already_read += data_len;
			
			continue;
		}
		else 
		{
			uint64 		data_len; /* data_len - part of record that is in the current page. It may be full record length */
			XLogRecord* record;

			reset_tmp_buff(raw_reader); /* we want tmp buffer to contain only record header */
			nbytes = append_to_tmp_buff(raw_reader, SizeOfXLogRecord);
			if (nbytes == 0)
				return WALSKIP_EOF;
			
			record = (XLogRecord*) raw_reader->tmp_buffer;

			/*
			 * Record may not fit on the rest of the current page
			 */
			if (! XlogRecFitsOnPage(raw_reader->already_read, record->xl_tot_len - SizeOfXLogRecord))
				data_len = BLCKSZ * (1 + (raw_reader->already_read) / BLCKSZ) - raw_reader->already_read; /* rest of current page */
			else
				data_len = record->xl_tot_len - SizeOfXLogRecord;

			lseek(raw_reader->wal_seg.fd, data_len, SEEK_CUR);
			raw_reader->already_read += data_len;
			
			if (data_len == (record->xl_tot_len - SizeOfXLogRecord)) /* If record fit on the current page */
			{
				lseek(raw_reader->wal_seg.fd, MAXALIGN(record->xl_tot_len) - SizeOfXLogRecord - data_len, SEEK_CUR); /* Records and maxaligned, so skip padding */
				raw_reader->already_read += MAXALIGN(record->xl_tot_len) - SizeOfXLogRecord - data_len;

				return WALSKIP_SUCCESS; /* full record skipped, so return success */
			}
			else /* remaining part of record will be skipped in next iterations */
				continue;
		}
	}
	return WALSKIP_FAIL;
}

/*
 * Read next record in WAL segment. You must reset raw_reader's buffers before 
 * calling this function
 * After execution, you can get pointer to raw record via WALRawReaderGetLastRecordRead
 */
WALRawRecordReadResult 
WALReadRawRecord(WALRawReaderState *raw_reader, XLogRecord *target)
{
	int	   nbytes = 0;

	while (true)
	{
		/* check whether we are in start of block */
		if ((raw_reader->already_read) % BLCKSZ == 0)
		{
			XLogPageHeader hdr;
			reset_tmp_buff(raw_reader); /* we want tmp buff to only contain page header */
			nbytes = append_to_tmp_buff(raw_reader, SizeOfXLogShortPHD); /* skip short header */
			if (nbytes == 0)
				return WALREAD_EOF;
			
			hdr = (XLogPageHeader) raw_reader->tmp_buffer;
			if (XLogPageHeaderSize(hdr) > SizeOfXLogShortPHD) /* check whether we are in start of segment */
			{
				XLogLongPageHeader long_hdr;
				nbytes = append_to_tmp_buff(raw_reader, SizeOfXLogLongPHD - SizeOfXLogShortPHD); /* skip rest of long header */
				if (nbytes == 0)
					return WALREAD_EOF;

				long_hdr  = (XLogLongPageHeader) raw_reader->tmp_buffer;

				/*
				 * If we are testing this function, raw_reader
				 * still does not contain this values
				 */
				if (raw_reader->wal_seg.segsize == 0)
				{
					raw_reader->wal_seg.segsize   = long_hdr->xlp_seg_size;
					raw_reader->system_identifier = long_hdr->xlp_sysid;
					raw_reader->first_page_addr   = hdr->xlp_pageaddr;
					raw_reader->wal_seg.tli 	  = hdr->xlp_tli;
				}
				
				ereport(LOG, errmsg("READ LONG HDR. ADDR : %ld", long_hdr->std.xlp_pageaddr));
			}

			if (hdr->xlp_rem_len > 0)
			{
				/*
				 * Even rest of record may be so large that it  will not fit on the page. 
				 * data_len - part of record that is in the current page
				 */
				uint64 data_len = 0;

				if (! XlogRecFitsOnPage(raw_reader->already_read, hdr->xlp_rem_len))
					data_len = BLCKSZ * (1 + (raw_reader->already_read / BLCKSZ)) - raw_reader->already_read; /* rest of current page */
				else
					data_len = hdr->xlp_rem_len;
				
				nbytes = append_to_buff(raw_reader, data_len);
				if (nbytes == 0)
					return WALREAD_EOF;

				if (data_len == hdr->xlp_rem_len) /* if record remains fit on the current page */
				{
					lseek(raw_reader->wal_seg.fd, MAXALIGN(hdr->xlp_rem_len) - data_len, SEEK_CUR); /* Records and maxaligned, so skip padding */
					raw_reader->already_read += MAXALIGN(hdr->xlp_rem_len) - data_len;
					return WALREAD_SUCCESS; /* full record is in out buffer, so return success */
				}
				else /* remaining part of record will be read in next iterations */
					continue;
			}
		}

		/*
		 * We are not in the start of the page, so try read record header. 
		 * We take into account that header may not fit on the rest of the current page
		 */

		if (! XlogRecHdrFitsOnPage(raw_reader->already_read))
		{
			uint64 data_len = BLCKSZ * (1 + (raw_reader->already_read) / BLCKSZ) - raw_reader->already_read; /* rest of current page */

			/*
			 * Read as much as we can. Remaining part of record will be read in next iterations
			 */
			nbytes = append_to_buff(raw_reader, data_len);
			if (nbytes == 0)
				return WALREAD_EOF;
			
			continue;
		}
		else 
		{
			uint64 		data_len; /* data_len - part of record that is in the current page. It may be full record length */
			XLogRecord* record;

			reset_tmp_buff(raw_reader); /* we want tmp buffer to contain only record header */
			nbytes = append_to_tmp_buff(raw_reader, SizeOfXLogRecord);
			if (nbytes == 0)
				return WALREAD_EOF;
			
			record = (XLogRecord*) raw_reader->tmp_buffer;

			/*
			 * avoid OOM
			 */
			if (SizeOfXLogRecord > WALRawReaderGetRestBufferCapacity(raw_reader))
			{
				ereport(WARNING, 
						errmsg("cannot write xlog header to buffer with rest capacity %ld", 
								WALRawReaderGetRestBufferCapacity(raw_reader)));
				return WALREAD_FAIL;
			}

			/*
			 * If we did everything right before, buffer is empty and we write header into it
			 */
			memcpy((void*) (raw_reader->buffer + raw_reader->buffer_fullness), raw_reader->tmp_buffer, SizeOfXLogRecord);
			raw_reader->buffer_fullness += SizeOfXLogRecord;

			/*
			 * Record may not fit on the rest of the current page
			 */
			if (! XlogRecFitsOnPage(raw_reader->already_read, record->xl_tot_len - SizeOfXLogRecord))
				data_len = BLCKSZ * (1 + (raw_reader->already_read) / BLCKSZ) - raw_reader->already_read; /* rest of current page */
			else
				data_len = record->xl_tot_len - SizeOfXLogRecord;
			
			nbytes = append_to_buff(raw_reader, data_len);
			if (nbytes == 0)
				return WALREAD_EOF;
			
			if (data_len == (record->xl_tot_len - SizeOfXLogRecord)) /* If record fit on the current page */
			{
				lseek(raw_reader->wal_seg.fd, MAXALIGN(record->xl_tot_len) - SizeOfXLogRecord - data_len, SEEK_CUR); /* Records and maxaligned, so skip padding */
				raw_reader->already_read += MAXALIGN(record->xl_tot_len) - SizeOfXLogRecord - data_len;

				return WALREAD_SUCCESS; /* full record is in out buffer, so return success */
			}
			else /* remaining part of record will be read in next iterations */
				continue;
		}
	}
	return WALREAD_FAIL;
}

/* 
 * WALDIFFWriteRecord
 * 
 * Accumulate records in WALDIFFWriter's buffer, then writes them all at once.
 */
WALDIFFRecordWriteResult 
WALDIFFWriteRecord(WALDIFFWriterState *writer, char *record)
{
	Size rem_data_len = 0;
	pg_crc32c	crc;
	int	nbytes = 0;
	char* record_copy = record; /* we need second pointer for navigation through record */
	XLogRecord* record_hdr = (XLogRecord*) record;

	/* We use it when we need to put padding into file */
	char 		null_buff[1024];
	memset(null_buff, 0, 1024);

	while (true)
	{
		/* check whether we are in start of block */
		if (writer->already_written % BLCKSZ == 0)
		{
			/* check whether we are in start of segment */
			if (writer->already_written == 0)
			{
				/*
				 * We must create long page header and write it to WALDIFF segment
				 */
				XLogLongPageHeaderData long_hdr = {0};

				long_hdr.xlp_sysid 		  = writer->system_identifier;
				long_hdr.xlp_seg_size	  = writer->waldiff_seg.segsize;
				long_hdr.xlp_xlog_blcksz  = XLOG_BLCKSZ;

				long_hdr.std.xlp_info 	  = 0;
				long_hdr.std.xlp_info 	  |= XLP_LONG_HEADER;
				long_hdr.std.xlp_tli 	  = writer->waldiff_seg.tli;
				long_hdr.std.xlp_rem_len  = 0;
				long_hdr.std.xlp_magic 	  = XLOG_PAGE_MAGIC;
				long_hdr.std.xlp_pageaddr = writer->first_page_addr;

				ereport(LOG, errmsg("WRITE LONG HDR. ADDR : %ld", long_hdr.std.xlp_pageaddr));
				
				nbytes = write_data_to_file(writer, (char*)&long_hdr, SizeOfXLogLongPHD);
				if (nbytes == 0)
					return WALDIFFWRITE_EOF;
			}
			else
			{
				/*
				 * We must create short page header and write it to WALDIFF segment
				 */
				XLogPageHeaderData hdr;

				hdr.xlp_info 	 = 0;
				hdr.xlp_info 	 |= XLP_BKP_REMOVABLE;

				if (rem_data_len > 0)
					hdr.xlp_info |= XLP_FIRST_IS_CONTRECORD;
				
				hdr.xlp_rem_len  = rem_data_len;
				
				hdr.xlp_tli 	 = writer->waldiff_seg.tli;
				hdr.xlp_magic 	 = XLOG_PAGE_MAGIC;
				hdr.xlp_pageaddr = writer->first_page_addr + writer->already_written;

				nbytes = write_data_to_file(writer, (char*)&hdr, SizeOfXLogShortPHD);
				if (nbytes == 0)
					return WALDIFFWRITE_EOF;
			}
		}

		if (rem_data_len > 0)
		{
			/*
			 * Record may not fit on the rest of the current page
			 */
			if (! XlogRecFitsOnPage(writer->already_written, rem_data_len))
			{
				uint64 data_len = BLCKSZ * (1 + (writer->already_written / BLCKSZ)) - writer->already_written; /* rest of current page */
				nbytes = write_data_to_file(writer, record_copy, data_len);
				rem_data_len = rem_data_len - data_len;
				record_copy += data_len; /* we don't need this part of record anymore */

				continue; /* remaining part of record will be written in next iterations */
			}
			else
			{
				nbytes = write_data_to_file(writer, record_copy, rem_data_len);

				/*
				 * Records are maxaligned, so we must write all padding too
				 */
				if (MAXALIGN(record_hdr->xl_tot_len) - record_hdr->xl_tot_len)
				{
					nbytes = write_data_to_file(writer, null_buff, MAXALIGN(record_hdr->xl_tot_len) - record_hdr->xl_tot_len);
					if (nbytes == 0)
						return WALDIFFWRITE_EOF;
				}

				return WALDIFFWRITE_SUCCESS;
			}
		}

		/*
		 * First record in first segment has no previous records
		 */
		if (writer->already_written == SizeOfXLogLongPHD && writer->waldiff_seg.segno == 1)
			record_hdr->xl_prev = 0;
		else
			record_hdr->xl_prev = writer->waldiff_seg.last_processed_record + writer->first_page_addr;


		writer->waldiff_seg.last_processed_record = writer->already_written;

		/*
		 * Creating checksum (we possibly change xl_prev, so checksum also must be changed)
		 */
		INIT_CRC32C(crc);
		COMP_CRC32C(crc, (char*) (record_hdr + SizeOfXLogRecord), record_hdr->xl_tot_len - SizeOfXLogRecord);
		COMP_CRC32C(crc, record_hdr, offsetof(XLogRecord, xl_crc));
		FIN_CRC32C(crc);
		record_hdr->xl_crc = crc;

		/*
		 * We take into account that header may not fit on the rest of the current page
		 */

		if (! XlogRecFitsOnPage(writer->already_written, record_hdr->xl_tot_len))
		{
			uint64 data_len = BLCKSZ * (1 + (writer->already_written / BLCKSZ)) - writer->already_written; /* rest of current page */
			rem_data_len = record_hdr->xl_tot_len - data_len;

			nbytes = write_data_to_file(writer, record_copy, data_len); /* write to file as much as we can */
			if (nbytes == 0)
				return WALDIFFWRITE_EOF;
			
			record_copy += data_len;
			continue; /* remaining part of record will be written in next iterations */
		}
		else
		{
			nbytes = write_data_to_file(writer, record_copy, record_hdr->xl_tot_len);
			if (nbytes == 0)
				return WALDIFFWRITE_EOF;

			/*
			 * Records are maxaligned, so we must write all padding too
			 */
			if (MAXALIGN(record_hdr->xl_tot_len) - record_hdr->xl_tot_len > 0)
			{
				nbytes = write_data_to_file(writer, null_buff, MAXALIGN(record_hdr->xl_tot_len) - record_hdr->xl_tot_len);
				if (nbytes == 0)
					return WALDIFFWRITE_EOF;
			}
			return WALDIFFWRITE_SUCCESS;
		}
	}

	return WALDIFFWRITE_FAIL;
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
			(src->type == XLOG_HEAP_UPDATE && (i > 0) ) )
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

			char prev_tup_user_data_copy[MaxHeapTupleSize];
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
				char prev_tup_user_data_copy[MaxHeapTupleSize]; // copy of unique to first update data

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

				memcpy((void*) (prev_tup_block_data + unique_part_len), curr_tup_block_data, curr_tup_user_data_len);
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
	WALDIFFRecordWriteResult res = 0;

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
		// TODO здесь мы должны назначить другой  callback для записи (или добавить все таки свой rmgr)
		// res = WALDIFFWriteRecord(writer, record);
		if (res == WALDIFFWRITE_FAIL) 
			ereport(ERROR, errmsg("error during writing WALDIFF records in constructWALDIFFs"));
	}
}

void 
finishWALDIFFSegment(void)
{

}