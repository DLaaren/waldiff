#include "waldiff.h"

PG_MODULE_MAGIC;

/* Private defines */
typedef struct HTABEntry
{
	uint32_t 	  key;
	WALDIFFRecord data;
} HTABEntry;

typedef struct XLogReaderPrivate
{
	TimeLineID	timeline;
	XLogRecPtr	startptr;
	XLogRecPtr	endptr;
	bool		endptr_reached;
} XLogReaderPrivate;

typedef struct MemoryContextStorage
{
	MemoryContext old;
	MemoryContext current;
} MemoryContextStorage;

// typedef struct WriterState {
// 	WALOpenSegment seg;
// 	XLogRecPtr	WriteRecPtr;	/* start of last record written */
// 	XLogRecPtr	EndRecPtr;		/* end+1 of last record written */
// } WriterState;


/* GUC value's store */
static char *waldiff_dir;

/* Global data */
static MemoryContextStorage *memory_context_storage;
static XLogReaderState		*reader_state;
// static WriterState			*writer_state;
static HTAB 				*hash_table;

/*
 * We maintain an image of pg_control in shared memory.
 */
static ControlFileData *ControlFile = NULL;

/* Forward declaration */
static void waldiff_startup(ArchiveModuleState *reader);
static bool waldiff_configured(ArchiveModuleState *reader);
static bool waldiff_archive(ArchiveModuleState *reader, const char *file, const char *path);
static void waldiff_shutdown(ArchiveModuleState *reader);

static void ReadControlFile(void);
static void WriteControlFile(void);

/* Custom defined callbacks for XLogReaderState */
void WalOpenSegment(XLogReaderState *reader,
					XLogSegNo nextSegNo,
					TimeLineID *tli_p);
int WalReadPage(XLogReaderState *reader, XLogRecPtr targetPagePtr, int reqLen,
				XLogRecPtr targetPtr, char *readBuff);
void WalCloseSegment(XLogReaderState *reader);

/* Main work */
static void first_passage(void);
static void second_passage(XLogRecPtr last_checkpoint);

/* This three fuctions returns palloced struct */
static void fetch_insert(WALDIFFRecord *WDRec);
static void fetch_hot_update(WALDIFFRecord *WDRec);

static int overlay_hot_update(WALDIFFRecord prev_tup, WALDIFFRecord curr_tup);
static XLogRecord *constructWALDIFF(WALDIFFRecord WDRec);

/* Helper functions */
static void free_waldiff_record(WALDIFFRecord record);

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
	HASHCTL hash_ctl;
	enum {hash_table_initial_size = 128};

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

#ifdef WALDIFF_DEBUG
	ereport(LOG, errmsg("WALDIFF: Memory contextes were allocated successfully"));
#endif

	/* Secondly, allocating the hash table */
    hash_ctl.keysize    = sizeof(uint32_t);
	hash_ctl.entrysize 	= sizeof(HTABEntry);
	hash_ctl.hash 		= &tag_hash;
	/* It is said hash table must have its own memory context */
	hash_ctl.hcxt = memory_context_storage->current; //AllocSetContextCreate(memory_context_storage->current,
										//   "WALDIFF_HTAB",
										//   ALLOCSET_DEFAULT_SIZES);  
	Assert(hash_ctl.hcxt != NULL);								  

    hash_table = hash_create("WALDIFFHashTable", hash_table_initial_size,
                             &hash_ctl, HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);
	Assert(hash_table != NULL);

#ifdef WALDIFF_DEBUG
	ereport(LOG, errmsg("WALDIFF: Hash tables was allocated successfully"));
#endif

	/* Thirdly, create a role for archiver and databases which names listed in conf file */
	// create_role_for_archiver();

	// for (;;) {


	// }
#ifdef WALDIFF_DEBUG
	ereport(LOG, errmsg("WALDIFF: Postgres was initialized successfully"));
#endif
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
		GUC_check_errmsg("WALDIFF: Specified WALDIFF archive directory does not exist: %m");
        GUC_check_errmsg("WALDIFF: Creating WALDIFF archive directory");
		if (pg_mkdir_p(waldiff_dir, 0700) != 0)
		{
			GUC_check_errmsg("WALDIFF: Could not allocate specified WALDIFF directory: %m");
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
	XLogReaderPrivate reader_private = {0};
	XLogSegNo 		  segno = 0;
	XLogRecPtr        last_checkpoint = InvalidXLogRecPtr;

#ifdef WALDIFF_DEBUG
	ereport(LOG, errmsg("WALDIFF: WAL segment is being archived: %s", WALpath));
#endif

	reader_private.timeline = 1;
	reader_private.startptr = InvalidXLogRecPtr;
	reader_private.endptr = InvalidXLogRecPtr;
	reader_private.endptr_reached = false;

	XLogFromFileName(WALfile, &reader_private.timeline, &segno, DEFAULT_XLOG_SEG_SIZE);
	XLogSegNoOffsetToRecPtr(segno, 0, DEFAULT_XLOG_SEG_SIZE, reader_private.startptr);
	XLogSegNoOffsetToRecPtr(segno + 1, 0, DEFAULT_XLOG_SEG_SIZE, reader_private.endptr);
	Assert(!XLogRecPtrIsInvalid(reader_private.startptr));
	Assert(!XLogRecPtrIsInvalid(reader_private.endptr));

#ifdef WALDIFF_DEBUG
	ereport(LOG, errmsg("WALDIFF: segNo: %lu; tli: %u; startptr: %lu; endptr: %lu",
						segno, reader_private.timeline, reader_private.startptr,
						reader_private.endptr));
#endif

	if (reader_state == NULL)
		reader_state = XLogReaderAllocate(DEFAULT_XLOG_SEG_SIZE, 
										  XLOGDIR,
										  XL_ROUTINE(.page_read = WalReadPage,
													 .segment_open = WalOpenSegment,
													 .segment_close = WalCloseSegment),
										  &reader_private);
	Assert(reader_state != NULL);

#ifdef WALDIFF_DEBUG
	ereport(LOG, errmsg("WALDIFF: XLogReader was allocated successfully"));
#endif

	// if (writer_state == NULL)
	// 	writer_state = palloc0(sizeof(WriterState));
	// Assert(writer_state != NULL);

	// XLogSegNoOffsetToRecPtr(segno, 0, DEFAULT_XLOG_SEG_SIZE, writer_state->WriteRecPtr);
	// XLogSegNoOffsetToRecPtr(segno + 1, 0, DEFAULT_XLOG_SEG_SIZE, writer_state->EndRecPtr);
	// Assert(!XLogRecPtrIsInvalid(writer_state->WriteRecPtr));
	// Assert(!XLogRecPtrIsInvalid(writer_state->EndRecPtr));
	// {
	// 	char fname[XLOG_FNAME_LEN];
	// 	char fpath[MAXPGPATH];
	// 	XLogFileName(fname, reader_private.timeline, segno, DEFAULT_XLOG_SEG_SIZE);

	// 	if (snprintf(fpath, MAXPGPATH, "%s/%s", waldiff_dir, fname) == -1)
	// 		ereport(ERROR,
	// 				errmsg("WALDIFF: error during reading WAL absolute path : %s/%s", waldiff_dir, fname));

	// 	writer_state->seg.ws_file = OpenTransientFile(fpath, PG_BINARY | O_RDONLY);
	// 	if (writer_state->seg.ws_file == -1)
	// 		ereport(ERROR,
	// 				(errcode_for_file_access(),
	// 				errmsg("WALDIFF: could not open WAL segment \"%s\": %m", fpath)));
	// }

	// ereport(LOG, errmsg("Writer was allocated successfully"));

	/* Main work */
#ifdef WALDIFF_DEBUG
	ereport(LOG, errmsg("WALDIFF: archiving WAL file: %s", WALpath));
#endif

	/* 
	 * first passage:
	 *		
	 *		Reading with decoding, aslo filling the HTAB with potential WALDIFF records
	 *
	 * second passage:
	 * 
	 * 		Simple reading with writing constructed WALDIFF records according to the HTAB
	 */

#ifdef WALDIFF_DEBUG
	ereport(LOG, errmsg("WALDIFF: first passage"));
#endif

	first_passage();
	XLogReaderFree(reader_state);

	reader_private.timeline = 1;
	reader_private.startptr = InvalidXLogRecPtr;
	reader_private.endptr = InvalidXLogRecPtr;
	reader_private.endptr_reached = false;

	XLogSegNoOffsetToRecPtr(segno, 0, DEFAULT_XLOG_SEG_SIZE, reader_private.startptr);
	XLogSegNoOffsetToRecPtr(segno + 1, 0, DEFAULT_XLOG_SEG_SIZE, reader_private.endptr);
	Assert(!XLogRecPtrIsInvalid(reader_private.startptr));
	Assert(!XLogRecPtrIsInvalid(reader_private.endptr));

	reader_state = XLogReaderAllocate(DEFAULT_XLOG_SEG_SIZE, 
										  XLOGDIR,
										  XL_ROUTINE(.page_read = WalReadPage,
													 .segment_open = WalOpenSegment,
													 .segment_close = WalCloseSegment),
										  &reader_private);
	Assert(reader_state != NULL);

#ifdef WALDIFF_DEBUG
	ereport(LOG, errmsg("WALDIFF: second passage"));
#endif

	second_passage(last_checkpoint);

	if (last_checkpoint != InvalidXLogRecPtr) {

		/*
		* Update ControlFile with last checkpoint data
		*/
		ControlFile = (ControlFileData*) palloc0(sizeof(ControlFileData));
#ifdef WALDIFF_DEBUG
		ereport(LOG, errmsg("WALDIFF: write new last checkpoint location : %X/%X", LSN_FORMAT_ARGS(last_checkpoint)));
#endif 

		LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
		ReadControlFile();

		ControlFile->checkPoint = ControlFile->checkPointCopy.redo = last_checkpoint;

		INIT_CRC32C(ControlFile->crc);
		COMP_CRC32C(ControlFile->crc,
					(char *) ControlFile,
					offsetof(ControlFileData, crc));
		FIN_CRC32C(ControlFile->crc);

		WriteControlFile();
		LWLockRelease(ControlFileLock);
		
	}

#ifdef WALDIFF_DEBUG
	ereport(LOG, errmsg("WALDIFF: archived WAL file: %s", WALpath));
#endif
	// XLogReaderFree(reader_state);

	return true;
}

static void 
first_passage(void)
{
	XLogReaderPrivate *reader_private = (XLogReaderPrivate *) (reader_state->private_data);
	XLogRecPtr 	   first_record = XLogFindNextRecord(reader_state, reader_private->startptr);
	
	if (first_record == InvalidXLogRecPtr)
		ereport(FATAL, 
				errmsg("WALDIFF: could not find a valid record after %X/%X", 
						LSN_FORMAT_ARGS(reader_private->startptr)));

	if (first_record != reader_private->startptr && 
		XLogSegmentOffset(reader_private->startptr, DEFAULT_XLOG_SEG_SIZE) != 0)
		ereport(LOG, 
				errmsg("WALDIFF: skipping over %u bytes", (uint32) (first_record - reader_private->startptr)));
	
	for (;;)
	{
		WALDIFFRecord  WDRec;
		XLogRecord 	  *WALRec;
		char 		  *errormsg;

		WALRec = XLogReadRecord(reader_state, &errormsg);
		if (WALRec == InvalidXLogRecPtr) {
			if (reader_private->endptr_reached)
				break;

            ereport(ERROR, 
					errmsg("WALDIFF: XLogReadRecord failed to read WAL record: %s", errormsg));
        }

		/* Now we're processing only several HEAP type WAL records and without image */
		if (XLogRecGetRmid(reader_state) == RM_HEAP_ID && !XLogRecHasBlockImage(reader_state, 0))
		{
			uint32_t prev_hash_key;
			uint32_t hash_key;
			HTABEntry *entry;
			bool is_found;

			switch(XLogRecGetInfo(reader_state) & XLOG_HEAP_OPMASK)
			{
				case XLOG_HEAP_INSERT:
				{
					fetch_insert(&WDRec);
					if (WDRec == NULL)
						ereport(ERROR, errmsg("WALDIFF: fetch_insert failed"));

					hash_key = GetHashKeyOfWALDIFFRecord(WDRec);
					hash_search(hash_table, (void*) &hash_key, HASH_FIND, &is_found);
					if (is_found)
						ereport(ERROR, errmsg("WALDIFF: found HTAB entry that shouldn't be there"));

					entry = (HTABEntry *) hash_search(hash_table, (void*) &hash_key, HASH_ENTER, NULL);
					entry->data = WDRec;

					Assert(entry->key == hash_key);

#ifdef WALDIFF_DEBUG
					// ereport(LOG, errmsg("WALDIFF: HEAP INSERT record lsn = %X/%X; hash_key = %u; blknum = %d; offnum = %u", 
										// LSN_FORMAT_ARGS(entry->data->lsn), hash_key, entry->data->blocks[0].blknum, ((xl_heap_insert *)(entry->data->main_data))->offnum));
#endif				
					break;
				}

// 				case XLOG_HEAP_HOT_UPDATE:
// 				{
// 					fetch_hot_update(&WDRec);
// 					if (WDRec == NULL)
// 						ereport(ERROR, errmsg("WALDIFF: fetch_hot_update failed"));

// 					prev_hash_key = GetHashKeyOfPrevWALDIFFRecord(WDRec);
// 					hash_key = GetHashKeyOfWALDIFFRecord(WDRec);

// 					entry = (HTABEntry *) hash_search(hash_table, (void*) &prev_hash_key, HASH_FIND, &is_found);
// 					if (is_found)
// 					{
// 						int overlay_result;

// #ifdef WALDIFF_DEBUG
// 						// ereport(LOG, errmsg("WALDIFF: HEAP HOT UPDATE record has previous record"));
// #endif		

// 						//overlay_result = overlay_hot_update(entry->data, WDRec);

// 						overlay_result = 0;

// 						if (overlay_result == -1)
// 						{
// 							/*
// 							 * Overlaying failed - we must store both records, the previous one is already in the HTAB
// 							 */
// 							entry = (HTABEntry *) hash_search(hash_table, (void*) &hash_key, HASH_ENTER, NULL);
// 							entry->data = WDRec;

// #ifdef WALDIFF_DEBUG
// 							ereport(LOG, errmsg("WALDIFF: cannot overlay HEAP HOT UPDATE record"));
// #endif		
// 						}
// 						else
// 						{
// 							/*
// 							 * Overlaying suceeded - we must store one overlaied record and free the previous one
// 							 */
// 							WALDIFFRecord overlaied_WDRec = entry->data;

// 							entry = (HTABEntry *) hash_search(hash_table, (void*) &prev_hash_key, HASH_REMOVE, NULL);

// 							Assert(entry != NULL);
// 							Assert(entry->data != NULL);
// 							free_waldiff_record(entry->data);

// 							entry = (HTABEntry *) hash_search(hash_table, (void*) &hash_key, HASH_ENTER, NULL);
// 							entry->data = overlaied_WDRec;

// 							entry->data->chain_length += 1;
// #ifdef WALDIFF_DEBUG
// 							ereport(LOG, errmsg("WALDIFF: overlaied HEAP HOT UPDATE record"));
// #endif		
// 						}
// 					}
// 					else 
// 					{
// #ifdef WALDIFF_DEBUG
// 						// ereport(LOG, errmsg("WALDIFF: HEAP HOT UPDATE record has no previous record"));
// #endif	
// 						entry = (HTABEntry *) hash_search(hash_table, (void*) &hash_key, HASH_ENTER, NULL);
// 						entry->data = WDRec;
// 					}
// 					Assert(entry->key == hash_key);

// #ifdef WALDIFF_DEBUG
// 					// ereport(LOG, errmsg("WALDIFF: HEAP HOT UPDATE record lsn = %X/%X; hash_key = %u; prev_hash_key = %u; chain lenght = %u; blknum = %d; old_offnum = %u; new_offnum = %u", 
// 										// LSN_FORMAT_ARGS(entry->data->lsn), hash_key, prev_hash_key, entry->data->chain_length, 
// 										// entry->data->blocks[0].blknum, ((xl_heap_update *)(entry->data->main_data))->old_offnum, ((xl_heap_update *)(entry->data->main_data))->new_offnum));
// #endif		
// 					break;
// 				}

				/* unprocessed record type */
				default:
					break;
			}			
		} 
	}
}

static void  
second_passage(XLogRecPtr last_checkpoint)
{
	XLogReaderPrivate 	*reader_private = (XLogReaderPrivate *) (reader_state->private_data);
	XLogRecPtr 	   		 first_record = XLogFindNextRecord(reader_state, reader_private->startptr);
	
	if (first_record == InvalidXLogRecPtr)
		ereport(FATAL, 
				errmsg("WALDIFF: could not find a valid record after %X/%X", 
						LSN_FORMAT_ARGS(reader_private->startptr)));

	if (first_record != reader_private->startptr && 
		XLogSegmentOffset(reader_private->startptr, DEFAULT_XLOG_SEG_SIZE) != 0)
		ereport(LOG, 
				errmsg("WALDIFF: skipping over %u bytes", (uint32) (first_record - reader_private->startptr)));
	
	for (;;)
	{
		WALDIFFRecord  WDRec;
		XLogRecord 	  *WALRec;
		char 		  *errormsg;

		WALRec = XLogReadRecord(reader_state, &errormsg);
		if (WALRec == InvalidXLogRecPtr) {
			if (reader_private->endptr_reached)
				break;

            ereport(ERROR, 
					errmsg("WALDIFF: XLogReadRecord failed to read WAL record: %s", errormsg));
        }

		if (WALRec->xl_rmid == RM_XLOG_ID)
		{
			if ((WALRec->xl_info & XLR_RMGR_INFO_MASK) == XLOG_CHECKPOINT_SHUTDOWN ||
				(WALRec->xl_info & XLR_RMGR_INFO_MASK) == XLOG_CHECKPOINT_ONLINE)
			{
				last_checkpoint = reader_state->currRecPtr;
			}
			else if ((WALRec->xl_info & XLR_RMGR_INFO_MASK) == XLOG_SWITCH)
				ereport(LOG, errmsg("WALDIFF: meet SWITCH record at position %X/%X", LSN_FORMAT_ARGS(reader_state->currRecPtr)));
		}

		/* Now we reckon that hash_table contains only HEAP records (INSERT, HOT UPDATE)
		   without image */
		/* firstly check in hash map*/
		if (WALRec->xl_rmid == RM_HEAP_ID && 
			!XLogRecHasBlockImage(reader_state, 0) &&
		   	((WALRec->xl_info & XLOG_HEAP_OPMASK) == XLOG_HEAP_INSERT || 
			(WALRec->xl_info & XLOG_HEAP_OPMASK) == XLOG_HEAP_HOT_UPDATE))
		{
			uint32_t hash_key;
			HTABEntry *entry;
			bool is_found;

			hash_key = GetHashKeyOfWALRecord(reader_state->record);
			
			entry = (HTABEntry *) hash_search(hash_table, (void*) &hash_key, HASH_FIND, &is_found);

			if (entry && is_found)
			{
				XLogRecord *construcred_record;
				int written_bytes;

				WDRec = entry->data;
				Assert(WDRec != NULL);

				/* construct WALDIFF */
				// construcred_record = constructWALDIFF(WDRec);

				// write WALDIFF record
				// TODO need writer

				// written_bytes = FileWrite(writer_state->seg.ws_file, construcred_record, construcred_record->xl_tot_len, writer_state->EndRecPtr % DEFAULT_XLOG_SEG_SIZE, WAIT_EVENT_WAL_COPY_WRITE);
				// if (written_bytes != construcred_record->xl_tot_len)
				// {
				// 	if (written_bytes < 0)
				// 		ereport(PANIC,
				// 				(errcode_for_file_access(),
				// 				errmsg("could not write WALDIFF: %m")));
				// 	else
				// 		ereport(PANIC,
				// 				(errcode(ERRCODE_DATA_CORRUPTED),
				// 				errmsg("could not write WALDIFF: write %d of %zu",
				// 						written_bytes, construcred_record->xl_tot_len)));
				// }
			}
			continue;
		}
		else 
		{
			// write regular WAL record
			// TODO need writer

			// use this to find raw record from reader_state
			//reader_state->readBuf + reader_state->currRecPtr - reader_state->latestPagePtr;
		}
	}
}

/*
 * walldiff_shutdown
 *
 * Frees all allocated resources.
 */
void 
waldiff_shutdown(ArchiveModuleState *reader)
{
	/* Must have 'case this function closes fd */
	XLogReaderFree(reader_state);
	// if (writer_state->seg.ws_file != -1)
		// FileClose(writer_state->seg.ws_file);
	// pfree(writer_state);

	//hash_destroy(hash_table);

	MemoryContextSwitchTo(memory_context_storage->old);
	Assert(CurrentMemoryContext != memory_context_storage->current);
	MemoryContextDelete(memory_context_storage->current);

#ifdef WALDIFF_DEBUG
	ereport(LOG, errmsg("WALDIFF: waldiff_shutdown"));
#endif
}

static void  
fetch_insert(WALDIFFRecord *WDRec)
{
	DecodedXLogRecord 		*decoded_record = reader_state->record;
	RelFileLocator 			 rel_file_locator;
	ForkNumber 				 forknum;
	BlockNumber 			 blknum;
	xl_heap_insert 			*main_data;
	XLogRecordBlockHeader 	 block_hdr;
	char 					*block_data;
	Size					 block_data_len;
	
	/* HEAP_INSERT contains one block */
	Assert(decoded_record->max_block_id == 0);
	
	// MemoryContextStats(memory_context_storage->current);

	*WDRec = palloc(SizeOfWALDIFFRecord + sizeof(WALDIFFBlock) * (decoded_record->max_block_id + 1));
	Assert(*WDRec != NULL);

	XLogRecGetBlockTag(reader_state, 0, &rel_file_locator, &forknum, &blknum);
	main_data = (xl_heap_insert *) XLogRecGetData(reader_state);
	block_data = XLogRecGetBlockData(reader_state, 0, &block_data_len);
	block_hdr.id = 0;
	block_hdr.fork_flags = decoded_record->blocks[0].flags;
	block_hdr.data_length = block_data_len;

	Assert(XLogRecHasBlockData(reader_state, 0));
	
	(*WDRec)->type = XLOG_HEAP_INSERT;
	(*WDRec)->lsn = decoded_record->lsn;
	(*WDRec)->rec_hdr = decoded_record->header;
	(*WDRec)->t_xmin = XLogRecGetXid(reader_state);
	(*WDRec)->t_xmax = 0;
	/* 
	 * Copy tuple's version pointers
	 * At this step, t_ctid always will be point to itself,
	 * because we reckon this record as first
	 */
	ItemPointerSetBlockNumber(&((*WDRec)->current_t_ctid), blknum);
    ItemPointerSetOffsetNumber(&((*WDRec)->current_t_ctid), main_data->offnum);
	(*WDRec)->prev_t_ctid = (*WDRec)->current_t_ctid;
	/* Copy main data */
	(*WDRec)->main_data = palloc(SizeOfHeapInsert);
	memcpy((*WDRec)->main_data, main_data, SizeOfHeapInsert);
	(*WDRec)->main_data_len = SizeOfHeapInsert;
	/* Copy 0th block */
	(*WDRec)->max_block_id 				= 0;
	(*WDRec)->blocks[0].blk_hdr 		= block_hdr;
	(*WDRec)->blocks[0].file_loc 		= rel_file_locator;
	(*WDRec)->blocks[0].forknum			= forknum;
	(*WDRec)->blocks[0].blknum 			= blknum;
	(*WDRec)->blocks[0].has_data 		= true;
	(*WDRec)->blocks[0].block_data_len  = block_data_len;
	(*WDRec)->blocks[0].block_data 		= palloc0(block_data_len);
	memcpy((*WDRec)->blocks[0].block_data, block_data, block_data_len);

	(*WDRec)->chain_length = 0;
}

/*
 * fetch_hot_update
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
 */
static void 
fetch_hot_update(WALDIFFRecord *WDRec)
{
	DecodedXLogRecord 		*decoded_record = reader_state->record;
	RelFileLocator 			 rel_file_locator;
	ForkNumber 				 forknum;
	BlockNumber 			 blknum;
	xl_heap_update 			*main_data;
	XLogRecordBlockHeader 	 block_hdr;
	char 					*block_data;
	Size					 block_data_len;

	/* HEAP_UPDATE_HOT contains one block */
	Assert(decoded_record->max_block_id == 0);

	*WDRec = palloc0(SizeOfWALDIFFRecord + sizeof(WALDIFFBlock) * (decoded_record->max_block_id + 1));
	Assert(*WDRec != NULL);

	XLogRecGetBlockTag(reader_state, 0, &rel_file_locator, &forknum, &blknum);
	main_data = (xl_heap_update *) XLogRecGetData(reader_state);
	block_data = XLogRecGetBlockData(reader_state, 0, &block_data_len);
	block_hdr.id = 0;
	block_hdr.fork_flags = decoded_record->blocks[0].flags;
	block_hdr.data_length = block_data_len;
	Assert(XLogRecHasBlockData(reader_state, 0));

	(*WDRec)->type = XLOG_HEAP_HOT_UPDATE;
	(*WDRec)->lsn = decoded_record->lsn;
	(*WDRec)->rec_hdr = decoded_record->header;
	(*WDRec)->t_xmin = main_data->old_xmax;
	(*WDRec)->t_xmax = main_data->new_xmax;
	/* 
	 * Copy tuple's version pointers
	 * At this step, t_ctid always will be point to itself,
	 * because we reckon this record as first
	 */
	ItemPointerSetBlockNumber(&((*WDRec)->current_t_ctid), blknum);
    ItemPointerSetOffsetNumber(&((*WDRec)->current_t_ctid), main_data->new_offnum);
	ItemPointerSetBlockNumber(&((*WDRec)->prev_t_ctid), blknum);
    ItemPointerSetOffsetNumber(&((*WDRec)->prev_t_ctid), main_data->old_offnum);
	/* Copy main data */
	(*WDRec)->main_data = palloc0(SizeOfHeapUpdate);
	memcpy((*WDRec)->main_data, main_data, SizeOfHeapUpdate);
	(*WDRec)->main_data_len = SizeOfHeapUpdate;
	/* Copy 0th block */
	(*WDRec)->max_block_id 				= 0;
	(*WDRec)->blocks[0].blk_hdr 		= block_hdr;
	(*WDRec)->blocks[0].file_loc 		= rel_file_locator;
	(*WDRec)->blocks[0].forknum			= forknum;
	(*WDRec)->blocks[0].blknum 			= blknum;
	(*WDRec)->blocks[0].has_data 		= true;
	(*WDRec)->blocks[0].block_data_len 	= block_data_len;
	(*WDRec)->blocks[0].block_data 		= palloc0(block_data_len);
	memcpy((*WDRec)->blocks[0].block_data, block_data, block_data_len);

	(*WDRec)->chain_length = 0;
}

static void free_waldiff_record(WALDIFFRecord record)
{
	for (int i = 0; i < record->max_block_id; i++)
	{
		Assert(record->blocks[i].block_data);
		pfree(record->blocks[i].block_data);
	}
	Assert(record->main_data);
	pfree(record->main_data);
	Assert(record);
	pfree(record);
}

static void
get_old_tuple(HeapTupleData *old_tuple, Relation relation, 
			  BlockNumber blocknum, OffsetNumber offnum)
{
	Buffer buffer;
	Page page;
	ItemId lp;

	buffer = ReadBuffer(relation, blocknum);
	page = BufferGetPage(buffer);
	if (PageGetMaxOffsetNumber(page) >= offnum)
		lp = PageGetItemId(page, offnum);

	if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp))
		elog(PANIC, "invalid lp");

	old_tuple->t_data = (HeapTupleHeader) PageGetItem(page, lp);
	old_tuple->t_len = ItemIdGetLength(lp);
}

static void
overlay_suffix_and_prefix_from_old(WALDIFFRecord WDRec, Relation relation,
								   TupleDesc tuple_desc, xl_heap_header **heap_hdr,
								   char **block_tuple, Size *block_tuple_len)
{
	uint16 			 prev_prefix_len = 0, prev_suffix_len = 0;
					//  curr_prefix_len = 0, curr_suffix_len = 0;

	xl_heap_update *main_data = (xl_heap_update *) WDRec->main_data;

	HeapTupleData old_tuple;
	char *overlayed_tuple;
	off_t offset = 0;
	Size  overlayed_tuple_len = WDRec->blocks[0].block_data_len;

	old_tuple.t_data = NULL;
	old_tuple.t_len = 0;

	if (main_data->flags & XLH_UPDATE_PREFIX_FROM_OLD)
	{
		prev_prefix_len = *((uint16 *) *heap_hdr);
		*heap_hdr = (xl_heap_header *) ((char *) *heap_hdr + sizeof(uint16));
		*block_tuple_len -= sizeof(uint16);
		overlayed_tuple_len -= sizeof(uint16);
		overlayed_tuple_len += prev_prefix_len;

		if (old_tuple.t_data == NULL)
			get_old_tuple(&old_tuple, relation, 
							WDRec->blocks[0].blknum, 
							main_data->old_offnum);
	}
	if ((main_data)->flags & XLH_UPDATE_SUFFIX_FROM_OLD)
	{
		prev_suffix_len = *((uint16 *) *heap_hdr);
		*heap_hdr = (xl_heap_header *) ((char *) *heap_hdr + sizeof(uint16));
		*block_tuple_len -= sizeof(uint16);
		overlayed_tuple_len -= sizeof(uint16);
		overlayed_tuple_len += prev_suffix_len;

		if (old_tuple.t_data == NULL)
			get_old_tuple(&old_tuple, relation, 
							WDRec->blocks[0].blknum, 
							main_data->old_offnum);
	}

	if (old_tuple.t_data != NULL) {
		overlayed_tuple = palloc(overlayed_tuple_len);

		memcpy(overlayed_tuple, *heap_hdr, SizeOfHeapHeader);
		offset += SizeOfHeapHeader;

		/* copy bitmap [+ padding] [+ oid] from WAL record */
		memcpy(overlayed_tuple + offset, *block_tuple, (*heap_hdr)->t_hoff - SizeofHeapTupleHeader);
		offset += (*heap_hdr)->t_hoff - SizeofHeapTupleHeader;

		if (prev_prefix_len > 0)
		{
			/* copy prefix from old tuple */
			memcpy(overlayed_tuple + offset, (char *) old_tuple.t_data + old_tuple.t_data->t_hoff, prev_prefix_len);
			offset += prev_prefix_len;
		}

		/* copy new tuple data from WAL record */
		memcpy(overlayed_tuple + offset, 
				(char *) *block_tuple + SizeOfHeapHeader + MAXALIGN(BITMAPLEN(tuple_desc->natts)), 
				*block_tuple_len - ((*heap_hdr)->t_hoff - SizeofHeapTupleHeader));
		offset += *block_tuple_len - ((*heap_hdr)->t_hoff - SizeofHeapTupleHeader);

		/* copy suffix from old tuple */
		if (prev_suffix_len > 0)
			memcpy(overlayed_tuple + offset, (char *) old_tuple.t_data + old_tuple.t_len - prev_suffix_len, prev_suffix_len);
	}
}

static Size
overlay_tuple(TupleDesc tuple_desc, HeapTuple prev_tuple, HeapTuple curr_tuple) 
{
	int			  numberOfAttributes = tuple_desc->natts;

	static Datum *prev_tuple_values = NULL;
	static bool  *prev_tuple_nulls = NULL;

	static Datum *curr_tuple_values = NULL;
	static bool  *curr_tuple_nulls = NULL;
	static bool  *curr_tuple_replace = NULL;
	
	if (prev_tuple_values == NULL)
		prev_tuple_values = palloc(numberOfAttributes * sizeof(Datum));
	if (prev_tuple_nulls == NULL)
		prev_tuple_nulls = palloc(numberOfAttributes * sizeof(bool));

	if (curr_tuple_values == NULL)
		curr_tuple_values = palloc(numberOfAttributes * sizeof(Datum));
	if (curr_tuple_nulls == NULL)
		curr_tuple_nulls = palloc(numberOfAttributes * sizeof(bool));
	if (curr_tuple_replace == NULL)
		curr_tuple_replace = palloc(numberOfAttributes * sizeof(bool));
	MemSet((char *) curr_tuple_replace, true, numberOfAttributes * sizeof(bool));

	heap_deform_tuple(prev_tuple, tuple_desc, prev_tuple_values, prev_tuple_nulls);
	heap_deform_tuple(curr_tuple, tuple_desc, curr_tuple_values, curr_tuple_nulls);

	for (int attoff = 0; attoff < numberOfAttributes; attoff++)
	{
		if (curr_tuple_replace[attoff])
		{
			prev_tuple_values[attoff] = curr_tuple_values[attoff];
			prev_tuple_nulls[attoff] = curr_tuple_nulls[attoff];
		}
	}

	prev_tuple = heap_form_tuple(tuple_desc, prev_tuple_values, prev_tuple_nulls);
	return heap_compute_data_size(tuple_desc, prev_tuple_values, prev_tuple_nulls);
}

/* 
 * After this function, curr_tup can be deallocated (if return value is 0)
 *
 * insert block data = xl_heap_header + t_bits + padding + oid + tuple
 * update block data = prefix + suffix + xl_heap_header + t_bits + padding + oid + tuple
 */
static int
overlay_hot_update(WALDIFFRecord prev_record, WALDIFFRecord curr_record)
{
	xl_heap_header 	*prev_heap_hdr, 
					*curr_heap_hdr;

	char 			*prev_block_tuple, 
					*curr_block_tuple;

	Size 			 prev_block_tuple_len, 
					 curr_block_tuple_len;

	char 			*prev_main_data,
					*curr_main_data;	

	char 		 *new_insert_block_data; // = xl_heap_hdr + bitmap + padding + (oid) + tuple
	Size  		  new_insert_block_data_len = 0;

	Oid 		  spcOid = prev_record->blocks[0].file_loc.spcOid;
	RelFileNumber relNumber = prev_record->blocks[0].file_loc.relNumber;
	Oid 		  rel_oid;
	Relation 	  relation;
	TupleDesc 	  tuple_desc;

	static HeapTuple prev_tuple = NULL;
	static HeapTuple curr_tuple = NULL;

	Assert(prev_record->type == XLOG_HEAP_INSERT || prev_record->type == XLOG_HEAP_HOT_UPDATE);
	Assert(curr_record->type == XLOG_HEAP_HOT_UPDATE);
	Assert(prev_record->max_block_id == 0);
	Assert(curr_record->max_block_id == 0);
	Assert(prev_record->blocks[0].has_data);
	Assert(curr_record->blocks[0].has_data);

	if (prev_tuple == NULL)
		prev_tuple = palloc(HEAPTUPLESIZE);
	if (curr_tuple == NULL)
		curr_tuple = palloc(HEAPTUPLESIZE);

	ereport(LOG, errmsg("MyDatabaseId = %d", MyDatabaseId));
	MyDatabaseId = prev_record->blocks[0].file_loc.dbOid;

#ifdef WALDIFF_DEBUG
	ereport(LOG, errmsg("WALDIFF: overlaying HEAP HOT UPDATE record: dbOid = %d, spaceOid = %d and relNum = %d", 
						MyDatabaseId,
						spcOid, 
						relNumber));
#endif		

	Assert(!IsTransactionState());
	StartTransactionCommand();

	Assert(RelFileNumberIsValid(relNumber));
	rel_oid = RelidByRelfilenumber(spcOid, relNumber);

	CommitTransactionCommand();
	Assert(OidIsValid(rel_oid));

#ifdef WALDIFF_DEBUG
	ereport(LOG, errmsg("WALDIFF: relation oid = %d", rel_oid));
#endif	

	// getting relation can be done only inside transaction
	
	relation = RelationIdGetRelation(rel_oid);

	Assert(relation != NULL);

#ifdef WALDIFF_DEBUG
	ereport(LOG, errmsg("WALDIFF: got relation"));
#endif	

	MyDatabaseId = 0;

	tuple_desc = RelationGetDescr(relation);

	Assert(tuple_desc != NULL);

#ifdef WALDIFF_DEBUG
	ereport(LOG, errmsg("WALDIFF: got tuple desc"));
#endif	

	prev_main_data = prev_record->main_data;
	curr_main_data = curr_record->main_data;

	prev_heap_hdr = (xl_heap_header *) prev_record->blocks[0].block_data;
	curr_heap_hdr = (xl_heap_header *) curr_record->blocks[0].block_data;

	prev_block_tuple = (char *) prev_heap_hdr + SizeOfHeapHeader;
	curr_block_tuple = (char *) curr_heap_hdr + SizeOfHeapHeader;

	prev_block_tuple_len = prev_record->blocks[0].block_data_len - SizeOfHeapHeader;
	curr_block_tuple_len = curr_record->blocks[0].block_data_len - SizeOfHeapHeader;

#ifdef WALDIFF_DEBUG
	ereport(LOG, errmsg("WALDIFF: checking for suffix and prefix"));
#endif	
	if (prev_record->type == XLOG_HEAP_HOT_UPDATE) 
	{
		// before overlaying tuples get full tuple data if there is prefix or/and suffix from the old one
		overlay_suffix_and_prefix_from_old(
			prev_record, relation,
			tuple_desc, &prev_heap_hdr,
			&prev_block_tuple, &prev_block_tuple_len);

	}

	// before overlaying tuples get full tuple data if there is prefix or/and suffix from the old one
	overlay_suffix_and_prefix_from_old(
			curr_record, relation,
			tuple_desc, &curr_heap_hdr,
			&curr_block_tuple, &curr_block_tuple_len);

	Assert((prev_heap_hdr->t_infomask2 & HEAP_NATTS_MASK) == 
		   (curr_heap_hdr->t_infomask2 & HEAP_NATTS_MASK));


	prev_tuple->t_data = palloc(SizeofHeapTupleHeader + prev_block_tuple_len);
	curr_tuple->t_data = palloc(SizeofHeapTupleHeader + curr_block_tuple_len);

	memcpy((char *) prev_tuple->t_data + SizeofHeapTupleHeader,
			prev_tuple,
			prev_block_tuple_len);
	memcpy((char *) curr_tuple->t_data + SizeofHeapTupleHeader,
			curr_tuple,
			curr_block_tuple_len);

	((HeapTupleData *) prev_tuple)->t_data->t_infomask2 = prev_heap_hdr->t_infomask2;
	((HeapTupleData *) prev_tuple)->t_data->t_infomask = prev_heap_hdr->t_infomask;
	((HeapTupleData *) prev_tuple)->t_data->t_hoff = prev_heap_hdr->t_hoff;

	((HeapTupleData *) curr_tuple)->t_data->t_infomask2 = curr_heap_hdr->t_infomask2;
	((HeapTupleData *) curr_tuple)->t_data->t_infomask = curr_heap_hdr->t_infomask;
	((HeapTupleData *) curr_tuple)->t_data->t_hoff = curr_heap_hdr->t_hoff;

	new_insert_block_data_len = SizeOfHeapHeader + MAXALIGN(BITMAPLEN(tuple_desc->natts));
	new_insert_block_data_len += overlay_tuple(tuple_desc, prev_tuple, curr_tuple);
	new_insert_block_data = palloc(new_insert_block_data_len);

	memcpy(new_insert_block_data, prev_heap_hdr, SizeOfHeapHeader);
	memcpy((char *) new_insert_block_data + SizeOfHeapHeader, curr_tuple->t_data->t_bits, new_insert_block_data_len - SizeOfHeapHeader);

	pfree(prev_record->blocks[0].block_data);
	prev_record->blocks[0].block_data = new_insert_block_data;

	// Todo check what happens with t_hoff with different situations

	/* 
	 * 1. prev_record = insert
	 *	  curr_record = hot update
	 */
	if (prev_record->type == XLOG_HEAP_INSERT)
	{
		// prev_heap_hdr->t_infomask = ;
		// prev_heap_hdr->t_infomask2 = ;
		// prev_heap_hdr->t_hoff = MAXALIGN(SizeOfHeapHeader + BITMAPLEN(prev_tuple_attr_num));
	
		/* main data */
		//((xl_heap_insert *)prev_main_data)->flags = ;
		((xl_heap_insert *) prev_main_data)->offnum = ((xl_heap_update *)curr_main_data)->new_offnum;
	}

	/* 
	 * 2. prev_record = hot update
	 *	  curr_record = hot update
	 */
	else if (prev_record->type == XLOG_HEAP_HOT_UPDATE)
	{
		((xl_heap_update *) prev_main_data)->new_offnum = ((xl_heap_update *)curr_main_data)->new_offnum;
		((xl_heap_update *) prev_main_data)->new_xmax = ((xl_heap_update *)curr_main_data)->new_xmax;
	}

	return 0;
}

/*
 * constructWALDIFF
 * 
 * Creates WALDIFF records according to data in hash table
 * 
 */
XLogRecord * 
constructWALDIFF(WALDIFFRecord WDRec)
{
	XLogRecord *constructed_record = palloc0(WDRec->rec_hdr.xl_tot_len);
	off_t 		curr_off = 0;
	pg_crc32c	crc;

	Assert(WDRec->rec_hdr.xl_rmid == RM_HEAP_ID && 
		   (WDRec->type == XLOG_HEAP_INSERT || 
		   WDRec->type == XLOG_HEAP_HOT_UPDATE));

	/* XLogRecord */
	memcpy(constructed_record, &(WDRec->rec_hdr), SizeOfXLogRecord);
	curr_off += SizeOfXLogRecord;

	/* XLogRecordBlockHeader */
	Assert(WDRec->max_block_id == 0);
	memcpy(constructed_record + curr_off, &(WDRec->blocks[0].blk_hdr), SizeOfXLogRecordBlockHeader);
	curr_off += SizeOfXLogRecordBlockHeader;

	/* RelFileLocator */
	if (!(WDRec->blocks[0].blk_hdr.fork_flags & BKPBLOCK_SAME_REL))
	{
		memcpy(constructed_record + curr_off, &(WDRec->blocks[0].file_loc), sizeof(RelFileLocator));
		curr_off += sizeof(RelFileLocator);
	}

	/* BlockNumber */
	memcpy(constructed_record + curr_off, &(WDRec->blocks[0].blknum), sizeof(BlockNumber));

	/* XLogRecordDataHeader[Short|Long] */
	if (WDRec->main_data_len < 256)
	{
		XLogRecordDataHeaderShort main_data_hdr = {XLR_BLOCK_ID_DATA_SHORT, WDRec->main_data_len};

		memcpy(constructed_record + curr_off, &main_data_hdr, SizeOfXLogRecordDataHeaderShort);
		curr_off += SizeOfXLogRecordDataHeaderShort;
	}
	else
	{
		XLogRecordDataHeaderLong main_data_hdr_long = {XLR_BLOCK_ID_DATA_LONG};

		memcpy(constructed_record + curr_off, &main_data_hdr_long, sizeof(uint8));
		curr_off += sizeof(uint8);
		memcpy(constructed_record + curr_off, &(WDRec->main_data_len), sizeof(uint32));
		curr_off += sizeof(uint32);
	}

	/* main data */
	memcpy(constructed_record + curr_off, WDRec->main_data, WDRec->main_data_len);
	curr_off += WDRec->main_data_len;

	Assert(WDRec->rec_hdr.xl_tot_len == curr_off);

	/* calculate CRC */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, ((char *) constructed_record) + SizeOfXLogRecord, constructed_record->xl_tot_len - SizeOfXLogRecord);
	COMP_CRC32C(crc, (char *) constructed_record, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(crc);
	constructed_record->xl_crc = crc;

	return constructed_record;
}

static void 
ReadControlFile(void)
{
	int			fd;
	int			read_bytes;

	fd = open(XLOG_CONTROL_FILE, O_RDWR | PG_BINARY);
	if (fd < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("WALDIFF: could not open file \"%s\": %m",
						XLOG_CONTROL_FILE)));

	pgstat_report_wait_start(WAIT_EVENT_CONTROL_FILE_READ);
	read_bytes = pg_pread(fd, ControlFile, sizeof(ControlFileData), 0);
	pgstat_report_wait_end();

	if (read_bytes != sizeof(ControlFileData))
	{
		if (read_bytes < 0)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("WALDIFF: could not read file \"%s\": %m",
							XLOG_CONTROL_FILE)));
		else
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("WALDIFF: could not read file \"%s\": read %d of %zu",
							XLOG_CONTROL_FILE, read_bytes, sizeof(ControlFileData))));
	}

	close(fd);
}

static void 
WriteControlFile(void)
{
	int			fd;
	int			written_bytes;

	fd = open(XLOG_CONTROL_FILE, O_RDWR | PG_BINARY);
	if (fd < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("WALDIFF: could not open file \"%s\": %m",
						XLOG_CONTROL_FILE)));

	pgstat_report_wait_start(WAIT_EVENT_CONTROL_FILE_WRITE_UPDATE);
	written_bytes = pg_pwrite(fd, ControlFile, sizeof(ControlFileData), 0);
	pgstat_report_wait_end();

	if (written_bytes != sizeof(ControlFileData))
	{
		if (written_bytes < 0)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("WALDIFF: could not write to file \"%s\": %m",
							XLOG_CONTROL_FILE)));
		else
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("WALDIFF: could not write to file \"%s\": write %d of %zu",
							XLOG_CONTROL_FILE, written_bytes, sizeof(ControlFileData))));
	}

	close(fd);
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
				errmsg("WALDIFF: error during reading WAL absolute path : %s/%s", reader->segcxt.ws_dir, fname));

	reader->seg.ws_file = OpenTransientFile(fpath, PG_BINARY | O_RDONLY);
	if (reader->seg.ws_file == -1)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("WALDIFF: could not open WAL segment \"%s\": %m", fpath)));
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
					errmsg("WALDIFF: could not read from file %s, offset %d: %m",
					fname, errinfo.wre_off));
		}
		else
			ereport(ERROR,
					errmsg("WALDIFF: could not read from file %s, offset %d: read %d of %d",
					fname, errinfo.wre_off, errinfo.wre_read,
					errinfo.wre_req));
	}

	return count;
}

void 
WalCloseSegment(XLogReaderState *reader)
{
	if (reader->seg.ws_file != -1)
		close(reader->seg.ws_file);
	reader->seg.ws_file = -1;
}