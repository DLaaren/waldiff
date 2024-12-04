#include "waldiff.h"

PG_MODULE_MAGIC;

/*--------------------------Pivate defines-------------------------*/
typedef struct HTABEntry
{
	uint32_t 	  key;
	WaldiffRecord data;
} HTABEntry;

typedef struct MemoryContextStorage
{
	MemoryContext old;
	MemoryContext current;
} MemoryContextStorage;

/* GUC value's store */
static char *waldiff_dir;

/* Global data */
static MemoryContextStorage *memory_context_storage;
static WaldiffWriter	    *writer;
static WaldiffReader	    *reader;
static HTAB 			    *hash_table;
static int 					 ControlFileFd = -1;

/* We maintain an image of pg_control in shared memory */
static ControlFileData ControlFile;

/*-----------------------Forward declarations----------------------*/
void waldiff_startup(ArchiveModuleState *reader);
bool waldiff_configured(ArchiveModuleState *reader);
bool waldiff_archive(ArchiveModuleState *reader, const char *file, const char *path);
void waldiff_shutdown(ArchiveModuleState *reader);

static void collecting_chains(void);
static void constructing_waldiff(XLogRecPtr *last_checkpoint);

static void ReadControlFile(void);
static void WriteControlFile(void);

static void free_waldiff_record(WaldiffRecord record);


// TODO
// /* This three fuctions returns palloced struct */
// static void fetch_insert(WaldiffRecord *WaldiffRec);
// static void fetch_hot_update(WaldiffRecord *WaldiffRec);

// static int overlay_hot_update(WaldiffRecord prev_tup, WaldiffRecord curr_tup);
// static XLogRecord *constructWALDIFF(WaldiffRecord WaldiffRec);

// /* Helper functions */
// static void free_waldiff_record(WaldiffRecord record);


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

static const ArchiveModuleCallbacks waldiff_callbacks = {
    .startup_cb 		 = waldiff_startup,
	.check_configured_cb = waldiff_configured,
	.archive_file_cb 	 = waldiff_archive,
	.shutdown_cb 		 = waldiff_shutdown
};

/* Returns the module's archiving callbacks */
const ArchiveModuleCallbacks *
_PG_archive_module_init(void)
{
	return &waldiff_callbacks;
}

/* Creates the module's memory context */
void 
waldiff_startup(ArchiveModuleState *state)
{
	/* The value should be a power of 2 */
	HASHCTL hash_ctl;
	enum {hash_table_initial_size = 128};

	memory_context_storage = 
		(MemoryContextStorage *) MemoryContextAllocZero(TopMemoryContext, 
														sizeof(MemoryContextStorage));
	Assert(memory_context_storage != NULL);

	memory_context_storage->current = AllocSetContextCreate(TopMemoryContext,
										                    "waldiff_memory_context",
										                    ALLOCSET_DEFAULT_SIZES);
    Assert(memory_context_storage->current != NULL);

	memory_context_storage->old = MemoryContextSwitchTo(memory_context_storage->current);    
	Assert(memory_context_storage->old != NULL);

    hash_ctl.keysize    = sizeof(uint32_t);
	hash_ctl.entrysize 	= sizeof(HTABEntry);
	hash_ctl.hash 		= &tag_hash;
	/* It is said hash table must have its own memory context */
	hash_ctl.hcxt = memory_context_storage->current; //AllocSetContextCreate(memory_context_storage->current,
										//   "WALDIFF_HTAB",
										//   ALLOCSET_DEFAULT_SIZES);  
	Assert(hash_ctl.hcxt != NULL);								  

    hash_table = hash_create("WaldiffHashTable", hash_table_initial_size,
                             &hash_ctl, HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);
	Assert(hash_table != NULL);

	writer = WaldiffWriterAllocate(waldiff_dir, DEFAULT_XLOG_SEG_SIZE);
	Assert(writer != NULL);
	reader = WaldiffReaderAllocate(XLOGDIR, DEFAULT_XLOG_SEG_SIZE);
	Assert(reader != NULL);

	ControlFileFd = open(XLOG_CONTROL_FILE, O_RDONLY | PG_BINARY);
	if (ControlFileFd < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("WALDIFF: could not open file \"%s\": %m",
						XLOG_CONTROL_FILE)));
}

bool 
waldiff_configured(ArchiveModuleState *state)
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


/* WalFile - just name of the WAL file 
 * WalPath - the full path including the WAL file name */
bool 
waldiff_archive(ArchiveModuleState *state, const char *WalFile, const char *WalPath)
{
	TimeLineID tli;
	XLogSegNo  segNo;
	XLogRecPtr start_lsn;
	XLogRecPtr last_checkpoint = InvalidXLogRecPtr;

	ereport(LOG, errmsg("WALDIFF: archiving WAL file: %s", WalPath));

	XLogFromFileName(WalFile, &tli, &segNo, DEFAULT_XLOG_SEG_SIZE);
	XLogSegNoOffsetToRecPtr(segNo, 0, DEFAULT_XLOG_SEG_SIZE, start_lsn);
	Assert(start_lsn != InvalidXLogRecPtr);

	ereport(LOG, errmsg("WALDIFF: segNo: %lu; tli: %u; start_lsn: %08X/%08X;",
						segNo, tli, LSN_FORMAT_ARGS(start_lsn)));

	LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
	ReadControlFile();
	LWLockRelease(ControlFileLock);

	WaldiffBeginReading(reader, ControlFile.system_identifier, segNo, tli);
	if (DoesWaldiffWriterFinishedSegment(writer))
		WaldiffBeginWriting(writer, ControlFile.system_identifier, segNo, tli);

	ereport(LOG, errmsg("WALDIFF: first passage"));
	/* Reading with decoding, also filling the HTAB with potential WALDIFF records */
	collecting_chains();

	return true;

	ereport(LOG, errmsg("WALDIFF: second passage"));
	/* Reading with writing constructed WALDIFF records according to the HTAB;
	   Also remember the last checkpoint lsn to update info in ControlFile */
	constructing_waldiff(&last_checkpoint);

	if (last_checkpoint != InvalidXLogRecPtr) 
	{
		ereport(LOG, errmsg("WALDIFF: write new last checkpoint location : %X/%X", 
							LSN_FORMAT_ARGS(last_checkpoint)));

		LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
		ReadControlFile();

		ControlFile.checkPoint = ControlFile.checkPointCopy.redo = last_checkpoint;

		INIT_CRC32C(ControlFile.crc);
		COMP_CRC32C(ControlFile.crc,
					(void *) &ControlFile,
					offsetof(ControlFileData, crc));
		FIN_CRC32C(ControlFile.crc);

		WriteControlFile();
		LWLockRelease(ControlFileLock);
	}

	ereport(LOG, errmsg("WALDIFF: archived WAL file: %s", WalPath));

	return true;
}

static void 
collecting_chains(void)
{
	for (;;)
	{
		XLogRecord 	  *WalRec;
		// WaldiffRecord  WaldiffRec;

		WalRec = WaldiffReaderRead(reader);
		if (WalRec == NULL) 
			break;
            // ereport(ERROR, 
					// errmsg("WALDIFF: WaldiffReaderRead failed to read WAL record"));

		WaldiffWriterWrite(writer, WalRec);

		pfree(WalRec);


		// ereport(FATAL, errmsg("idi nahuy"));

// 		/* Now we're processing only several HEAP type WAL records and without image */
// 		if (XLogRecGetRmid(reader_state) == RM_HEAP_ID && !XLogRecHasBlockImage(reader_state, 0))
// 		{
// 			uint32_t prev_hash_key;
// 			uint32_t hash_key;
// 			HTABEntry *entry;
// 			bool is_found;

// 			switch(XLogRecGetInfo(reader_state) & XLOG_HEAP_OPMASK)
// 			{
// 				case XLOG_HEAP_INSERT:
// 				{
// 					fetch_insert(&WaldiffRec);
// 					if (WaldiffRec == NULL)
// 						ereport(ERROR, errmsg("WALDIFF: fetch_insert failed"));

// 					hash_key = GetHashKeyOfWALDIFFRecord(WaldiffRec);
// 					hash_search(hash_table, (void*) &hash_key, HASH_FIND, &is_found);
// 					if (is_found)
// 						ereport(ERROR, errmsg("WALDIFF: found HTAB entry that shouldn't be there"));

// 					entry = (HTABEntry *) hash_search(hash_table, (void*) &hash_key, HASH_ENTER, NULL);
// 					entry->data = WaldiffRec;

// 					Assert(entry->key == hash_key);

// #ifdef WALDIFF_DEBUG
// 					// ereport(LOG, errmsg("WALDIFF: HEAP INSERT record lsn = %X/%X; hash_key = %u; blknum = %d; offnum = %u", 
// 										// LSN_FORMAT_ARGS(entry->data->lsn), hash_key, entry->data->blocks[0].blknum, ((xl_heap_insert *)(entry->data->main_data))->offnum));
// #endif				
// 					break;
// 				}

// 				case XLOG_HEAP_HOT_UPDATE:
// 				{
// 					fetch_hot_update(&WaldiffRec);
// 					if (WaldiffRec == NULL)
// 						ereport(ERROR, errmsg("WALDIFF: fetch_hot_update failed"));

// 					prev_hash_key = GetHashKeyOfPrevWALDIFFRecord(WaldiffRec);
// 					hash_key = GetHashKeyOfWALDIFFRecord(WaldiffRec);

// 					entry = (HTABEntry *) hash_search(hash_table, (void*) &prev_hash_key, HASH_FIND, &is_found);
// 					if (is_found)
// 					{
// 						int overlay_result;

// #ifdef WALDIFF_DEBUG
// 						// ereport(LOG, errmsg("WALDIFF: HEAP HOT UPDATE record has previous record"));
// #endif		

// 						//overlay_result = overlay_hot_update(entry->data, WaldiffRec);

// 						overlay_result = 0;

// 						if (overlay_result == -1)
// 						{
// 							/*
// 							 * Overlaying failed - we must store both records, the previous one is already in the HTAB
// 							 */
// 							entry = (HTABEntry *) hash_search(hash_table, (void*) &hash_key, HASH_ENTER, NULL);
// 							entry->data = WaldiffRec;

// #ifdef WALDIFF_DEBUG
// 							ereport(LOG, errmsg("WALDIFF: cannot overlay HEAP HOT UPDATE record"));
// #endif		
// 						}
// 						else
// 						{
// 							/*
// 							 * Overlaying suceeded - we must store one overlaied record and free the previous one
// 							 */
// 							WaldiffRecord overlaied_WDRec = entry->data;

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
// 						entry->data = WaldiffRec;
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
// 				default:
// 					break;
// 			}			
// 		} 
	}
}

static void  
constructing_waldiff(XLogRecPtr *last_checkpoint)
{
	// XLogReaderPrivate 	*reader_private = (XLogReaderPrivate *) (reader_state->private_data);
	// XLogRecPtr 	   		 first_record = XLogFindNextRecord(reader_state, reader_private->startptr);
	
	// if (first_record == InvalidXLogRecPtr)
	// 	ereport(FATAL, 
	// 			errmsg("WALDIFF: could not find a valid record after %X/%X", 
	// 					LSN_FORMAT_ARGS(reader_private->startptr)));

	// if (first_record != reader_private->startptr && 
	// 	XLogSegmentOffset(reader_private->startptr, DEFAULT_XLOG_SEG_SIZE) != 0)
	// 	ereport(LOG, 
	// 			errmsg("WALDIFF: skipping over %u bytes", (uint32) (first_record - reader_private->startptr)));

	// for (;;)
	// {
	// 	WaldiffRecord  WaldiffRec;
	// 	XLogRecord 	  *WalRec;
	// 	char 		  *errormsg;

	// 	WalRec = XLogReadRecord(reader_state, &errormsg);
	// 	if (WalRec == InvalidXLogRecPtr) {
	// 		if (reader_private->endptr_reached)
	// 			break;

    //         ereport(ERROR, 
	// 				errmsg("WALDIFF: XLogReadRecord failed to read WAL record: %s", errormsg));
    //     }

	// 	if (WalRec->xl_rmid == RM_XLOG_ID)
	// 	{
	// 		if ((WalRec->xl_info & XLR_RMGR_INFO_MASK) == XLOG_CHECKPOINT_SHUTDOWN ||
	// 			(WalRec->xl_info & XLR_RMGR_INFO_MASK) == XLOG_CHECKPOINT_ONLINE)
	// 		{
	// 			last_checkpoint = reader_state->currRecPtr;
	// 		}
	// 		else if ((WalRec->xl_info & XLR_RMGR_INFO_MASK) == XLOG_SWITCH)
	// 			ereport(LOG, errmsg("WALDIFF: meet SWITCH record at position %X/%X", LSN_FORMAT_ARGS(reader_state->currRecPtr)));
	// 	}

	// 	/* Now we reckon that hash_table contains only HEAP records (INSERT, HOT UPDATE)
	// 	   without image */
	// 	/* firstly check in hash map*/
	// 	if (WalRec->xl_rmid == RM_HEAP_ID && 
	// 		!XLogRecHasBlockImage(reader_state, 0) &&
	// 	   	((WalRec->xl_info & XLOG_HEAP_OPMASK) == XLOG_HEAP_INSERT || 
	// 		(WalRec->xl_info & XLOG_HEAP_OPMASK) == XLOG_HEAP_HOT_UPDATE))
	// 	{
	// 		uint32_t hash_key;
	// 		HTABEntry *entry;
	// 		bool is_found;

	// 		hash_key = GetHashKeyOfWALRecord(reader_state->record);
			
	// 		entry = (HTABEntry *) hash_search(hash_table, (void*) &hash_key, HASH_FIND, &is_found);

	// 		if (entry && is_found)
	// 		{
	// 			XLogRecord *construcred_record;
	// 			int written_bytes;

	// 			WaldiffRec = entry->data;
	// 			Assert(WaldiffRec != NULL);

	// 			/* construct WALDIFF */
	// 			// construcred_record = constructWALDIFF(WaldiffRec);

	// 			// write WALDIFF record
	// 			// TODO need writer

	// 			// written_bytes = FileWrite(writer_state->seg.ws_file, construcred_record, construcred_record->xl_tot_len, writer_state->EndRecPtr % DEFAULT_XLOG_SEG_SIZE, WAIT_EVENT_WAL_COPY_WRITE);
	// 			// if (written_bytes != construcred_record->xl_tot_len)
	// 			// {
	// 			// 	if (written_bytes < 0)
	// 			// 		ereport(PANIC,
	// 			// 				(errcode_for_file_access(),
	// 			// 				errmsg("could not write WALDIFF: %m")));
	// 			// 	else
	// 			// 		ereport(PANIC,
	// 			// 				(errcode(ERRCODE_DATA_CORRUPTED),
	// 			// 				errmsg("could not write WALDIFF: write %d of %zu",
	// 			// 						written_bytes, construcred_record->xl_tot_len)));
	// 			// }
	// 		}
	// 		continue;
	// 	}
	// 	else 
	// 	{
	// 		// write regular WAL record
	// 		WaldiffWriterWrite(writer, WalRec);
	// 	}
	// }
}

void 
waldiff_shutdown(ArchiveModuleState *reader)
{
	close(ControlFileFd);

	MemoryContextSwitchTo(memory_context_storage->old);
	Assert(CurrentMemoryContext != memory_context_storage->current);
	MemoryContextDelete(memory_context_storage->current);

	ereport(LOG, errmsg("WALDIFF: waldiff_shutdown"));
}

static void free_waldiff_record(WaldiffRecord record)
{
	for (int i = 0; i < record->max_block_id; i++)
	{
		if (record->blocks[i].has_data && record->blocks[i].block_data_len) 
		{
			Assert(record->blocks[i].block_data);
			pfree(record->blocks[i].block_data);
		}
	}
	if (record->has_main_data && record->main_data_len) 
	{
		Assert(record->main_data);
		pfree(record->main_data);
	}
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
overlay_suffix_and_prefix_from_old(WaldiffRecord WaldiffRec, Relation relation,
								   TupleDesc tuple_desc, xl_heap_header **heap_hdr,
								   char **block_tuple, Size *block_tuple_len)
{
	uint16 			 prev_prefix_len = 0, prev_suffix_len = 0;
					//  curr_prefix_len = 0, curr_suffix_len = 0;

	xl_heap_update *main_data = (xl_heap_update *) WaldiffRec->main_data;

	HeapTupleData old_tuple;
	char *overlayed_tuple;
	off_t offset = 0;
	Size  overlayed_tuple_len = WaldiffRec->blocks[0].block_data_len;

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
							WaldiffRec->blocks[0].blknum, 
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
							WaldiffRec->blocks[0].blknum, 
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
overlay_hot_update(WaldiffRecord prev_record, WaldiffRecord curr_record)
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

	Assert(GetRecordType(prev_record) == XLOG_HEAP_INSERT || GetRecordType(prev_record) == XLOG_HEAP_HOT_UPDATE);
	Assert(GetRecordType(curr_record) == XLOG_HEAP_HOT_UPDATE);
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
	if (GetRecordType(prev_record) == XLOG_HEAP_HOT_UPDATE) 
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
	if (GetRecordType(prev_record) == XLOG_HEAP_INSERT)
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
	else if (GetRecordType(prev_record) == XLOG_HEAP_HOT_UPDATE)
	{
		((xl_heap_update *) prev_main_data)->new_offnum = ((xl_heap_update *)curr_main_data)->new_offnum;
		((xl_heap_update *) prev_main_data)->new_xmax = ((xl_heap_update *)curr_main_data)->new_xmax;
	}

	return 0;
}

/* Must be wrapped with 
   LWLockAcquire(ControlFileLock, LW_EXCLUSIVE) and LWLockRelease(ControlFileLock); */
static void 
ReadControlFile(void)
{
	int			read_bytes;

	pgstat_report_wait_start(WAIT_EVENT_CONTROL_FILE_READ);
	read_bytes = pg_pread(ControlFileFd, &ControlFile, sizeof(ControlFileData), 0);
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
}

/* Must be wrapped with 
   LWLockAcquire(ControlFileLock, LW_EXCLUSIVE) and LWLockRelease(ControlFileLock); */
static void 
WriteControlFile(void)
{
	int			written_bytes;

	pgstat_report_wait_start(WAIT_EVENT_CONTROL_FILE_WRITE_UPDATE);
	written_bytes = pg_pwrite(ControlFileFd, &ControlFile, sizeof(ControlFileData), 0);
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
}