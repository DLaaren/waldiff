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

/*
 * We maintain an image of pg_control in shared memory.
 */
static ControlFileData *ControlFile = NULL;

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
int WalReadPage(XLogReaderState *reader, XLogRecPtr targetPagePtr, int reqLen,
				XLogRecPtr targetPtr, char *readBuff);
void WalCloseSegment(XLogReaderState *reader);

/* This three fuctions returns palloced struct */
static WALDIFFRecord fetch_insert(XLogReaderState *record);
static WALDIFFRecord fetch_update(XLogReaderState *record);
static WALDIFFRecord fetch_delete(XLogReaderState *record);

static void free_waldiff_record(WALDIFFRecord record);
static void copy_waldiff_record(WALDIFFRecord* dest, WALDIFFRecord src);

static int overlay_update(WALDIFFRecord prev_tup, WALDIFFRecord curr_tup);

static void first_passage(int wal_segment_size, XLogReaderPrivate *reader_private);
static void second_passage(XLogRecPtr *last_checkpoint);

static void write_main_data_hdr(char* record, uint32 total_rec_len, uint32* current_rec_len);
static int getWALsegsize(const char *WALpath);											 
static char *constructWALDIFF(WALDIFFRecord WDrec);
// do we need this?
// need to check how postgres deal with not full WAL segment
static void finishWALDIFFSegment(WALDIFFWriterState *writer);

static void ReadControlFile(void);
static void WriteControlFile(void);

#define ROTL32(x, y) ((x << y) | (x >> (32 - y)))

static inline uint32_t 
_hash_combine(uint32_t seed, uint32_t value)
{
    return seed ^ (ROTL32(value, 15) + 0x9e3779b9 + (seed << 6) + (seed >> 2));
}

#define GetHashKeyOfWALDIFFRecord(record) \
({ \
    uint32_t key = 0; \
    key = _hash_combine(key, (record)->blocks[0].file_loc.spcOid); \
    key = _hash_combine(key, (record)->blocks[0].file_loc.dbOid); \
    key = _hash_combine(key, (record)->blocks[0].file_loc.relNumber); \
    key = _hash_combine(key, (record)->current_t_ctid.ip_blkid.bi_hi); \
    key = _hash_combine(key, (record)->current_t_ctid.ip_blkid.bi_lo); \
    key = _hash_combine(key, (record)->current_t_ctid.ip_posid); \
    key + 1; \
})

#define GetHashKeyOfPrevWALDIFFRecord(record) \
({ \
    uint32_t key = 0; \
    key = _hash_combine(key, (record)->blocks[0].file_loc.spcOid); \
    key = _hash_combine(key, (record)->blocks[0].file_loc.dbOid); \
    key = _hash_combine(key, (record)->blocks[0].file_loc.relNumber); \
    key = _hash_combine(key, (record)->prev_t_ctid.ip_blkid.bi_hi); \
    key = _hash_combine(key, (record)->prev_t_ctid.ip_blkid.bi_lo); \
    key = _hash_combine(key, (record)->prev_t_ctid.ip_posid); \
    key + 1; \
})

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
	static int 		  wal_segment_size = 0;
	XLogReaderPrivate reader_private = {0};
	XLogSegNo 		  segno;
	XLogRecPtr	  	  last_checkpoint = InvalidXLogRecPtr;

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

	WALDIFFBeginWrite(writer, segno, reader_private.timeline, PG_BINARY | O_RDWR | O_CREAT);

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

	WALRawBeginRead(raw_reader, segno, reader_private.timeline, O_RDONLY | PG_BINARY);

	/* Main work */
	ereport(LOG, errmsg("archiving WAL file: %s", WALpath));


	/* 
	 * first passage:
	 *		
	 *		Reading with decoding, aslo fill the array with potential WALDIFF records' lsn 
	 *
	 * second passage:
	 * 
	 * 		Simple reading with writing constructed WALDIFF records according to the lsn array
	 */

	ereport(LOG, errmsg("first passage"));
	first_passage(wal_segment_size, &reader_private);

	ereport(LOG, errmsg("second passage"));
	second_passage(&last_checkpoint);

	// ereport(LOG, errmsg("constructing WALDIFFs"));
	/* Constructing and writing WALDIFFs to WALDIFF segment */
	// constructWALDIFF(writer);

	ereport(LOG, errmsg("finishing WALDIFF segment"));
	
	/* Questionable */
	/* End the segment with SWITCH record */
	finishWALDIFFSegment(writer);
	
	ControlFile = (ControlFileData*) palloc0(sizeof(ControlFileData));
	ereport(LOG, errmsg("write new last checkpoint location : %X/%X", LSN_FORMAT_ARGS(last_checkpoint)));

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

	ereport(LOG, errmsg("archived WAL file: %s", WALpath));

	return true;
}

static void 
first_passage(int wal_segment_size, XLogReaderPrivate *reader_private)
{
	/* Some pre-work */
	XLogLongPageHeader page_hdr;
	XLogRecPtr 	   first_record = XLogFindNextRecord(reader_state, reader_private->startptr);
	
	if (first_record == InvalidXLogRecPtr)
		ereport(FATAL, 
				errmsg("could not find a valid record after %X/%X", 
						LSN_FORMAT_ARGS(reader_private->startptr)));

	page_hdr = (XLogLongPageHeader) reader_state->readBuf;

	/* This cases we should consider later */ 
	if (page_hdr->std.xlp_rem_len)
		ereport(LOG, 
				errmsg("got some remaining data from a previous page : %d", page_hdr->std.xlp_rem_len));

	if (first_record != reader_private->startptr && 
		XLogSegmentOffset(reader_private->startptr, wal_segment_size) != 0)
		ereport(LOG, 
				errmsg("skipping over %u bytes", (uint32) (first_record - reader_private->startptr)));
	
	writer->first_page_addr = raw_reader->first_page_addr = page_hdr->std.xlp_pageaddr;
	writer->system_identifier = raw_reader->system_identifier = page_hdr->xlp_sysid;	

	
	for (;;) /* Main reading & constructing */
	{
		WALDIFFRecord WDrec = {0};
		XLogRecord 	  *WALRec;
		char 		  *errormsg;

		WALRec = XLogReadRecord(reader_state, &errormsg);
		if (WALRec == InvalidXLogRecPtr) {
			if (reader_private->endptr_reached)
				break;

            ereport(ERROR, 
					errmsg("XLogReadRecord failed to read WAL record: %s", errormsg));
        }

		/* Now we're processing only several HEAP type WAL records and without image */
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

					/*
					 * Inform raw_reader, that this lsn must be skipped in second passage
					 */
					NeedlessLsnListPush(raw_reader->needless_lsn_list, WDrec->lsn);

					hash_key = GetHashKeyOfWALDIFFRecord(WDrec);
					hash_search(hash_table, (void*) &hash_key, HASH_FIND, &is_found);
					if (is_found)
					{
						entry = (HTABElem *) hash_search(hash_table, (void*) &hash_key, HASH_REMOVE, NULL);
						free_waldiff_record(entry->data);
						entry->data = NULL;
					}

					entry = (HTABElem *) hash_search(hash_table, (void*) &hash_key, HASH_ENTER, NULL);
					entry->data = NULL;
					copy_waldiff_record(&(entry->data), WDrec);
					free_waldiff_record(WDrec);

					Assert(entry->key == hash_key);
					break;

				case XLOG_HEAP_UPDATE:
					WDrec = fetch_update(reader_state);
					if (WDrec == NULL)
						ereport(ERROR, errmsg("fetch_update failed"));

					/*
					 * Inform raw_reader, that this lsn must be skipped in second passage
					 */
					NeedlessLsnListPush(raw_reader->needless_lsn_list, WDrec->lsn);

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
							copy_waldiff_record(&(entry->data), WDrec);
							free_waldiff_record(WDrec);
							Assert(entry->key == hash_key);
						}
						else
						{
							WALDIFFRecord tmp = entry->data;
							entry = (HTABElem *) hash_search(hash_table, (void*) &prev_hash_key, HASH_REMOVE, NULL);
							entry->data = NULL;
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
						copy_waldiff_record(&(entry->data), WDrec);
						free_waldiff_record(WDrec);
						Assert(entry->key == hash_key);
					}

					break;

				case XLOG_HEAP_DELETE:
					WDrec = fetch_delete(reader_state);
					if (WDrec == NULL)
						ereport(ERROR, errmsg("fetch_delete failed"));

					/*
					 * Inform raw_reader, that this lsn must be skipped in second passage
					 */
					NeedlessLsnListPush(raw_reader->needless_lsn_list, WDrec->lsn);

					prev_hash_key = GetHashKeyOfPrevWALDIFFRecord(WDrec);
					hash_key = GetHashKeyOfWALDIFFRecord(WDrec);

					entry = hash_search(hash_table, (void*) &prev_hash_key, HASH_FIND, &is_found);
					if (is_found)
					{
						/* if prev WDrec is presented in the HTAB it is deleted by this delete record */
						entry = (HTABElem *) hash_search(hash_table, (void*) &prev_hash_key, HASH_REMOVE, NULL);
						free_waldiff_record(entry->data);
						entry->data = NULL;
					}

					/* insert/update (the prev record) is in another WAL segment */
					else 
					{
						entry = (HTABElem *) hash_search(hash_table, (void*) &hash_key, HASH_ENTER, NULL);
						entry->data = NULL;
						copy_waldiff_record(&(entry->data), WDrec);
						free_waldiff_record(WDrec);
						Assert(entry->key == hash_key);
					}

					break;

				/* unprocessed record type */
				default:
					break;
			}			
		} 
	}
}

static uint16
find_hash_key_from_raw_record(XLogRecord *WALrec)
{
	WALDIFFRecord WDrec = (WALDIFFRecord) palloc0(SizeOfWALDIFFRecord + 2 * sizeof(WALDIFFBlock));
	uint32_t hash_key;
	OffsetNumber offnum;
	uint8 block_id;
	size_t block_data_len_sum = 0; 
	char *curr_ptr = (char *)WALrec + SizeOfXLogRecord;

	Assert(WALrec->xl_rmid == RM_HEAP_ID);

	WDrec->type = WALrec->xl_info & XLOG_HEAP_OPMASK;

	Assert(WDrec->type == XLOG_HEAP_INSERT || 
			WDrec->type == XLOG_HEAP_UPDATE ||
			WDrec->type == XLOG_HEAP_DELETE);

	while(true) 
	{
		memcpy(&block_id, curr_ptr, sizeof(uint8));
		/* curr_ptr points to data hdr */
		if (block_id == XLR_BLOCK_ID_DATA_SHORT)
		{
			curr_ptr += SizeOfXLogRecordDataHeaderShort;
			break;
		}
		else if (block_id == XLR_BLOCK_ID_DATA_LONG)
		{
			curr_ptr += SizeOfXLogRecordDataHeaderLong;
			break;
		}
		else 
		{
			XLogRecordBlockHeader *blk_hdr;
			Assert(block_id <= XLR_MAX_BLOCK_ID);

			blk_hdr = (XLogRecordBlockHeader *)curr_ptr;

			/*
			 * For now, we not working with FPW
			 */
			if (blk_hdr->fork_flags & BKPBLOCK_HAS_IMAGE)
			{
				pfree(WDrec);
				return 0;
			}

			if (! (blk_hdr->fork_flags & BKPBLOCK_SAME_REL))
				curr_ptr += sizeof(RelFileLocator);

			curr_ptr += (SizeOfXLogRecordBlockHeader + sizeof(BlockNumber));
			block_data_len_sum += blk_hdr->data_length;
		}
	}

	curr_ptr += block_data_len_sum;

	switch(WDrec->type)
	{
		case XLOG_HEAP_INSERT:
		{
			xl_heap_insert *main_data = (xl_heap_insert *) curr_ptr;
			offnum = main_data->offnum;
			break;
		}
		case XLOG_HEAP_UPDATE:
		{
			xl_heap_update *main_data = (xl_heap_update *) curr_ptr;
			offnum = main_data->new_offnum;
			break;
		}
		case XLOG_HEAP_DELETE:
		{
			xl_heap_delete *main_data = (xl_heap_delete *) curr_ptr;
			offnum = main_data->offnum;
			break;
		}
		default:
			ereport(ERROR, errmsg("unproccessed XLOG_HEAP type"));
			break;
	}

	WDrec->current_t_ctid.ip_posid = offnum;

	hash_key = GetHashKeyOfWALDIFFRecord(WDrec);

	pfree(WDrec);

	return hash_key;
}

static void 
second_passage(XLogRecPtr *last_checkpoint)
{
	WALDIFFRecordWriteResult write_res;
	WALRawRecordReadResult   read_res;
	XLogRecord *WALrec;
	WALDIFFRecord WDrec;
	XLogRecPtr curr_lsn;
	uint32_t hash_key;
	bool isFound;
	
	for (;;) /* Main reading & writing */
	{
		reset_buff(raw_reader);
		reset_tmp_buff(raw_reader);

		read_res = raw_reader->routine.read_record(raw_reader, NULL);
		if (read_res == WALREAD_FAIL)
			ereport(ERROR, errmsg("error during reading raw record from WAL segment: %s", 
									WALDIFFWriterGetErrMsg(writer)));

		WALrec = (XLogRecord *) WALRawReaderGetLastRecordRead(raw_reader);
		curr_lsn = raw_reader->wal_seg.last_processed_record;

		if (WALrec->xl_rmid == RM_XLOG_ID)
		{
			if (WALrec->xl_info == XLOG_CHECKPOINT_SHUTDOWN ||
				WALrec->xl_info == XLOG_CHECKPOINT_ONLINE)
			{
				*last_checkpoint = writer->already_written + writer->first_page_addr;
			}
			else if (WALrec->xl_info == XLOG_SWITCH)
				ereport(LOG, errmsg("meet SWITCH record at position %X/%X", LSN_FORMAT_ARGS(raw_reader->first_page_addr + raw_reader->wal_seg.last_processed_record)));
		}

		/* Now we reckon that hash_table contains only HEAP records (INSERT, UPDATE, DELETE)
		   without image */
		/* firstly check in hash map*/
		if (WALrec->xl_rmid == RM_HEAP_ID &&
		   ( (WALrec->xl_info & XLOG_HEAP_OPMASK) == XLOG_HEAP_INSERT || 
			 (WALrec->xl_info & XLOG_HEAP_OPMASK) == XLOG_HEAP_UPDATE ||
			 (WALrec->xl_info & XLOG_HEAP_OPMASK) == XLOG_HEAP_DELETE ) &&
			WALrec->xl_info )
		{
			HTABElem *entry;
			/* Get necessary data for hash_key from raw record */
			hash_key = find_hash_key_from_raw_record(WALrec);

			/*
			 * If hash key is not found, it means that we are not working
			 * with this record (because of FPW, for example), so just write it
			 * to WALDIFF segment
			 */
			if (hash_key == 0)
				goto write;
			
			/* try finding in hash table record with this lsn */
			entry = (HTABElem *) hash_search(hash_table, (void*) &hash_key, HASH_FIND, &isFound);

			if (isFound)
			{
				char *construcred_record = NULL;
				WDrec = entry->data;

				Assert(WDrec != NULL);

				/* construct WALDIFF */
				construcred_record = constructWALDIFF(WDrec);

				/* write it */
				goto write;
			}

			/* secondly check in NeedlessLsnList */
			else if (! NeedlessLsnListFind(raw_reader->needless_lsn_list, curr_lsn))
			{
				/* Just write it down unchanged */
				goto write;
				/* If record's lsn is in NeedlessLsnList, 
				* what means that it is already a part of a WALDIFF,
				* then just skip this record */
			}

			continue;
		}

write:
		write_res = writer->routine.write_record(writer, WALRawReaderGetLastRecordRead(raw_reader));
		if (write_res == WALDIFFWRITE_FAIL) 
			ereport(ERROR, errmsg("error during writing WALDIFF records in waldiff_archive: %s", 
									WALDIFFWriterGetErrMsg(writer)));

		//TODO что ето
		else if (read_res == WALREAD_EOF) /* if we just read last record in WAL segment */
			break;
	}
}

static void 
ReadControlFile(void)
{
	int			fd;
	int			read_result;

	/*
	 * Read data...
	 */
	fd = BasicOpenFile(XLOG_CONTROL_FILE,
					   O_RDWR | PG_BINARY);
	if (fd < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m",
						XLOG_CONTROL_FILE)));

	pgstat_report_wait_start(WAIT_EVENT_CONTROL_FILE_READ);
	read_result = read(fd, ControlFile, sizeof(ControlFileData));
	if (read_result != sizeof(ControlFileData))
	{
		if (read_result < 0)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m",
							XLOG_CONTROL_FILE)));
		else
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not read file \"%s\": read %d of %zu",
							XLOG_CONTROL_FILE, read_result, sizeof(ControlFileData))));
	}
	pgstat_report_wait_end();

	close(fd);
}

static void 
WriteControlFile(void)
{
	int			fd;
	int			write_result;

	/*
	 * Read data...
	 */
	fd = BasicOpenFile(XLOG_CONTROL_FILE,
					   O_RDWR | PG_BINARY);
	if (fd < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m",
						XLOG_CONTROL_FILE)));

	pgstat_report_wait_start(WAIT_EVENT_CONTROL_FILE_READ);
	write_result = write(fd, ControlFile, sizeof(ControlFileData));
	if (write_result != sizeof(ControlFileData))
	{
		if (write_result < 0)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not write to file \"%s\": %m",
							XLOG_CONTROL_FILE)));
		else
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not write to file \"%s\": write %d of %zu",
							XLOG_CONTROL_FILE, write_result, sizeof(ControlFileData))));
	}
	pgstat_report_wait_end();

	close(fd);
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

void 
WalCloseSegment(XLogReaderState *reader)
{
	Assert(reader->seg.ws_file != -1);
	CloseTransientFile(reader->seg.ws_file);
	reader->seg.ws_file = -1;
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
	memcpy((void *) WDrec->main_data, (void *) main_data, SizeOfHeapDelete);
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
 * *dest should be NULL pointer
 */
static void copy_waldiff_record(WALDIFFRecord* dest, WALDIFFRecord src)
{
	int num_blocks = 0;

	Assert(*dest == NULL);
	Assert(src != NULL);

	if (src->type == XLOG_HEAP_INSERT || src->type == XLOG_HEAP_DELETE)
		num_blocks = 1;
	else if (src->type == XLOG_HEAP_UPDATE)
		num_blocks = 2;

	Assert(num_blocks != 0);

	*dest = (WALDIFFRecord) palloc0(SizeOfWALDIFFRecord + (sizeof(WALDIFFBlock) * num_blocks));
	memcpy(*dest, src, SizeOfWALDIFFRecord);

	(*dest)->main_data = (char*) palloc0(sizeof(char) * src->main_data_len);
	memcpy((*dest)->main_data, src->main_data, src->main_data_len);

	for (int i = 0; i < num_blocks; i++)
	{
		memcpy((void*)& (*dest)->blocks[i], (void*)&src->blocks[i], sizeof(WALDIFFBlock));
		
		if (src->type == XLOG_HEAP_DELETE || 
			(src->type == XLOG_HEAP_UPDATE && (i > 0) ) )
			continue;

		(*dest)->blocks[i].block_data = (char*) palloc0(sizeof(char) * src->blocks[i].block_data_len);
		memcpy((*dest)->blocks[i].block_data, src->blocks[i].block_data, src->blocks[i].block_data_len);
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
	Size 			curr_tup_block_data_len   = curr_tup->blocks[0].block_data_len;

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
		 * OK, now we are sure, that we can overlay updates. Now we must
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
			/* prefix's and suffix's values remain the same */
		}

		else if (prev_tup_suffix_len >= curr_tup_suffix_len &&
				 prev_tup_prefix_len >= curr_tup_prefix_len)
		{
			/*
			 * Second update completely "overlays" first update :
			 * 	[###] - first update
			 * [#####] - second update
			 */

			prev_tup->blocks[0].block_data_len = curr_tup_user_data_len;
			prev_tup->blocks[0].block_data = (char *) repalloc(prev_tup->blocks[0].block_data, prev_tup->blocks[0].block_data_len);
			prev_tup_block_data = prev_tup->blocks[0].block_data;

			memcpy(prev_tup_block_data, curr_tup_block_data, curr_tup_block_data_len);

			/* update prefix's and suffix's values */
			if (curr_tup_prefix_len > 0)
				memcpy(prev_tup->blocks[0].block_data, &curr_tup_prefix_len, sizeof(uint16));
			
			if (curr_tup_suffix_len > 0)
				memcpy((void *) (prev_tup->blocks[0].block_data + sizeof(uint16)), &curr_tup_suffix_len, sizeof(uint16));
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

			/* update prefix's and suffix's values */
			if (prev_tup_prefix_len > 0)
				memcpy(prev_tup->blocks[0].block_data, &prev_tup_prefix_len, sizeof(uint16));
			
			if (curr_tup_suffix_len > 0)
				memcpy((void *) (prev_tup->blocks[0].block_data + sizeof(uint16)), &curr_tup_suffix_len, sizeof(uint16));
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
			
			/* update prefix's and suffix's values */
			if (curr_tup_prefix_len > 0)
				memcpy(prev_tup->blocks[0].block_data, &curr_tup_prefix_len, sizeof(uint16));
			
			if (prev_tup_suffix_len > 0)
				memcpy((void *) (prev_tup->blocks[0].block_data + sizeof(uint16)), &prev_tup_suffix_len, sizeof(uint16));
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
			
				/* update prefix's and suffix's values */
				if (curr_tup_prefix_len > 0)
					memcpy(prev_tup->blocks[0].block_data, &curr_tup_prefix_len, sizeof(uint16));
				
				if (prev_tup_suffix_len > 0)
					memcpy((void *) (prev_tup->blocks[0].block_data + sizeof(uint16)), &prev_tup_suffix_len, sizeof(uint16));
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
			
				/* update prefix's and suffix's values */
				if (prev_tup_prefix_len > 0)
					memcpy(prev_tup->blocks[0].block_data, &prev_tup_prefix_len, sizeof(uint16));
				
				if (curr_tup_suffix_len > 0)
					memcpy((void *) (prev_tup->blocks[0].block_data + sizeof(uint16)), &curr_tup_suffix_len, sizeof(uint16));
			}
		}
	}

	{
		char *prev_tup_bitmap = (char *) (prev_tup_block_data - prev_tup_bitmap_len);
		char *curr_tup_bitmap = (char *) (curr_tup_block_data - curr_tup_bitmap_len);
		char *result_bitmap   = (char *) palloc0(sizeof(char) * prev_tup_bitmap_len);

		for (int i = 0; i < prev_tup_bitmap_len; i++)
		{
			int bit1       = 0;
			int bit2       = 0;
			int bit        = 0;

			for (bit = 0; bit < 8; bit++)
			{
				bit1 = (prev_tup_bitmap[i] >> bit) & 1;
				bit2 = (curr_tup_bitmap[i] >> bit) & 1;
			}

			if (bit1 == 1 && bit2 == 1)
				result_bitmap[i] |= (1 << bit);
			else
				result_bitmap[i] &= ~(1 << bit);
		}

		memcpy(prev_tup_bitmap, result_bitmap, prev_tup_bitmap_len);
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

void
write_main_data_hdr(char* record, uint32 main_data_len, uint32* current_rec_len)
{
	if (main_data_len < 256)
	{
		XLogRecordDataHeaderShort main_data_hdr = {XLR_BLOCK_ID_DATA_SHORT, main_data_len};

		memcpy(record + *current_rec_len, (void*)&main_data_hdr, SizeOfXLogRecordDataHeaderShort);
		*current_rec_len += SizeOfXLogRecordDataHeaderShort;
	}
	else
	{
		XLogRecordDataHeaderLong main_data_hdr = {XLR_BLOCK_ID_DATA_LONG};

		memcpy(record + *current_rec_len, (void*)&main_data_hdr, sizeof(uint8));
		*current_rec_len += sizeof(uint8);

		memcpy(record + *current_rec_len, (void*)&main_data_len, sizeof(uint32));
		*current_rec_len += sizeof(uint32);
	}
}

/*
 * constructWALDIFF
 * 
 * Creates WALDIFF records according to data in hash table
 * 
 * [ I reckon We should remain RMGR records' structure ]
 */
char* 
constructWALDIFF(WALDIFFRecord WDrec)
{
	static char *record = NULL;

	if (record == NULL)
		record = (char*) palloc0(XLogRecordMaxSize);

	if (WDrec->rec_hdr.xl_rmid == RM_HEAP_ID)
	{
		uint32 rec_tot_len = 0;

		/* The records have the same header */
		memcpy(record, &(WDrec->rec_hdr), SizeOfXLogRecord);
		rec_tot_len += SizeOfXLogRecord;

		switch(WDrec->type)
		{
			/* 
			 * Contains XLogRecord +
			 * XLogRecordBlockHeader_0 +  
			 * XLogRecordDataHeader[Short|Long] +
			 * RelFileLocator_0 + BlockNumber_0 + 
			 * block_data_0(xl_heap_header + tuple data) + main_data(xl_heap_insert)
			 */
			case XLOG_HEAP_INSERT:
			{
				WALDIFFBlock block_0;

				Assert(WDrec->max_block_id == 0);
				block_0 = WDrec->blocks[0];

				Assert(WDrec->blocks[0].blk_hdr.id == 0);

				/* XLogRecordBlockHeader */
				memcpy(record + rec_tot_len, &(block_0.blk_hdr), SizeOfXLogRecordBlockHeader);
				rec_tot_len += SizeOfXLogRecordBlockHeader;

				if (!(block_0.blk_hdr.fork_flags & BKPBLOCK_SAME_REL))
				{
					/* RelFileLocator */
					memcpy(record + rec_tot_len, &(block_0.file_loc), sizeof(RelFileLocator));
					rec_tot_len +=  sizeof(RelFileLocator);
				}

				/* BlockNumber */
				memcpy(record + rec_tot_len, &(block_0.blknum), sizeof(BlockNumber));
				rec_tot_len += sizeof(BlockNumber);

				/* XLogRecordDataHeader[Short|Long] */
				write_main_data_hdr(record, WDrec->main_data_len, &rec_tot_len);

				/* block data */
				memcpy(record + rec_tot_len, block_0.block_data, block_0.block_data_len);
				rec_tot_len += block_0.block_data_len;

				break;
			}
			
			/* 
			 * Contains XLogRecord +
			 * XLogRecordBlockHeader_0 + RelFileLocator_0 + 
			 * BlockNumber_0 + 
			 * ((If XLH_UPDATE_PREFIX_FROM_OLD or XLH_UPDATE_SUFFIX_FROM_OLD flags are set)
			 * prefix + suffix + xl_heap_header + tuple data) +
			 * XLogRecordBlockHeader_1 + BlockNumber_1 +
			 * XLogRecordDataHeader[Short|Long] +
			 * block_data_0 + main_data(xl_heap_update)
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
				
				Assert(WDrec->max_block_id == 1);
				block_0 = WDrec->blocks[0];
				block_1 = WDrec->blocks[1];

				/* XLogRecordBlockHeader 0 */
				memcpy(record + rec_tot_len, &(block_0.blk_hdr), SizeOfXLogRecordBlockHeader);
				rec_tot_len += SizeOfXLogRecordBlockHeader;

				if (!(block_0.blk_hdr.fork_flags & BKPBLOCK_SAME_REL))
				{
					/* RelFileLocator 0 */
					memcpy(record + rec_tot_len, &(block_0.file_loc), sizeof(RelFileLocator));
					rec_tot_len +=  sizeof(RelFileLocator);
				}

				/* BlockNumber 0 */
				memcpy(record + rec_tot_len, &(block_0.blknum), sizeof(BlockNumber));
				rec_tot_len += sizeof(BlockNumber);

				/* XLogRecordBlockHeader 1 */
				memcpy(record + rec_tot_len, &(block_1.blk_hdr), SizeOfXLogRecordBlockHeader);
				rec_tot_len += SizeOfXLogRecordBlockHeader;

				if (!(block_1.blk_hdr.fork_flags & BKPBLOCK_SAME_REL))
				{
					/* RelFileLocator 1 */
					memcpy(record + rec_tot_len, &(block_1.file_loc), sizeof(RelFileLocator));
					rec_tot_len +=  sizeof(RelFileLocator);
				}

				/* BlockNumber 1 */
				memcpy(record + rec_tot_len, &(block_1.blknum), sizeof(BlockNumber));
				rec_tot_len += sizeof(BlockNumber);

				/* XLogRecordDataHeader[Short|Long] */
				write_main_data_hdr(record, WDrec->main_data_len, &rec_tot_len);

				/* block data 0 */
				memcpy(record + rec_tot_len, block_0.block_data, block_0.block_data_len);
				rec_tot_len += block_0.block_data_len;

				break;
			}

			/* 
			 * Contains XLogRecord +
			 * XLogRecordBlockHeader_0 + XLogRecordDataHeader[Short|Long] +
			 * RelFileLocator_0 + 
			 * BlockNumber_0 + main_data(xl_heap_delete)
			 */
			case XLOG_HEAP_DELETE:
			{
				WALDIFFBlock block_0;

				Assert(WDrec->max_block_id == 0);
				block_0 = WDrec->blocks[0];

				/* XLogRecordBlockHeader */
				memcpy(record + rec_tot_len, &(block_0.blk_hdr), SizeOfXLogRecordBlockHeader);
				rec_tot_len += SizeOfXLogRecordBlockHeader;

				if (!(block_0.blk_hdr.fork_flags & BKPBLOCK_SAME_REL))
				{
					/* RelFileLocator */
					memcpy(record + rec_tot_len, &(block_0.file_loc), sizeof(RelFileLocator));
					rec_tot_len +=  sizeof(RelFileLocator);
				}

				/* BlockNumber */
				memcpy(record + rec_tot_len, &(block_0.blknum), sizeof(BlockNumber));
				rec_tot_len += sizeof(BlockNumber);

				/* XLogRecordDataHeader[Short|Long] */
				write_main_data_hdr(record, WDrec->main_data_len, &rec_tot_len);

				break;
			}

			default:
				ereport(ERROR, errmsg("unproccessed XLOG_HEAP type"));
		}

		/* main data */
		memcpy(record + rec_tot_len, WDrec->main_data, WDrec->main_data_len);
		rec_tot_len += WDrec->main_data_len;

		Assert(WDrec->rec_hdr.xl_tot_len == rec_tot_len);

	}
	else
	{
		ereport(ERROR, errmsg("WALDIFF cannot contains not XLOG_HEAP types"));
		return NULL;
	}

	return record;
}

/*
 * If we met SWITCH record before, we must store it in the end of waldiff file
 */
void 
finishWALDIFFSegment(WALDIFFWriterState *writer)
{
	XLogRecord noop_rec = {0};
	XLogRecord switch_rec = {0};

	WALDIFFRecordWriteResult write_res;	

    noop_rec.xl_tot_len = SizeOfXLogRecord;
	noop_rec.xl_info = XLOG_NOOP;
    noop_rec.xl_rmid = RM_XLOG_ID;

	switch_rec.xl_tot_len = SizeOfXLogRecord;
	switch_rec.xl_info = XLOG_SWITCH;
    switch_rec.xl_rmid = RM_XLOG_ID;

    while (writer->waldiff_seg.segsize - writer->already_written >= SizeOfXLogRecord)
	{
		if ((writer->waldiff_seg.segsize - writer->already_written < (SizeOfXLogRecord * 2)))
		{
			ereport(LOG, errmsg("write SWITCH record at position : %X/%08X",  LSN_FORMAT_ARGS(writer->first_page_addr + writer->already_written)));
			write_res = writer->routine.write_record(writer, (char *) &switch_rec);
			if (write_res == WALDIFFWRITE_FAIL) 
				ereport(ERROR, errmsg("error during writing WALDIFF records in waldiff_archive: %s", 
								      WALDIFFWriterGetErrMsg(writer)));
		}
		else
		{
			write_res = writer->routine.write_record(writer, (char*) &noop_rec);
			if (write_res == WALDIFFWRITE_FAIL) 
				ereport(ERROR, errmsg("error during writing WALDIFF records in waldiff_archive: %s", 
								      WALDIFFWriterGetErrMsg(writer)));
		}
	}

	WALDIFFFinishWithZeros();
}
