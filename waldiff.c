#include "waldiff.h"
#include "waldiff_writer.h"
#include "waldiff_reader.h"
#include "waldiff_test.h"

PG_MODULE_MAGIC;

/* The value hould be a power of 2 */
#define INITIAL_HASHTABLE_SIZE 128
/* GUC value's store */
static char *waldiff_dir;

/* Global data */
static MemoryContextStorage *memory_context_storage;
static WALDIFFWriterState   *writer_state;
static WALDIFFReaderState   *reader_state;
static HTAB 				*hash_table;

typedef struct HTABElem
{
	uint32_t 	  key;
	WALDIFFRecord data;
} HTABElem;

/* Forward declaration */
static void waldiff_startup(ArchiveModuleState *state);
static bool waldiff_configured(ArchiveModuleState *state);
static bool waldiff_archive(ArchiveModuleState *state, const char *file, const char *path);
static void waldiff_shutdown(ArchiveModuleState *state);

void WALDIFFOpenSegment(WALDIFFSegmentContext *segcxt, WALDIFFSegment *seg);
void WALDIFFCloseSegment(WALDIFFSegment *seg);

WALDIFFRecordReadResult WALDIFFReadRecord(WALDIFFReaderState *waldiff_reader);
WALDIFFRecordWriteResult WALDIFFWriteRecord(WALDIFFWriterState *waldiff_writer,
											WALDIFFRecord record);

/* This three fuctions returns palloced struct */
static WALDIFFRecord fetch_insert(XLogRecord *record);
static WALDIFFRecord fetch_update(XLogRecord *record);
static WALDIFFRecord fetch_delete(XLogRecord *record);

static void overlay_update(WALDIFFRecord prev_tup, WALDIFFRecord curr_tup);

static int getWALsegsize(const char *WALpath);											 
static void constructWALDIFFs();
static void finishWALDIFFSegment(); // do we need this? 

#define GetHashKeyOfWALDIFFRecord(record) \
( \
	(uint32_t)((record)->file_loc.spcOid + \
			   (record)->file_loc.dbOid + \
			   (record)->file_loc.relNumber + \
			   (record)->current_t_ctid.ip_blkid.bi_hi + \
			   (record)->current_t_ctid.ip_blkid.bi_lo + \
			   (record)->current_t_ctid.ip_posid) \
)

#define GetHashKeyOfPrevWALDIFFRecord(record) \
( \
	(uint32_t)((record)->file_loc.spcOid + \
			   (record)->file_loc.dbOid + \
			   (record)->file_loc.relNumber + \
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

    hash_table = hash_create("WALDIFFHashTable", INITIAL_HASHTABLE_SIZE,
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

	if (wal_segment_size == 0)
		wal_segment_size = getWALsegsize(WALpath);
	Assert(IsValidWalSegSize(wal_segment_size));

	ereport(LOG, errmsg("WAL segment size is: %d", wal_segment_size));

	if (reader_state == NULL)
		reader_state = WALDIFFReaderAllocate(wal_segment_size, waldiff_dir,
											 WALDIFFREADER_ROUTINE(.read_record = WALDIFFReadRecord,
																   .segment_open = WALDIFFOpenSegment,
																   .segment_close = WALDIFFCloseSegment));
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

	/* Determines tli, segno and startPtr values of archived WAL segment
	 * and future WALDIFF segment
	 */
	{
		XLogSegNo segNo;
		TimeLineID tli;
		XLogRecPtr startPtr;

		XLogFromFilName(WALfile, &tli, &segNo, reader_state->segcxt.segsize);
		XLogSegNoOffsetToRecPtr(segNo, 0, reader_state->segcxt.segsize, startPtr);
		Assert(startPtr != InvalidXLogRecPtr);

		WALDIFFBeginRead(reader_state, startPtr, segNo, tli);
		WALDIFFBeginWrite(writer_state, startPtr, segNo, tli);

		ereport(LOG, errmsg("segNo is: %d", segNo));
		ereport(LOG, errmsg("tli is: %d", tli));
		ereport(LOG, errmsg("startPtr is: %d", startPtr));
	}

	/* Main reading & constructing & writing loop */
	for (;;)
	{
		WALDIFFRecord WDRec;
		XLogRecord *readRec;

		WALDIFFRecordReadResult res = WALDIFFReadRecord(reader_state);
		if (res == WALDIFFREAD_FAIL)
			ereport(ERROR,
				errmsg("error during reading WAL record"));
			
		if (res == WALDIFFREAD_EOF)
			break;

		readRec = WALGetRec(reader_state);

		/* Now we're processing only several HEAP type WAL records*/
		if (WALRecGetRmid(WALGetRec(reader_state)) == RM_HEAP_ID)
		{
			uint32_t prev_hash_key;
			uint32_t hash_key;
			HTABElem *entry;
			bool is_found;

			switch(WALRecGetXLogType(WALGetRec(reader_state)))
			{
				case XLOG_HEAP_INSERT:
					WDRec = fetch_insert(readRec);
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
					WDRec = fetch_update(readRec);
					if (WDRec == NULL)
						ereport(ERROR, errmsg("fetch_update failed"));

					prev_hash_key = GetHashKeyOfPrevWALDIFFRecord(WDRec);
					hash_key = GetHashKeyOfWALDIFFRecord(WDRec);

					entry = (HTABElem *) hash_search(hash_table, (void*) &prev_hash_key, HASH_FIND, &is_found);
					if (is_found)
					{
						bool is_insert_WDrec = (WALDIFFRecGetXLogType(entry->data) == XLOG_HEAP_INSERT);
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
					WDRec = fetch_delete(readRec);
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
					WALDIFFRecordWriteResult res = WALDIFFWriteRecord(writer_state, readRec);
					if (res == WALDIFFWRITE_FAIL) 
						ereport(ERROR, errmsg("Error during writing WALDIFF records"));
					break;
			}
		} 
		
		else 
		{
			WALDIFFRecordWriteResult res = WALDIFFWriteRecord(writer_state, readRec);
			if (res == WALDIFFWRITE_FAIL) 
				ereport(ERROR, errmsg("Error during writing WALDIFF records"));
		}
	}

	Assert(((writer_state->seg.segno + 1) * writer_state->segcxt.segsize) - 
			writer_state->segoff > 0);

	ereport(LOG, errmsg("Start constructing WALDIFFs"));

	/* Constructing and writing WALDIFFs to WALDIFF segment */
	constructWALDIFFs();

	ereport(LOG, errmsg("Finishing WALDIFF segment"));

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
	close(reader_state->seg.fd);
	close(writer_state->seg.fd);

	hash_destroy(hash_table);

	MemoryContextSwitchTo(memory_context_storage->old);
	Assert(CurrentMemoryContext != memory_context_storage->current);
	MemoryContextDelete(memory_context_storage->current);
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
				errmsg("error during opening WAL segment: %s/%s", segcxt->dir, fname));
	
	seg->fd = OpenTransientFile(fpath, PG_BINARY | O_RDWR | O_CREAT | O_APPEND);
	if (seg->fd == -1)
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("could not open WAL segment \"%s\": %m", fpath)));
}

/*
 * WALDIFFCloseSegment
 *
 * Closes previously opend WAL segment
 */
void 
WALDIFFCloseSegment(WALDIFFSegment *seg)
{
	Assert(seg->fd != -1);
	close(seg->fd);

	seg->fd = -1;
	seg->segno = 0;
	seg->tli = 0;
}


/* 
 * WALDIFFReadRecord
 * 
 * Read one record
 */
WALDIFFRecordReadResult 
WALDIFFReadRecord(WALDIFFReaderState *waldiff_reader)
{

}

/* 
 * WALDIFFWriteRecord
 * 
 * Accumulate records in buffer, then writes them all at once.
 */
WALDIFFRecordWriteResult 
WALDIFFWriteRecord(WALDIFFWriterState *waldiff_writer,
					WALDIFFRecord record)
{

}

/*
 * fetch_insert
 * 
 */
WALDIFFRecord 
fetch_insert(XLogRecord *record)
{

}

/*
 * fetch_update
 * 
 */
WALDIFFRecord 
fetch_update(XLogRecord *record)
{

}

/*
 * fetch_delete
 * 
 */
WALDIFFRecord 
fetch_delete(XLogRecord *record)
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

	return wal_segment_size;
}

/*
 * constructWALDIFFs
 * 
 * Creates WALDIFF records according to data in hash table 
 * and writes them into WALDIFF segment
 */
void 
constructWALDIFFs()
{
	HTABElem *entry;
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, hash_table);
	while ((entry = (HTABElem *) hash_seq_search(&status)) != NULL)
	{
		if (WALRecGetRmid(WALGetRec(reader_state)) == RM_HEAP_ID)
		{
			switch(WALRecGetXLogType(WALGetRec(reader_state)))
			{
				case XLOG_HEAP_INSERT:

					break;
				
				case XLOG_HEAP_UPDATE:

					break;

				case XLOG_HEAP_DELETE:

					break;

				default:
					ereport(ERROR, errmsg("Unknown XLOG_HEAP type"));
			}
		}
		else 
			ereport(ERROR, errmsg("WALDIFF cannot contains not XLOG_HEAP types"));

		// append to write buf
		// when full then write
		// WALDIFFRecordWriteResult res = WALDIFFWriteRecords();
	}
}

void 
finishWALDIFFSegment()
{

}