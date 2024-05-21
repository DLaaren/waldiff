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
static WALDIFFWriterState *writer_state;
static WALDIFFReaderState *reader_state;
static HTAB *hash_table;

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
void WALDIFFOpenSegment(WALDIFFSegmentContext segcxt, WALDIFFSegment *seg);
void WALDIFFCloseSegment(WALDIFFSegment *seg);
WALDIFFRecordReadResult WALDIFFReadRecord(WALDIFFReaderState *waldiff_reader,
										  XLogRecPtr targetPagePtr,
										  XLogRecPtr targetRecPtr,
										  char *readBuf,
										  int reqLen);
WALDIFFRecordWriteResult WALDIFFWriteRecords(WALDIFFWriterState *waldiff_writer,
											 XLogRecPtr targetPagePtr,
											 XLogRecPtr targetRecPtr,
											 char *writeBuf,
											 int reqLen);

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
							   "WALDIFF destination directory.",
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
    memory_context_storage->current = AllocSetContextCreate(TopMemoryContext,
										                    "waldiff",
										                    ALLOCSET_DEFAULT_SIZES);
    memory_context_storage->old = MemoryContextSwitchTo(memory_context_storage->current);    

    /* Secondly, allocating the hash table */
    hash_ctl.keysize    = sizeof(uint32_t);
	hash_ctl.entrysize 	= sizeof(HTABElem);
	hash_ctl.hash 		= &tag_hash;
	/* It is said hash table must have its own memory context */
	// hash_ctl.hcxt 		= tmemory_conext_storage->current;      
	hash_ctl.hcxt = AllocSetContextCreate(memory_context_storage->current,
										  "WALDIFF_HTAB",
										  ALLOCSET_DEFAULT_SIZES);                   
    hash_table = hash_create("WALDIFFHashTable", INITIAL_HASHTABLE_SIZE,
                             &hash_ctl, HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);
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
waldiff_archive(ArchiveModuleState *state, const char *file, const char *path)
{

	/* Preparations */

    if (writer_state == NULL)
		writer_state = WALDIFFWriterAllocate(XLOG_BLCKSZ, XLOGDIR, 
											 WALDIFFWRITER_ROUTINE(.write_records = WALDIFFWriteRecords,
									  							   .segment_open = WALDIFFOpenSegment,
									  							   .segment_close = WALDIFFCloseSegment));

	if (reader_state == NULL)
		reader_state = WALDIFFReaderAllocate(XLOG_BLCKSZ, waldiff_dir,
											 WALDIFFREADER_ROUTINE(.read_record = WALDIFFReadRecord,
																   .segment_open = WALDIFFOpenSegment,
																   .segment_close = WALDIFFCloseSegment));


	/* Main work */
	

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

void 
WALDIFFOpenSegment(WALDIFFSegmentContext segcxt, WALDIFFSegment *seg)
{


}

void 
WALDIFFCloseSegment(WALDIFFSegment *seg)
{
	close(seg->fd);
	seg->fd = -1;
	seg->segno = 0;
	seg->tli = 0;
}


/* Read WAL record. Returns NULL on end-of-WAL or failure */
WALDIFFRecordReadResult 
WALDIFFReadRecord(WALDIFFReaderState *waldiff_reader,
				  XLogRecPtr targetPagePtr,
				  XLogRecPtr targetRecPtr,
				  char *readBuf,
				  int reqLen)
{

}

/* Write WALDIFF record. Returns NULL on end-of-WALDIFF or failure */
WALDIFFRecordWriteResult 
WALDIFFWriteRecords(WALDIFFWriterState *waldiff_writer,
					XLogRecPtr targetPagePtr,
					XLogRecPtr targetRecPtr,
					char *writeBuf,
					int reqLen)
{

}