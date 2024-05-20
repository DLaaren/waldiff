#include "waldiff.h"
#include "waldiff_writer.h"
#include "waldiff_reader.h"
#include "waldiff_test.h"

PG_MODULE_MAGIC;

#define DEFAULT_ARCHIVE_DIRECTORY "waldiff_dir"
#define INITIAL_HASHTABLE_SIZE 100

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
							   &(writer_state->segcxt.wds_dir),
							   NULL,
							   PGC_SIGHUP,
							   0,
							   check_archive_directory, NULL, NULL);

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
static void waldiff_startup(ArchiveModuleState *state)
{
    HASHCTL hash_ctl = {0};

    /* First, allocating the archive module's memory context */
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
	hash_ctl.hcxt 		= memory_context_storage->current;                         
    hash_table = hash_create("WALDIFFHashTable", INITIAL_HASHTABLE_SIZE,
                             &hash_ctl, HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);
}

/*
 * waldiff_configured
 *
 * Checks if waldiff_dir is not blank and exists
 */
static bool waldiff_configured(ArchiveModuleState *state)
{
    struct stat st = {0};
    return  writer_state->segcxt.wds_dir != NULL && 
			writer_state->segcxt.wds_dir[0] != '\0' &&
            stat(writer_state->segcxt.wds_dir, &st) != 0 && 
            !S_ISDIR(st.st_mode);
}

/*
 * check_archive_directory
 *
 * Checks that the provided archive directory exists. If it does not, then
 * use default archive directory name.
 */
static bool 
check_archive_directory(char **newval, void **extra, GucSource source)
{
    struct stat st = {0};

	if (*newval == NULL || *newval[0] == '\0')
	{
		GUC_check_errmsg("WALDIFF archive directory name is blank");
        GUC_check_errmsg("Using default WALDIFF directory name: %s", DEFAULT_ARCHIVE_DIRECTORY);
        writer_state->segcxt.wds_dir = palloc(sizeof(DEFAULT_ARCHIVE_DIRECTORY));
        sprintf(writer_state->segcxt.wds_dir, "%s", DEFAULT_ARCHIVE_DIRECTORY);
		return true;
	}
	if (strlen(*newval) >= MAXPGPATH)
	{
		GUC_check_errmsg("WALDIFF archive directory name is too long");
		return false;
	}	
	if (stat(*newval, &st) != 0 || !S_ISDIR(st.st_mode))
	{
		GUC_check_errmsg("Specified WALDIFF archive directory does not exist: %m");
        GUC_check_errmsg("Creating WALDIFF archive directory");
		if (pg_mkdir_p(*newval, 0700) != 0)
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
 * file -- just name of the WAL file 
 * path -- the full path including the WAL file name
 */
static bool waldiff_archive(ArchiveModuleState *state, const char *file, const char *path)
{
    // stopped here
}

/*
 * walldiff_shutdown
 *
 * Frees all allocated reaources.
 */
static void waldiff_shutdown(ArchiveModuleState *state)
{
	close(reader_state->seg.ws_fd);
	close(writer_state->seg.wds_fd);

	hash_destroy(hash_table);

	MemoryContextSwitchTo(memory_context_storage->old);
	MemoryContextDelete(memory_context_storage->current);
	Assert(CurrentMemoryContext != memory_context_storage->current);
}
