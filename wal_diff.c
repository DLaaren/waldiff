#include "postgres.h"

/* system stuff */
#include <assert.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

/* postgreSQL stuff */
#include "access/heapam_xlog.h"
#include "access/xlogreader.h"
#include "access/xlogstats.h"
#include "access/xlog_internal.h"
#include "access/xlogdefs.h"
#include "archive/archive_module.h"
#include "common/int.h"
#include "common/logging.h"
#include "common/hashfn.h"
#include "miscadmin.h"
#include "lib/stringinfo.h"
#include "storage/copydir.h"
#include "storage/fd.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/hsearch.h"
#include "utils/wait_event.h"

#include "wal_diff_func.h"
#include "wal_diff_rmgr.h"

#define INITIAL_HASHTABLE_SIZE 100 // TODO: find the optimum value

PG_MODULE_MAGIC;

/**********************************************************************
 * Information for PostgreSQL
 **********************************************************************/
typedef struct ArchiveData
{
	MemoryContext oldcontext;
	MemoryContext context;
} ArchiveData;

RmgrData waldiff_rmgr = {
	.rm_name = WALDIFF_RM_NAME,
	.rm_redo = waldiff_rmgr_redo,
	.rm_desc = waldiff_rmgr_desc,
	.rm_identify = waldiff_rmgr_identify
};
/**********************************************************************
 * Chained wal records for constructing one wal-diff
 **********************************************************************/

// пока, наверное, оставим так, вся инфа есть внутри heap_tuple, если нужно что-то достать

typedef struct ChainRecordData
{
	/*
	 * These 3 fields are representing HeapTupleFields struct.

	 * If some of them not used in ChainRecord (for example. insert does not need t_xmax), they will be null'd 
	 * during fetching.
	 */
	TransactionId t_xmin;
	TransactionId t_xmax;
	CommandId	t_cid;			// never used

	/*
	 * Pointer to latest tuple version
	 */
	ItemPointerData current_t_ctid; 	// never used

	/*
	 * In delete/update case this is the pointer on deleted tuple version.
	 */
	ItemPointerData old_t_ctid;		// never used

	ForkNumber 		forknum;		// never used 
	RelFileLocator 	file_loc;		// never used
	uint16			t_infomask2;	// never used
	uint16			t_infomask;		// never used 
	uint8 			info;

	RmgrId			rm_id;
	uint8 			xlog_type;

	/*
	 * Offset to user data.
	 */
	uint8			t_hoff;

	/*
	 * Size of [bitmap] + [padding] + appropriate header + [prefix length] + [suffix length] + user_data.
	 */
	uint32 			data_len;

	/*
	 * Here comes [bitmap] + [padding] and then appropriate header + user_data.
	 * In update case 'user_data' also includes prefix and suffix lengths (both of them may be = 0)
	 * that comes right after 'xl_heap_update'
	 */
	bits8			t_bits[FLEXIBLE_ARRAY_MEMBER];
} ChainRecordData;
// t_bits follows

typedef ChainRecordData* ChainRecord;

#define SizeOfChainRecord offsetof(ChainRecordData, t_bits)

/**********************************************************************
 * Chained wal records hash entry for hash table
 **********************************************************************/
typedef struct ChainRecordHashEntry
{
	uint32_t 	hash_key;
	ChainRecord data;
} ChainRecordHashEntry;

// Use this to create key for struct ChainRecord in hashmap
#define GetHashKeyFromChainRecord(record) \
( \
	(uint32_t)((record)->file_loc.spcOid + \
			   (record)->file_loc.dbOid + \
			   (record)->file_loc.relNumber + \
			   (record)->current_t_ctid.ip_blkid.bi_hi + \
			   (record)->current_t_ctid.ip_blkid.bi_lo + \
			   (record)->current_t_ctid.ip_posid) \
)

// Use this to find previous chain record 
// (in case of delete/udate) in hashmap
#define GetHashKeyOfPrevChainRecord(record) \
( \
	(uint32_t)((record)->file_loc.spcOid + \
			   (record)->file_loc.dbOid + \
			   (record)->file_loc.relNumber + \
			   (record)->old_t_ctid.ip_blkid.bi_hi + \
			   (record)->old_t_ctid.ip_blkid.bi_lo + \
			   (record)->old_t_ctid.ip_posid) \
)


/**********************************************************************
 * Global data
 **********************************************************************/
static HTAB    				*hash_table;

WalDiffWriterState 			writer_state = {
	.src_dir 	= NULL,
	.dest_path 	= NULL,
	.dest_dir 	= NULL,
	.last_read_rec = 0
};

/**********************************************************************
 * Forward declarations
 **********************************************************************/
static bool create_wal_diff(char* xlog_rec_buffer);
static bool check_archive_directory(char **newval, void **extra, GucSource source);
static void continuous_reading_wal_file(XLogReaderState *reader_state, XLogDumpPrivate *private);
static void overlay_update(ChainRecord old_tup, ChainRecord new_tup);
static bool wal_diff_archive(ArchiveModuleState *state, const char *file, const char *path);
static bool wal_diff_configured(ArchiveModuleState *state);
static void wal_diff_shutdown(ArchiveModuleState *state);
static void wal_diff_startup(ArchiveModuleState *state);

// This three fuctions returns palloced struct
static ChainRecord fetch_insert(XLogReaderState *record);
static ChainRecord fetch_update(XLogReaderState *record);
static ChainRecord fetch_delete(XLogReaderState *record);


/**********************************************************************
 * Code
 **********************************************************************/

/*
 * _PG_init
 *
 * Defines the module's GUC.
 */
void
_PG_init(void)
{
	RegisterCustomRmgr(WALDIFF_RM_ID, &waldiff_rmgr);
	   
	DefineCustomStringVariable("wal_diff.wal_diff_directory",
							   gettext_noop("Archive WAL-diff destination directory."),
							   NULL,
							   &(writer_state.dest_dir),
							   "wal_diff_directory",
							   PGC_SIGHUP,
							   0,
							   check_archive_directory, NULL, NULL);

	MarkGUCPrefixReserved("wal_diff");
}

static const ArchiveModuleCallbacks wal_diff_callbacks = {
    .startup_cb 		 = wal_diff_startup,
	.check_configured_cb = wal_diff_configured,
	.archive_file_cb 	 = wal_diff_archive,
	.shutdown_cb 		 = wal_diff_shutdown
};

/*
 * _PG_archive_module_init
 *
 * Returns the module's archiving callbacks.
 */
const ArchiveModuleCallbacks *
_PG_archive_module_init(void)
{
	return &wal_diff_callbacks;
}

/*
 * wal_diff_startup
 *
 * Creates the module's memory context.
 */
void 
wal_diff_startup(ArchiveModuleState *state)
{
	ArchiveData *data;
	HASHCTL hash_ctl;

	data = (ArchiveData *) MemoryContextAllocZero(TopMemoryContext, sizeof(ArchiveData));
	data->context = AllocSetContextCreate(TopMemoryContext,
										  "archive",
										  ALLOCSET_DEFAULT_SIZES);
	state->private_data = (void *) data;
	data->oldcontext = MemoryContextSwitchTo(data->context);

	hash_ctl.keysize 	= sizeof(uint32_t);
	hash_ctl.entrysize 	= sizeof(ChainRecordHashEntry);

	/*
	 * reference to tag_hash from src/common/hashfn.c
	 */
	hash_ctl.hash 		= &tag_hash;

	hash_ctl.hcxt 		= data->context;

	hash_table = hash_create("ChainRecordHashTable",
							 INITIAL_HASHTABLE_SIZE,
							 &hash_ctl,
							 HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
}

/*
 * check_archive_directory
 *
 * Checks that the provided archive directory exists.
 */
static bool 
check_archive_directory(char **newval, void **extra, GucSource source)
{
	struct stat st;

	if (*newval == NULL || *newval[0] == '\0')
	{
		GUC_check_errmsg("Archive directory name is blank");
		return false;
	}
	if (strlen(*newval) >= MAXPGPATH)
	{
		GUC_check_errmsg("Archive directory name is too long");
		return false;
	}	
	if (stat(*newval, &st) != 0 || !S_ISDIR(st.st_mode))
	{
		GUC_check_errdetail("Specified archive directory does not exist: %m");
		if (pg_mkdir_p(*newval, 0700) != 0)
		{
			GUC_check_errmsg("Could not allocate specified directory: %m");
			return false;
		}
	}
	return true;
}

/*
 * wal_diff_configured
 *
 * Checks if wal_diff_directory is not blank.
 */
static bool 
wal_diff_configured(ArchiveModuleState *state)
{
    return  writer_state.dest_dir != NULL && 
			writer_state.dest_dir[0] != '\0';
}

/*
 * TODO:
 * 
 * Add funcionality for a scenario when we are recovering after crash
 */

/*
 * wal_diff_archive
 *
 * Archives one WAL-diff file.
 * 
 * file -- just name of the WAL file 
 * path -- the full path including the WAL file name
 */
static bool
wal_diff_archive(ArchiveModuleState *state, const char *file, const char *path)
{
	PGAlignedXLogBlock 	buff; // local variable, holding a page buffer
    int 				read_count;
    XLogDumpPrivate 	private;
	XLogPageHeader 		page_hdr;
	XLogSegNo 			segno;
	XLogRecPtr 			first_record;
	XLogReaderState 	*reader_state;

	ereport(LOG, 
			errmsg("archiving file : %s", file));
	
	// probably there is better way than constantly checking if we know wal directory
	if (writer_state.src_dir == NULL || strlen(writer_state.src_dir) == 0)
		getWalDirecotry(path, file);

	writer_state.src_path = path;
	writer_state.fname = file;

	{
		char wal_diff_path[MAXPGPATH];
		sprintf(wal_diff_path, "%s/%s", writer_state.dest_dir, file);

		writer_state.dest_path = palloc0(strlen(wal_diff_path) + 1);
		sprintf(writer_state.dest_path, "%s", wal_diff_path);
	}

	writer_state.dest_fd = OpenTransientFile(writer_state.dest_path, O_RDWR | O_CREAT | O_APPEND | PG_BINARY);
	if (writer_state.dest_fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("could not open file \"%s\": %m", writer_state.dest_path)));

	writer_state.src_fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (writer_state.src_fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("could not open file \"%s\": %m", path)));

	read_count = read(writer_state.src_fd, buff.data, XLOG_BLCKSZ);

    if (read_count == XLOG_BLCKSZ) {
        XLogLongPageHeader longhdr = (XLogLongPageHeader) buff.data;
        writer_state.wal_segment_size = longhdr->xlp_seg_size;
        if (!IsValidWalSegSize(writer_state.wal_segment_size)) {
            ereport(ERROR, 
					errmsg("Invalid wal segment size : %d\n", writer_state.wal_segment_size));
        }
		writer_state.page_addr = longhdr->std.xlp_pageaddr;
    }
	else if (read_count < 0) {
		ereport(ERROR,
				errmsg("Could not read file \"%s\": %m", path));
	}
    else {
        ereport(ERROR,
				errmsg("Could not read file \"%s\": read %d of %d", path, read_count, XLOG_BLCKSZ));
    }

    memset(&private, 0, sizeof(XLogDumpPrivate));
    private.timeline = 1;
	private.startptr = InvalidXLogRecPtr;
	private.endptr = InvalidXLogRecPtr;
	private.endptr_reached = false;

	XLogFromFileName(file, &private.timeline, &segno, writer_state.wal_segment_size);
    XLogSegNoOffsetToRecPtr(segno, 0, writer_state.wal_segment_size, private.startptr);
	XLogSegNoOffsetToRecPtr(segno + 1, 0, writer_state.wal_segment_size, private.endptr);

	reader_state = 
		XLogReaderAllocate(writer_state.wal_segment_size, writer_state.src_dir,
						   XL_ROUTINE(.page_read = WalReadPage,
									  .segment_open = WalOpenSegment,
									  .segment_close = WalCloseSegment),
						   &private);

	if (!reader_state) 
	{
		ereport(FATAL, 
				errmsg("out of memory while allocating a WAL reading processor"));
		return false;
	}

	first_record = XLogFindNextRecord(reader_state, private.startptr);

	if (first_record == InvalidXLogRecPtr)
	{
        ereport(FATAL, 
				errmsg("could not find a valid record after %X/%X", 
						LSN_FORMAT_ARGS(private.startptr)));
        return false;
    }

	page_hdr = (XLogPageHeader) reader_state->readBuf;

	// This cases we should consider later 
	if (page_hdr->xlp_rem_len)
    	ereport(LOG, 
				errmsg("got some remaining data from a previous page : %d", page_hdr->xlp_rem_len));

	if (first_record != private.startptr && 
		XLogSegmentOffset(private.startptr, writer_state.wal_segment_size) != 0)
		ereport(LOG, 
				errmsg("skipping over %u bytes", (uint32) (first_record - private.startptr)));

	continuous_reading_wal_file(reader_state, &private);

	ereport(LOG, errmsg("wal-diff created for file %s", file));

	return true;
}

static void 
continuous_reading_wal_file(XLogReaderState *reader_state, 
							XLogDumpPrivate *private)
{
	char 			*errormsg;
	XLogRecord 		*record;
	uint8 			rm_identity;
	ChainRecord 	chain_record;
	bool 			is_found;
	uint32_t 		hash_key;

	XLogRecPtr 		seg_start;
	XLogRecPtr		seg_end;
	XLogRecPtr		last_read_rec = 0;

	ChainRecordHashEntry* entry;

	char* tmp_buffer = palloc0(BLCKSZ * 2);
	char* xlog_rec_buffer = palloc0(XLogRecordMaxSize);

	writer_state.src_curr_offset = writer_state.page_addr;
	seg_start = reader_state->ReadRecPtr;
	seg_end = reader_state->EndRecPtr;

	for (;;)
	{
		record = XLogReadRecord(reader_state, &errormsg);

		if (record == InvalidXLogRecPtr) {
			if (private->endptr_reached)
				break;
            ereport(ERROR, 
					errmsg("XLogReadRecord failed to read record: %s", errormsg));
        }

		last_read_rec = reader_state->record->lsn;

		writer_state.last_read_rec_len = reader_state->record->header.xl_tot_len;

		if (XLogRecGetRmid(reader_state) == RM_HEAP_ID)
		{
			if (XLogRecHasBlockImage(reader_state, 0))
				continue;

			rm_identity = XLogRecGetInfo(reader_state) & XLR_RMGR_INFO_MASK & XLOG_HEAP_OPMASK;

			switch (rm_identity)
			{
				case XLOG_HEAP_INSERT:
					chain_record = fetch_insert(reader_state);
					if (!chain_record)
						continue;

					hash_key = GetHashKeyFromChainRecord(chain_record);
					hash_search(hash_table, (void*) &hash_key, HASH_FIND, &is_found);
					if (is_found)
					{
						entry = (ChainRecordHashEntry*) hash_search(hash_table, (void*) &hash_key, HASH_REMOVE, NULL);
					}

					entry = (ChainRecordHashEntry*) hash_search(hash_table, (void*) &hash_key, HASH_ENTER, NULL);
					entry->data = chain_record;

					break;

				case XLOG_HEAP_UPDATE:
					chain_record = fetch_update(reader_state);
					if (!chain_record)
						continue;

					hash_key = GetHashKeyOfPrevChainRecord(chain_record);
					entry = (ChainRecordHashEntry*) hash_search(hash_table, (void*) &hash_key, HASH_FIND, &is_found);
					if (is_found)
					{
						bool is_insert_chain = ((entry->data->xlog_type) == XLOG_HEAP_INSERT);
						overlay_update(entry->data, chain_record);

						if (!is_insert_chain) 
						{
							entry = (ChainRecordHashEntry*) hash_search(hash_table, (void*) &hash_key, HASH_REMOVE, NULL);
							hash_key = GetHashKeyFromChainRecord(chain_record);
							entry = (ChainRecordHashEntry*) hash_search(hash_table, (void*) &hash_key, HASH_ENTER, NULL);
							entry->data = chain_record;
						}
					}
					else 
					{
						hash_key = GetHashKeyFromChainRecord(chain_record);
						entry = (ChainRecordHashEntry*) hash_search(hash_table, (void*) &hash_key, HASH_ENTER, NULL);
						entry->data = chain_record;	
					}

					break;

				case XLOG_HEAP_HOT_UPDATE:
					/*
					 * We don't need to deal with hot update, because even if there is hot-update chain in
					 * WAL file, we will compress it to one update/insert record. So we just want index to refer
					 * to our "super update[insert]"
					 */
					continue;

				case XLOG_HEAP_DELETE:
					chain_record = fetch_delete(reader_state);
					if (!chain_record)
						continue;

					hash_key = GetHashKeyOfPrevChainRecord(chain_record);
					entry = hash_search(hash_table, (void*) &hash_key, HASH_FIND, &is_found);

					if (is_found)
					{
						// if chain_record is not in hash map then it is not in wal diff file
						entry = (ChainRecordHashEntry*) hash_search(hash_table, (void*) &hash_key, HASH_REMOVE, NULL);
					}
					// insert/update is in another wal file
					else 
					{
						entry = (ChainRecordHashEntry*) hash_search(hash_table, (void*) &hash_key, HASH_ENTER, NULL);
						entry->data = chain_record;
					}

					break;
					
				default:
					ereport(LOG, 
							errmsg("unknown op code %u", rm_identity));
					continue;
			}

			seg_start = reader_state->ReadRecPtr;
			seg_end = reader_state->EndRecPtr;
			if ((seg_start - writer_state.src_curr_offset) > 0) 
			{
				// ereport(LOG, errmsg("START : %ld\tEND : %ld, REC_LEN : %ld\tSIZE : %ld", writer_state.src_curr_offset, seg_start, MAXALIGN(reader_state->record->header.xl_tot_len), seg_start - writer_state.src_curr_offset));
				copy_file_part(seg_start - writer_state.src_curr_offset, 
							   writer_state.src_curr_offset - writer_state.page_addr, 
							   tmp_buffer, 
							   xlog_rec_buffer);
			}

			writer_state.src_curr_offset = seg_end;
		}
		else
		{
			/*
			* Now we deal only with HEAP and HEAP2 rmgrs 
			*/
			continue;
		}
	}

	// ereport(LOG, errmsg("FINISH : GLOBAL OFFSET = %ld\tLAST READ = %ld\tLAST READ LSN : %X/%X", writer_state.src_curr_offset, last_read_rec, LSN_FORMAT_ARGS(last_read_rec)));
	if (last_read_rec - writer_state.src_curr_offset > 0)
	{
		copy_file_part(last_read_rec - writer_state.src_curr_offset + writer_state.last_read_rec_len, 
					   writer_state.src_curr_offset - writer_state.page_addr, 
					   tmp_buffer, 
					   xlog_rec_buffer);
	}
	if (create_wal_diff(xlog_rec_buffer))
		ereport(LOG, errmsg("add all chain records to wal diff file \"%s\"", writer_state.fname));
	else 
		ereport(ERROR, errmsg("error while creating WAL-diff"));

	pfree(tmp_buffer);
	pfree(xlog_rec_buffer);
}

static ChainRecord 
fetch_insert(XLogReaderState *record)
{
	RelFileLocator 	target_locator;
    BlockNumber 	blknum;
    ForkNumber 		forknum;
    xl_heap_insert 	*xlrec;
    char* 			data;
    Size 			data_len;
	Size 			bitmap_len;
    xl_heap_header 	xlhdr;

	ChainRecord 	fetched_record = NULL;

    XLogRecGetBlockTag(record, 0, &target_locator, &forknum, &blknum);
    xlrec = (xl_heap_insert *) XLogRecGetData(record);

    data = XLogRecGetBlockData(record, 0, &data_len);

    memcpy((char *) &xlhdr, data, SizeOfHeapHeader);
	data_len -= SizeOfHeapHeader;
	data += SizeOfHeapHeader;

	fetched_record = (ChainRecord) palloc0((Size) (SizeOfChainRecord + 
												   SizeOfHeapInsert + 
												   data_len));
	fetched_record->rm_id = RM_HEAP_ID;
	fetched_record->xlog_type = XLOG_HEAP_INSERT;
	fetched_record->file_loc = target_locator;
	fetched_record->forknum = forknum;
	fetched_record->info = record->record->header.xl_info;

	fetched_record->data_len = data_len + SizeOfHeapInsert;

	/*
	 * In our case, t_ctid always will be point to itself,
	 * so we can learn about blknum and offset from this filed
	 */
	ItemPointerSetBlockNumber(&(fetched_record->current_t_ctid), blknum);
    ItemPointerSetOffsetNumber(&(fetched_record->current_t_ctid), xlrec->offnum);
	fetched_record->old_t_ctid = fetched_record->current_t_ctid;

	/*
	 * Copy bitmap + padding (if present) from xlog record
	 */
	bitmap_len = xlhdr.t_hoff - SizeofHeapTupleHeader;
    memcpy((char *) fetched_record + SizeOfChainRecord, data, bitmap_len);
	data += bitmap_len;

	/*
	 * Copy xl_heap_insert to our struct
	 */
	memcpy((char*) fetched_record + SizeOfChainRecord + bitmap_len, (char*) xlrec, SizeOfHeapInsert);

	/*
	 * Copy user data to our struct
	 */
	memcpy((char*) fetched_record + SizeOfChainRecord + bitmap_len + SizeOfHeapInsert, data, data_len - bitmap_len);

	/*
	 * user_data offset = size of ChainRecordData struct + size of bitmap + padding + insert_header
	 */
	fetched_record->t_hoff = SizeOfChainRecord + bitmap_len + SizeOfHeapInsert;

    fetched_record->t_infomask2 = xlhdr.t_infomask2;
    fetched_record->t_infomask 	= xlhdr.t_infomask;
	fetched_record->t_xmin = XLogRecGetXid(record);

	Assert(!(fetched_record->t_infomask & HEAP_MOVED));
	fetched_record->t_cid = FirstCommandId;
	fetched_record->t_infomask &= ~HEAP_COMBOCID;

	return fetched_record;
}

static ChainRecord 
fetch_update(XLogReaderState *record)
{
	RelFileLocator 	target_locator;
    BlockNumber 	old_blknum;
	BlockNumber 	new_blknum;
    ForkNumber 		forknum;
    xl_heap_update* xlrec;
    char* 			data;
	char*			data_end;

    Size 			data_len;
	Size			tuplen;
	Size 			bitmap_len;
    xl_heap_header 	xlhdr;

	uint16			prefixlen = 0,
					suffixlen = 0;

	ChainRecord 	fetched_record = NULL;

	XLogRecGetBlockTag(record, 0, &target_locator, &forknum, &new_blknum);
	if (!XLogRecGetBlockTagExtended(record, 1, NULL, NULL, &old_blknum, NULL))
		old_blknum = new_blknum;
	
    xlrec = (xl_heap_update *) XLogRecGetData(record);

	data = XLogRecGetBlockData(record, 0, &data_len);
	data_end = data + data_len;

	if (xlrec->flags & XLH_UPDATE_PREFIX_FROM_OLD)
	{
		Assert(new_blknum == old_blknum);
		memcpy(&prefixlen, data, sizeof(uint16));
		data += sizeof(uint16);
		data_len -= sizeof(uint16);
	}
	if (xlrec->flags & XLH_UPDATE_SUFFIX_FROM_OLD)
	{
		Assert(new_blknum == old_blknum);
		memcpy(&suffixlen, data, sizeof(uint16));
		data += sizeof(uint16);
		data_len -= sizeof(uint16);
	}

	memcpy((char *) &xlhdr, data, SizeOfHeapHeader);
	data += SizeOfHeapHeader;
	data_len -= SizeOfHeapHeader;
	tuplen = data_end - data;

	fetched_record = (ChainRecord) palloc0((Size) (SizeOfChainRecord + 
												   SizeOfHeapUpdate + 
												   (sizeof(uint16) * 2) +
												   data_len));
	fetched_record->rm_id = RM_HEAP_ID;
	fetched_record->xlog_type = XLOG_HEAP_UPDATE;
	fetched_record->file_loc = target_locator;
	fetched_record->forknum = forknum;

	fetched_record->t_infomask2 = xlhdr.t_infomask2;
    fetched_record->t_infomask 	= xlhdr.t_infomask;
	fetched_record->t_xmin = XLogRecGetXid(record);
	fetched_record->t_xmax = xlrec->new_xmax;

	/*
	 * Copy ItemPointerData of inserted and deleted tuple, because
	 * we need both to find in hashmap
	 */
	ItemPointerSetBlockNumber(&(fetched_record->current_t_ctid), new_blknum);
    ItemPointerSetOffsetNumber(&(fetched_record->current_t_ctid), xlrec->new_offnum);
	ItemPointerSetBlockNumber(&(fetched_record->old_t_ctid), old_blknum);
    ItemPointerSetOffsetNumber(&(fetched_record->old_t_ctid), xlrec->old_offnum);

	Assert(!(fetched_record->t_infomask & HEAP_MOVED));
	fetched_record->t_cid = FirstCommandId;
	fetched_record->t_infomask &= ~HEAP_COMBOCID;

	/*
	 * Copy bitmap + padding (if present) from xlog record
	 */
	bitmap_len = xlhdr.t_hoff - SizeofHeapTupleHeader;
    memcpy((char *) fetched_record + SizeOfChainRecord, data, bitmap_len);
	data += bitmap_len;

	/*
	 * Copy xl_heap_update to our struct
	 */
	memcpy((char*) fetched_record + SizeOfChainRecord + bitmap_len, (char*) xlrec, SizeOfHeapUpdate);

	/*
	 * Copy prefix and suffix length 
	 */
	memcpy((char*) fetched_record + SizeOfChainRecord + bitmap_len + SizeOfHeapUpdate, (void*) &prefixlen, sizeof(uint16));
	memcpy((char*) fetched_record + SizeOfChainRecord + bitmap_len + SizeOfHeapUpdate + sizeof(uint16), (void*) &suffixlen, sizeof(uint16));

	/*
	 * Copy changed part of old
	 */
	memcpy((char*) fetched_record + SizeOfChainRecord + bitmap_len + SizeOfHeapUpdate + (sizeof(uint16) * 2), data, tuplen - bitmap_len);

	fetched_record->data_len = SizeOfHeapUpdate + (sizeof(uint16) * 2) + tuplen;
	fetched_record->t_hoff = SizeOfChainRecord + bitmap_len + SizeOfHeapUpdate + (sizeof(uint16) * 2);
	return fetched_record;
}

static ChainRecord
fetch_delete(XLogReaderState *record)
{
	RelFileLocator 	target_locator;
	BlockNumber 	blknum;
    ForkNumber 		forknum;
	xl_heap_delete 	*xlrec = (xl_heap_delete *) XLogRecGetData(record);

	ChainRecord 	fetched_record = NULL;

	fetched_record = (ChainRecord) palloc0((Size) (SizeOfChainRecord));

	XLogRecGetBlockTag(record, 0, &target_locator, &forknum, &blknum);

	fetched_record->rm_id = RM_HEAP_ID;
	fetched_record->xlog_type = XLOG_HEAP_DELETE;
	fetched_record->t_xmax = xlrec->xmax;
	fetched_record->file_loc = target_locator;
	fetched_record->forknum = forknum;

	ItemPointerSetBlockNumber(&(fetched_record->old_t_ctid), blknum);
    ItemPointerSetOffsetNumber(&(fetched_record->old_t_ctid), xlrec->offnum);

	memcpy((char*) fetched_record + SizeOfChainRecord, (char*) xlrec, SizeOfHeapDelete);
	return fetched_record;
}

/*
 * Data from old_tup will overlay data from new_tup, if necessary.
 * After this call, you can deallocate old_tup
 */
static void 
overlay_update(ChainRecord old_tup, ChainRecord new_tup)
{
	uint16 old_prefix_len,
		   old_suffix_len,
		   new_prefix_len,
		   new_suffix_len,
		   old_offset,
		   new_offset;

	uint16 prefix_diff_len = 0;
	uint16 suffix_diff_len = 0;
	
	char*  prefix_diff = NULL;
	char*  suffix_diff = NULL;
	char*  tup_cpy;

	Size   tuplen,
	   	   old_tuplen;
	
	Assert((old_tup->xlog_type == XLOG_HEAP_UPDATE && new_tup->xlog_type == XLOG_HEAP_UPDATE) ||
			(old_tup->xlog_type == XLOG_HEAP_INSERT && new_tup->xlog_type == XLOG_HEAP_UPDATE));

	new_tup->old_t_ctid = old_tup->old_t_ctid;

	old_offset = old_tup->t_hoff - (sizeof(uint16) * 2);
	new_offset = new_tup->t_hoff - (sizeof(uint16) * 2);

	if (old_tup->xlog_type == XLOG_HEAP_UPDATE) 
	{
		memcpy((void*) &old_prefix_len, (char*) old_tup + old_offset, sizeof(uint16));
		old_offset += sizeof(uint16);
		memcpy((void*) &new_prefix_len, (char*) new_tup + new_offset, sizeof(uint16));
		new_offset += sizeof(uint16);

		memcpy((void*) &old_suffix_len, (char*) old_tup + old_offset, sizeof(uint16));
		memcpy((void*) &new_suffix_len, (char*) new_tup + new_offset, sizeof(uint16));

		if (new_prefix_len > old_prefix_len)
		{
			prefix_diff_len = new_prefix_len - old_prefix_len;
			prefix_diff = palloc0((Size) prefix_diff_len);

			memcpy(prefix_diff, (char*) old_tup + old_tup->t_hoff, prefix_diff_len);
		}

		if (new_suffix_len > old_suffix_len)
		{
			suffix_diff_len = new_suffix_len - old_suffix_len;
			suffix_diff = palloc0((Size) suffix_diff_len);

			memcpy(suffix_diff, (char*) old_tup + SizeOfChainRecord + old_tup->data_len - suffix_diff_len, suffix_diff_len);
		}

		// TODO надеемся, что repalloc просто добавит памяти в конце
		if (suffix_diff_len + prefix_diff_len > 0)
		{
			tuplen = new_tup->data_len - (new_tup->t_hoff - SizeOfChainRecord);
			old_tuplen = new_tup->data_len;
			tup_cpy = palloc0(tuplen);

			memcpy(tup_cpy, (char*) new_tup + new_tup->t_hoff, tuplen);

			new_tup->data_len += suffix_diff_len;
			new_tup->data_len += prefix_diff_len;
			new_tup = (ChainRecord) repalloc((void*) new_tup, (Size) (SizeOfChainRecord + 
																	SizeOfHeapUpdate + 
																	new_tup->data_len));
		}
			

		if (prefix_diff_len != 0)
		{
			memcpy((char*) new_tup + new_tup->t_hoff, prefix_diff, prefix_diff_len);
			memcpy((char*) new_tup + new_tup->t_hoff + prefix_diff_len, tup_cpy, tuplen);
		}
		else if (suffix_diff_len != 0)
		{
			memcpy((char*) new_tup + new_tup->t_hoff, tup_cpy, tuplen);
		}

		if (suffix_diff_len != 0)
		{
			memcpy((char*) new_tup + SizeOfChainRecord + old_tuplen + prefix_diff_len, suffix_diff, suffix_diff_len);
		}
	}
	else 
	{
		memcpy((void*) &new_prefix_len, (char*) new_tup + new_offset, sizeof(uint16));
		new_offset += sizeof(uint16);

		memcpy((void*) &new_suffix_len, (char*) new_tup + new_offset, sizeof(uint16));

		tuplen = new_tup->data_len - (new_tup->t_hoff - SizeOfChainRecord);

		memcpy((char*) old_tup + old_tup->t_hoff + new_prefix_len,
			   (char*) new_tup + new_tup->t_hoff, 
			   tuplen);		
	}
	
}

/*
 * create_wal_diff
 *
 * Creates one WAL-diff file.
 */
static bool 
create_wal_diff(char* xlog_rec_buffer)
{
	ChainRecordHashEntry *entry;
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, hash_table);
	while ((entry = (ChainRecordHashEntry *)hash_seq_search(&status)) != NULL)
	{
		ChainRecord 				chain_record = entry->data;
		XLogRecord 					record;
		XLogRecordDataHeaderShort 	short_hdr;
		XLogRecordDataHeaderLong 	long_hdr;

		HeapTupleHeaderData 		htup;
		HeapTupleFields 			htup_fields;

		int 						offset = 0;

		record.xl_tot_len = SizeOfXLogRecord + chain_record->data_len;
		record.xl_xid 	  = chain_record->t_xmin; // TODO что тут нужно то?
		record.xl_rmid 	  = RM_EXPERIMENTAL_ID;

		if (chain_record->data_len < 256)
		{
			short_hdr.id = XLR_BLOCK_ID_DATA_SHORT;
			short_hdr.data_length = chain_record->data_len;
			record.xl_tot_len += SizeOfXLogRecordDataHeaderShort;

			memcpy(xlog_rec_buffer, (char*) &record, SizeOfXLogRecord);
			offset = SizeOfXLogRecord;

			memcpy((char*) xlog_rec_buffer + offset, (char*) &short_hdr, SizeOfXLogRecordDataHeaderShort);
			offset += SizeOfXLogRecordDataHeaderShort;
		}
		else
		{
			long_hdr.id = XLR_BLOCK_ID_DATA_LONG;
			record.xl_tot_len += SizeOfXLogRecordDataHeaderLong;

			memcpy(xlog_rec_buffer, (char*) &record, SizeOfXLogRecord);
			offset = SizeOfXLogRecord;

			memcpy((char*) xlog_rec_buffer + offset, (char*) &long_hdr, sizeof(uint8));
			memcpy((char*) xlog_rec_buffer + offset + sizeof(uint8), (char*) &(chain_record->data_len), sizeof(uint32));
			offset += SizeOfXLogRecordDataHeaderLong;
		}

		htup_fields.t_xmin 		   = chain_record->t_xmin;
		htup_fields.t_xmax 		   = chain_record->t_xmax;
		htup_fields.t_field3.t_cid = chain_record->t_cid;

		htup.t_choice.t_heap = htup_fields;

		if (chain_record->xlog_type == XLOG_HEAP_DELETE)
			htup.t_ctid = chain_record->old_t_ctid;
		else
			htup.t_ctid = chain_record->current_t_ctid;
		
		htup.t_hoff 	 = 0; // this value will never be in use
		htup.t_infomask  = chain_record->t_infomask;
		htup.t_infomask2 = chain_record->t_infomask2;

		memcpy((char*) xlog_rec_buffer + offset, (char*) &htup, SizeofHeapTupleHeader);
		offset += SizeofHeapTupleHeader;

		memcpy((char*) xlog_rec_buffer + offset, (char*) chain_record->t_bits, chain_record->data_len);

		write_one_xlog_rec(writer_state.dest_fd, writer_state.dest_path, xlog_rec_buffer);
	}

	return true;
}

/*
 * wall_diff_shutdown
 *
 * Frees our allocated state.
 */
static void 
wal_diff_shutdown(ArchiveModuleState *state)
{
	ArchiveData *data = (ArchiveData *) state->private_data;

	close(writer_state.src_fd);
	close(writer_state.dest_fd);

	if (data == NULL)
		return;
	hash_destroy(hash_table);

	MemoryContextSwitchTo(data->oldcontext);
	MemoryContextReset(data->context);

	Assert(CurrentMemoryContext != data->context);
	if (MemoryContextIsValid(data->context))
		MemoryContextDelete(data->context);
	data->context = NULL;

	state->private_data = NULL;
}
