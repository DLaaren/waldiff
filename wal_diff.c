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


/**********************************************************************
 * Chained wal records for constructing one wal-diff
 **********************************************************************/
typedef struct ChainRecordData
{
	// it's needed for hash_key
	RelFileLocator file_loc;
	ItemPointerData current_t_ctid;
	ItemPointerData old_t_ctid;

	// here are rm_id and rm_identity = info & ~XLR_INFO_MASK and record_total_len
	XLogRecord xlog_record_header;

	HeapTuple heap_tuple;

	char record[XLogRecordMaxSize]; // unchanged record

} ChainRecordData;
typedef ChainRecordData* ChainRecord;

#define SizeOfChainRecord (SizeOfXLogRecord + sizeof(RelFileLocator) + 2 * sizeof(ItemPointerData) + HEAPTUPLESIZE + XLogRecordMaxSize)


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
static WalDiffWriterState 	wal_diff_writer_state;


/**********************************************************************
 * Forward declarations
 **********************************************************************/
static bool check_archive_directory(char **newval, void **extra, GucSource source);
static void continuous_reading_wal_file(XLogReaderState *xlogreader_state, 
										XLogDumpPrivate *private, 
										const char* orig_wal_file_name, 
										const char* wal_diff_file_name,
										XLogRecPtr initial_file_offset);
static bool create_wal_diff();
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
	DefineCustomStringVariable("wal_diff.wal_diff_directory",
							   gettext_noop("Archive WAL-diff destination directory."),
							   NULL,
							   wal_diff_writer_state.dest_dir,
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
    return  wal_diff_writer_state.dest_dir != NULL && 
			wal_diff_writer_state.dest_dir[0] != '\0';
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
	int 				fd = -1;
	PGAlignedXLogBlock 	buff; // local variable, holding a page buffer
    int 				read_count;
    XLogDumpPrivate 	private;
	XLogPageHeader 		page_hdr;
	XLogSegNo 			segno;
	XLogRecPtr 			first_record;
	XLogRecPtr			page_addr;
	XLogReaderState 	*xlogreader_state;
	char				wal_diff_file[MAXPGPATH];

	ereport(LOG, 
			errmsg("archiving file : %s", file));

	if (strlen(wal_diff_writer_state.src_dir) == 0)
		getWalDirecotry(path, file);

	sprintf(wal_diff_writer_state.dest_fname, "%s/%s", wal_diff_writer_state.src_dir, file);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("could not open file \"%s\": %m", path)));

	read_count = read(fd, buff.data, XLOG_BLCKSZ);

	CloseTransientFile(fd);

    if (read_count == XLOG_BLCKSZ) {
        XLogLongPageHeader longhdr = (XLogLongPageHeader) buff.data;
        wal_diff_writer_state.wal_segment_size = longhdr->xlp_seg_size;
        if (!IsValidWalSegSize(wal_diff_writer_state.wal_segment_size)) {
            ereport(ERROR, 
					errmsg("Invalid wal segment size : %d\n", wal_diff_writer_state.wal_segment_size));
        }
		page_addr = longhdr->std.xlp_pageaddr;
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

	XLogFromFileName(file, &private.timeline, &segno, wal_diff_writer_state.wal_segment_size);
    XLogSegNoOffsetToRecPtr(segno, 0, wal_diff_writer_state.wal_segment_size, private.startptr);
	XLogSegNoOffsetToRecPtr(segno + 1, 0, wal_diff_writer_state.wal_segment_size, private.endptr);

	xlogreader_state = 
		XLogReaderAllocate(wal_diff_writer_state.wal_segment_size, &(wal_diff_writer_state.src_dir),
							XL_ROUTINE(.page_read = WalReadPage,
									   .segment_open = WalOpenSegment,
									   .segment_close = WalCloseSegment),
							&private);

	if (!xlogreader_state) 
	{
		ereport(FATAL, 
				errmsg("out of memory while allocating a WAL reading processor"));
		return false;
	}

	first_record = XLogFindNextRecord(xlogreader_state, private.startptr);

	if (first_record == InvalidXLogRecPtr)
	{
        ereport(FATAL, 
				errmsg("could not find a valid record after %X/%X", 
						LSN_FORMAT_ARGS(private.startptr)));
        return false;
    }

	page_hdr = (XLogPageHeader) xlogreader_state->readBuf;

	/*
	 * This cases we should consider later 
	 */
	if (page_hdr->xlp_rem_len)
    	ereport(LOG, 
				errmsg("got some remaining data from a previous page : %d", page_hdr->xlp_rem_len));

	if (first_record != private.startptr && 
		XLogSegmentOffset(private.startptr, wal_diff_writer_state.wal_segment_size) != 0)
		ereport(LOG, 
				errmsg("skipping over %u bytes", (uint32) (first_record - private.startptr)));

	continuous_reading_wal_file(xlogreader_state, &private, path, wal_diff_file, page_addr);

	ereport(LOG, errmsg("Wal Diff Created"));

	return true;
}

static void 
continuous_reading_wal_file(XLogReaderState *xlogreader_state, 
							XLogDumpPrivate *private, 
							const char* orig_wal_file_name, 
							const char* wal_diff_file_name,
							XLogRecPtr initial_file_offset)
{
	char 			*errormsg;
	XLogRecord 		*record;
	uint8 			info_bits;
	ChainRecord 	chain_record;
	bool 			is_found;
	uint32_t 		hash_key;

	XLogRecPtr 		seg_start;
	XLogRecPtr		seg_end;

	XLogRecPtr		last_read_rec = 0;
	Size			last_read_rec_len = 0;

	uint64			global_offset = initial_file_offset;
	XLogRecPtr		wal_diff_file_offset = 0;

	ChainRecordHashEntry* entry;

	int dst_fd;

	char* tmp_buffer = palloc0(BLCKSZ * 2);
	char* xlog_rec_buffer = palloc0(XLogRecordMaxSize);

	dst_fd = OpenTransientFile(wal_diff_file_name, O_RDWR | O_CREAT | O_APPEND | PG_BINARY);
	if (dst_fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", wal_diff_file_name)));

	for (;;)
	{
		record = XLogReadRecord(xlogreader_state, &errormsg);

		if (record == InvalidXLogRecPtr) {
			if (private->endptr_reached)
				break;
            ereport(ERROR, 
					errmsg("XLogReadRecord failed to read record: %s", errormsg));
        }

		last_read_rec = xlogreader_state->record->lsn;
		last_read_rec_len = xlogreader_state->record->header.xl_tot_len;

		if (XLogRecGetRmid(xlogreader_state) == RM_HEAP_ID)
		{
			if (XLogRecHasBlockImage(xlogreader_state, 0))
				continue;

			info_bits = XLogRecGetInfo(xlogreader_state) & ~XLR_INFO_MASK;

			switch (info_bits & XLOG_HEAP_OPMASK)
			{
				case XLOG_HEAP_INSERT:
					chain_record = fetch_insert(xlogreader_state);
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
					chain_record = fetch_update(xlogreader_state);
					if (!chain_record)
						continue;

					hash_key = GetHashKeyOfPrevChainRecord(chain_record);
					entry = (ChainRecordHashEntry*) hash_search(hash_table, (void*) &hash_key, HASH_FIND, &is_found);
					if (is_found)
					{
						bool is_insert_chain = (entry->data->xlog_type == XLOG_HEAP_INSERT);
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
					chain_record = fetch_delete(xlogreader_state);
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
							errmsg("unknown op code %u", info_bits));
					continue;
			}

			seg_start = xlogreader_state->ReadRecPtr;
			seg_end = xlogreader_state->EndRecPtr;

			if ((seg_start - global_offset) > 0) 
			{
				// ereport(LOG, errmsg("START : %ld\tEND : %ld, REC_LEN : %ld\tSIZE : %ld", global_offset, seg_start, MAXALIGN(xlogreader_state->record->header.xl_tot_len), seg_start - global_offset));
				copy_file_part(orig_wal_file_name, wal_diff_file_name, dst_fd, 
							seg_start - global_offset, 
							global_offset - initial_file_offset, 
							tmp_buffer, 
							xlog_rec_buffer,
							&wal_diff_file_offset);
			}

			global_offset = seg_end;
		}
		else
		{
			/*
			* Now we deal only with HEAP and HEAP2 rmgrs 
			*/
			continue;
		}
	}

	if (create_wal_diff())
	{
		ereport(LOG, errmsg("created WAL-diff for file \"%s\"", orig_wal_file_name));
		return true;
	} 
	else 
	{
		ereport(ERROR, errmsg("error while creating WAL-diff"));
		return false;
	}

	// ereport(LOG, errmsg("FINISH : GLOBAL OFFSET = %ld\tLAST READ = %ld\tLAST READ LSN : %X/%X", global_offset, last_read_rec, LSN_FORMAT_ARGS(last_read_rec)));
	if (last_read_rec - global_offset > 0)
	{
		copy_file_part(orig_wal_file_name, wal_diff_file_name, dst_fd, 
					   last_read_rec - global_offset + last_read_rec_len, 
					   global_offset - initial_file_offset, 
					   tmp_buffer, 
					   xlog_rec_buffer,
					   &wal_diff_file_offset);
	}

	CloseTransientFile(dst_fd);
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
create_wal_diff()
{
	ChainRecordHashEntry *entry;
	ChainRecord chain_record;
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, hash_table);
	while ((entry = (ChainRecordHashEntry *)hash_seq_search(&status)) != NULL)
	{
		char *constructed_record;
		char *constructed_record_curr_size = constructed_record;
		XLogRecord record;
		size_t record_size;

		chain_record = entry->data;

		record.xl_tot_len = SizeOfXLogRecord + chain_record->data_len;
		record.xl_prev = prev_record + page_addr;

		record.xl_info = chain_record->info;
		record.xl_rmid = chain_record->rm_id;

		pg_crc32c crc = INIT_CRC32C(crc);
		record.xl_crc = COMP_CRC32C(crc, &record + SizeOfXLogRecord, SizeOfXLogRecord - sizeof(pg_crc32c));

		// let it be only DataHeaderShort for now
		if (chain_record->xlog_type == XLOG_HEAP_INSERT)
		{
			XLogRecordBlockHeader blkhdr; 
			XLogRecordDataHeaderShort hdrshort;
			xl_heap_header xl_heap_hdr;


			record_size = SizeOfXLogRecordBlockHeader + SizeOfXLogRecordDataHeaderShort + SizeOfHeapHeader + chain_record->data_len;
			constructed_record = palloc0(record_size);
			record.xl_xid = chain_record->t_xmin;
			
			blkhdr.id = 0;
			blkhdr.fork_flags = 0;
			blkhdr.data_length = chain_record->data_len;

			hdrshort.id = 0;
			hdrshort.data_length = chain_record->data_len;

			xl_heap_hdr.t_hoff = chain_record->t_hoff;
			xl_heap_hdr.t_infomask2 = chain_record->t_infomask2;
			xl_heap_hdr.t_infomask = chain_record->t_infomask;
			
			memcpy(constructed_record_curr_size, &record, SizeOfXLogRecord);
			constructed_record_curr_size += SizeOfXLogRecord;
			memcpy(constructed_record_curr_size, &blkhdr, SizeOfXLogRecordBlockHeader);
			constructed_record_curr_size += SizeOfXLogRecordBlockHeader;
			memcpy(constructed_record_curr_size, &hdrshort, SizeOfXLogRecordDataHeaderShort);
			constructed_record_curr_size += SizeOfXLogRecordDataHeaderShort;
			memcpy(constructed_record_curr_size, &xl_heap_hdr, SizeOfHeapHeader);
			constructed_record_curr_size += SizeOfHeapHeader;
			memcpy(constructed_record_curr_size, chain_record->t_bits, chain_record->data_len);
		}
		// вопрос :
		// если у нас в цепочке нет isert'а, то мы просто обновляем поле с tuple data, а потом после вставляем один update
		// а если у нас в начале был insert, то то накладывая update, возвращается insert или update, уже соединённый с insert'om?
		else if (chain_record->xlog_type == XLOG_HEAP_UPDATE)
		{
			XLogRecordBlockHeader blkhdr; 
			XLogRecordDataHeaderShort hdrshort0;
			XLogRecordDataHeaderShort hdrshort1;

			record_size = SizeOfXLogRecordBlockHeader + SizeOfXLogRecordDataHeaderShort + SizeOfHeapHeader + chain_record->data_len;
			constructed_record = palloc0(record_size);
			record.xl_xid = chain_record->t_xmax;


		}
		// если хранить как я предложила, то заново собирать не нужно
		else if (chain_record->xlog_type == XLOG_HEAP_DELETE)
		{
			XLogRecordBlockHeader blkhdr; 
			// XLogRecordDataHeaderShort hdrshort;
			xl_heap_header xl_heap_hdr;

			record_size = SizeOfXLogRecordBlockHeader + SizeOfHeapHeader + chain_record->data_len;
			constructed_record = palloc0(record_size);
			record.xl_xid = chain_record->t_xmax;

			blkhdr.id = 0;
			blkhdr.fork_flags = 0;
			blkhdr.data_length = chain_record->data_len;

			xl_heap_hdr.t_hoff = chain_record->t_hoff;
			xl_heap_hdr.t_infomask2 = chain_record->t_infomask2;
			xl_heap_hdr.t_infomask = chain_record->t_infomask;

			memcpy(constructed_record_curr_size, &record, SizeOfXLogRecord);
			constructed_record_curr_size += SizeOfXLogRecord;
			memcpy(constructed_record_curr_size, &blkhdr, SizeOfXLogRecordBlockHeader);
			constructed_record_curr_size += SizeOfXLogRecordBlockHeader;
			memcpy(constructed_record_curr_size, &xl_heap_hdr, SizeOfHeapHeader);
			constructed_record_curr_size += SizeOfHeapHeader;
			memcpy(constructed_record_curr_size, chain_record->t_bits, chain_record->data_len);
		}

		prev_record += record_size;

		// write_one_xlog_rec(dest_fd, dest_filename, constructed_record, file_offset);
	}
	hash_seq_term(&status);

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
