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

#define INSERT_CHAIN 1
#define UPDATE_CHAIN 2
#define DELETE_CHAIN 3
#define INITIAL_HASHTABLE_SIZE 100 // TODO скорее всего впоследствии мы его увеличим

PG_MODULE_MAGIC;

/**********************************************************************
  * Global data
  **********************************************************************/

static char 	wal_directory[MAXPGPATH];
static char 	*wal_diff_directory = NULL;
static int		WalSegSz;
static HTAB*	hash_table = NULL;

/**********************************************************************
  * Forward declarations
  **********************************************************************/

static bool check_archive_directory(char **newval, void **extra, GucSource source);
static bool create_wal_diff(const char *src, const char *dest);
static bool compare_files(const char *file1, const char *file2);
static bool is_file_archived(const char *file, const char *destination, const char *archive_directory);
static void wal_diff_startup(ArchiveModuleState *state);
static bool wal_diff_configured(ArchiveModuleState *state);
static bool wal_diff_archive(ArchiveModuleState *state, const char *file, const char *path);
static void wall_diff_shutdown(ArchiveModuleState *state);

 /**********************************************************************
  * Information for PostgreSQL
  **********************************************************************/
typedef struct ArchiveData
{
	MemoryContext oldcontext;
	MemoryContext context;
} ArchiveData;

typedef struct XLogDumpPrivate
{
	TimeLineID	timeline;
	XLogRecPtr	startptr;
	XLogRecPtr	endptr;
	bool		endptr_reached;
} XLogDumpPrivate;

static const ArchiveModuleCallbacks wal_diff_callbacks = {
    .startup_cb 		 = wal_diff_startup,
	.check_configured_cb = wal_diff_configured,
	.archive_file_cb 	 = wal_diff_archive,
	.shutdown_cb 		 = wall_diff_shutdown
};

 /**********************************************************************
  * The information we store about processed by wal-diff records
  **********************************************************************/
typedef struct ChainRecordData // TODO глянуть, как лучше раскидать поля для выравнивания
{
	/*
	 * These 3 fields are representing HeapTupleFields struct.

	 * If some of them not used in ChainRecord (for example. insert does not need t_xmax), they will be null'd 
	 * during fetching.
	 */
	TransactionId t_xmin;
	TransactionId t_xmax;
	CommandId	t_cid;

	/*
	 * Pointer to latest tuple version
	 */
	ItemPointerData current_t_ctid;

	/*
	 * In delete/update case this is the pointer on deleted tuple version.
	 */
	ItemPointerData old_t_ctid;

	ForkNumber 		forknum;
	RelFileLocator 	file_loc;
	uint16			t_infomask2;
	uint16			t_infomask;

	/*
	 * Size of [bitmap] + [padding] + appropriate header + [prefix length] + [suffix length] + user_data.
	 */
	uint16 			data_len;

	uint8 			chain_type;

	/*
	 * Offset to user data.
	 */
	uint8			t_hoff;

	/*
	 * Here comes [bitmap] + [padding] and then appropriate header + user_data.
	 * In update case 'user_data' also includes prefix and suffix lengths (both of them may be = 0)
	 * that comes right after 'xl_heap_update'
	 */
	bits8			t_bits[FLEXIBLE_ARRAY_MEMBER];
} ChainRecordData;

typedef ChainRecordData* ChainRecord;

#define SizeOfChainRecord offsetof(ChainRecordData, t_bits)

typedef struct ChainRecordHashEntry
{
	uint32_t 	hash_key;
	ChainRecord data;
} ChainRecordHashEntry;

/*
 * Use this to create key for struct ChainRecord in hashmap
 */
#define GetHashKeyFromChainRecord(record) \
( \
	(uint32_t)((record)->file_loc.spcOid + \
			   (record)->file_loc.dbOid + \
			   (record)->file_loc.relNumber + \
			   (record)->current_t_ctid.ip_blkid.bi_hi + \
			   (record)->current_t_ctid.ip_blkid.bi_lo + \
			   (record)->current_t_ctid.ip_posid) \
)

/*
 * Use this to find previous chain record 
 * (in case of delete/udate) in hashmap
 */
#define GetHashKeyOfPrevChainRecord(record) \
( \
	(uint32_t)((record)->file_loc.spcOid + \
			   (record)->file_loc.dbOid + \
			   (record)->file_loc.relNumber + \
			   (record)->old_t_ctid.ip_blkid.bi_hi + \
			   (record)->old_t_ctid.ip_blkid.bi_lo + \
			   (record)->old_t_ctid.ip_posid) \
)

static void continuous_reading_wal_file(XLogReaderState *xlogreader_state, XLogDumpPrivate *private);

/*
 * This three fuctions returns palloced struct
 * 
 * At end of struct you can find xl_heap_insert[update][delete] and main tuple data
 */
static ChainRecord fetch_insert(XLogReaderState *record);
static ChainRecord fetch_update(XLogReaderState *record);
static ChainRecord fetch_hot_update(XLogReaderState *record);
static ChainRecord fetch_delete(XLogReaderState *record);

static void overlay_update(ChainRecord old_tup, ChainRecord new_tup);
static void XLogDisplayRecord(XLogReaderState *record);

// static uint32_t wal_diff_hash_key(const void *key, size_t keysize);

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
							   &wal_diff_directory,
							   "wal_diff_directory",
							   PGC_SIGHUP,
							   0,
							   check_archive_directory, NULL, NULL);

	MarkGUCPrefixReserved("wal_diff");
}

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

// add checking if there "still temp" wal-diffs

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

	data = (ArchiveData *) MemoryContextAllocZero(TopMemoryContext,
													   sizeof(ArchiveData));
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
    return  wal_diff_directory != NULL && 
			wal_diff_directory[0] != '\0';
}

static int 
WalReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
				XLogRecPtr targetPtr, char *readBuff)
{
	XLogDumpPrivate *private = state->private_data;
	int			count = XLOG_BLCKSZ;
	WALReadError errinfo;

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

	if (!WALRead(state, readBuff, targetPagePtr, count, private->timeline,
				 &errinfo))
	{
		WALOpenSegment *seg = &errinfo.wre_seg;
		char		fname[MAXPGPATH];

		XLogFileName(fname, seg->ws_tli, seg->ws_segno,
					 state->segcxt.ws_segsize);

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

static void 
WalOpenSegment(XLogReaderState *state, XLogSegNo nextSegNo,
				   TimeLineID *tli_p)
{
	TimeLineID tli = *tli_p;
    char fname[MAXPGPATH];
	char fpath[MAXPGPATH];

    XLogFileName(fname, tli, nextSegNo, state->segcxt.ws_segsize);

	if (snprintf(fpath, MAXPGPATH, "%s/%s", state->segcxt.ws_dir, fname) == -1)
		ereport(ERROR,
				errmsg("error during reading WAL absolute path : %s/%s", state->segcxt.ws_dir, fname));

	state->seg.ws_file = OpenTransientFile(fpath, O_RDONLY | PG_BINARY);
	if (state->seg.ws_file < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", fpath)));
}

static void 
WalCloseSegment(XLogReaderState *state)
{
	close(state->seg.ws_file);
	state->seg.ws_file = -1;
}

static void
getWalDirecotry(char *wal_directory, const char *path, const char *file)
{
	if (strlen(path) > MAXPGPATH)
		ereport(ERROR,
				errmsg("WAL file absolute name is too long : %s", path));

	if (snprintf(wal_directory, strlen(path), "%s", path) == -1)
		ereport(ERROR,
				errmsg("error during reading WAL directory path : %s", path));

	MemSet(wal_directory + (strlen(path) - strlen(file) - 1), 0, strlen(file));
	ereport(LOG, 
			errmsg("wal directory is : %s", wal_directory));
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
	XLogReaderState 	*xlogreader_state;
	char				wal_diff_file[MAXPGPATH];

	ereport(LOG, 
			errmsg("archiving file : %s", file));

	if (strlen(wal_directory) == 0)
		getWalDirecotry(wal_directory, path, file);

	sprintf(wal_diff_file, "%s/%s", wal_diff_directory, file);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("could not open file \"%s\": %m", path)));

	read_count = read(fd, buff.data, XLOG_BLCKSZ);

	CloseTransientFile(fd);

    if (read_count == XLOG_BLCKSZ) {
        XLogLongPageHeader longhdr = (XLogLongPageHeader) buff.data;
        WalSegSz = longhdr->xlp_seg_size;
        if (!IsValidWalSegSize(WalSegSz)) {
            ereport(ERROR, 
					errmsg("Invalid wal segment size : %d\n", WalSegSz));
        }
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

	XLogFromFileName(file, &private.timeline, &segno, WalSegSz);
    XLogSegNoOffsetToRecPtr(segno, 0, WalSegSz, private.startptr);
	XLogSegNoOffsetToRecPtr(segno + 1, 0, WalSegSz, private.endptr);

	xlogreader_state = 
		XLogReaderAllocate(WalSegSz, wal_directory,
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
		XLogSegmentOffset(private.startptr, WalSegSz) != 0)
		ereport(LOG, 
				errmsg("skipping over %u bytes", (uint32) (first_record - private.startptr)));

	continuous_reading_wal_file(xlogreader_state, &private);

	// а потом жоско скрафтим wal_diff

	if (create_wal_diff(path, wal_diff_file))
	{
		ereport(LOG, errmsg("created WAL-diff for file \"%s\"", file));
		return true;
	} 
	else 
	{
		ereport(ERROR, errmsg("error while creating WAL-diff"));
		return false;
	}

	ereport(LOG, errmsg("Wal Diff Created"));

	return true;
}

static void 
continuous_reading_wal_file(XLogReaderState *xlogreader_state, XLogDumpPrivate *private)
{
	char 			*errormsg;
	XLogRecord 		*record;
	uint8 			info_bits;
	ChainRecord 	chain_record;
	bool 			is_found;
	uint32_t 		hash_key;

	ChainRecordHashEntry* entry;

	for (;;)
	{
		record = XLogReadRecord(xlogreader_state, &errormsg);

		if (record == InvalidXLogRecPtr) {
			if (private->endptr_reached)
				break;
            ereport(ERROR, 
					errmsg("XLogReadRecord failed to read record: %s", errormsg));
        }

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
					continue;

				case XLOG_HEAP_UPDATE:
					chain_record = fetch_update(xlogreader_state);
					if (!chain_record)
						continue;

					hash_key = GetHashKeyOfPrevChainRecord(chain_record);
					entry = (ChainRecordHashEntry*) hash_search(hash_table, (void*) &hash_key, HASH_FIND, &is_found);
					if (is_found)
					{
						bool is_insert_chain = (entry->data->chain_type == INSERT_CHAIN);
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

					continue;

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

					continue;
				default:
					ereport(LOG, 
							errmsg("unknown op code %u", info_bits));
					continue;
			}
		}
		// лол сделали второй RMGR только потому что закончились op коды
		else if (XLogRecGetRmid(xlogreader_state) == RM_HEAP2_ID) {
			// TODO
		}

		/*
		 * Now we deal only with HEAP and HEAP2 rmgrs 
		 */
		else
			continue;
	}
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
	fetched_record->chain_type = INSERT_CHAIN;
	fetched_record->file_loc = target_locator;
	fetched_record->forknum = forknum;

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
	fetched_record->chain_type = UPDATE_CHAIN;
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

	fetched_record->chain_type = DELETE_CHAIN;
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
	
	Assert((old_tup->chain_type == UPDATE_CHAIN && new_tup->chain_type == UPDATE_CHAIN) ||
			(old_tup->chain_type == INSERT_CHAIN && new_tup->chain_type == UPDATE_CHAIN));

	new_tup->old_t_ctid = old_tup->old_t_ctid;

	old_offset = old_tup->t_hoff - (sizeof(uint16) * 2);
	new_offset = new_tup->t_hoff - (sizeof(uint16) * 2);

	if (old_tup->chain_type == UPDATE_CHAIN) 
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

// pring record to stdout
static void
XLogDisplayRecord(XLogReaderState *record)
{
	const char *id;
	const RmgrData desc = GetRmgr(XLogRecGetRmid(record));
	uint32		rec_len;
	uint32		fpi_len;
	uint8		info = XLogRecGetInfo(record);
	XLogRecPtr	xl_prev = XLogRecGetPrev(record);
	StringInfoData s;

	XLogRecGetLen(record, &rec_len, &fpi_len);

	ereport(LOG, errmsg("rmgr: %-11s \nlen (rec/tot): %6u/%6u, \ntx: %10u, \nlsn: %X/%08X, \nprev %X/%08X, \n",
		   desc.rm_name,
		   rec_len, XLogRecGetTotalLen(record),
		   XLogRecGetXid(record),
		   LSN_FORMAT_ARGS(record->ReadRecPtr),
		   LSN_FORMAT_ARGS(xl_prev)));

	id = desc.rm_identify(info);
	if (id == NULL)
		ereport(LOG, errmsg("desc: UNKNOWN (%x) ", info & ~XLR_INFO_MASK));
	else
		ereport(LOG, errmsg("desc: %s ", id));

	initStringInfo(&s);
	desc.rm_desc(&s, record);
	ereport(LOG, errmsg("%s", s.data));

	resetStringInfo(&s);
	XLogRecGetBlockRefInfo(record, true, true, &s, NULL);
	ereport(LOG, errmsg("%s", s.data));
}


/*
 * is_file_archived
 *
 * Returns whether the file has already been archived.
 */
static bool 
is_file_archived(const char *file, const char *destination, const char *archive_directory) {
	struct stat st;
	
	if (stat(destination, &st) == 0)
	{
		if (compare_files(file, destination))
		{
			ereport(WARNING,
					errmsg("file \"%s\" already exists with identical contents",
							destination));

			//make sure that this file is fsynced to the disk
			fsync_fname(destination, false);
			fsync_fname(archive_directory, true);
			return true;
		}
		return false;
	}
	else if (errno != ENOENT)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat file \"%s\": %m", destination)));
		return false;
	}

	return false;
}

/*
 * compare_files
 *
 * Returns whether the contents of the files are the same.
 */
static bool
compare_files(const char *file1, const char *file2) 
{
#define CMP_BUF_SIZE (4096)
	char buf1[CMP_BUF_SIZE];
	char buf2[CMP_BUF_SIZE];
	int fd1;
	int fd2;
	bool ret = true;

	fd1 = OpenTransientFile(file1, O_RDONLY | PG_BINARY);
	if (fd1 < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", file1)));

	fd2 = OpenTransientFile(file2, O_RDONLY | PG_BINARY);
	if (fd2 < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", file2)));

	for (;;)
	{
		int			nbytes = 0;
		int			buf1_len = 0;
		int			buf2_len = 0;

		while (buf1_len < CMP_BUF_SIZE)
		{
			nbytes = read(fd1, buf1 + buf1_len, CMP_BUF_SIZE - buf1_len);
			if (nbytes < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read file \"%s\": %m", file1)));
			else if (nbytes == 0)
				break;

			buf1_len += nbytes;
		}

		while (buf2_len < CMP_BUF_SIZE)
		{
			nbytes = read(fd2, buf2 + buf2_len, CMP_BUF_SIZE - buf2_len);
			if (nbytes < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read file \"%s\": %m", file2)));
			else if (nbytes == 0)
				break;

			buf2_len += nbytes;
		}

		if (buf1_len != buf2_len || memcmp(buf1, buf2, buf1_len) != 0)
		{
			ret = false;
			break;
		}
		else if (buf1_len == 0)
			break;
	}

	if (CloseTransientFile(fd1) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", file1)));

	if (CloseTransientFile(fd2) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", file2)));

	return ret;
}

/*
 * create_wal_diff
 *
 * Creates one WAL-diff file.
 */
static bool 
create_wal_diff(const char *src, const char *dest)
{
	copy_file(src, dest);
	return true;
}

/*
 * wall_diff_shutdown
 *
 * Frees our allocated state.
 */
static void 
wall_diff_shutdown(ArchiveModuleState *state)
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
