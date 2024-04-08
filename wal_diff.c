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

#define INITIAL_HASHTABLE_SIZE 100 // TODO скорее всего впоследствии мы его увеличим

PG_MODULE_MAGIC;

/**********************************************************************
  * Global data
  **********************************************************************/

static char 	wal_directory[MAXPGPATH];
static char 	*wal_diff_directory = NULL;
static int		WalSegSz;
static HTAB*	hash_table = NULL;

/*
 * Some info we need to know from wal segment
 * while constracting wal_diff segment
 */
static XLogRecPtr prev_record = 0;
static uint64	  sys_id = 0;
static XLogRecPtr page_addr = 0;
static TimeLineID tli = 0;

/**********************************************************************
  * Forward declarations
  **********************************************************************/

static bool check_archive_directory(char **newval, void **extra, GucSource source);
static bool create_wal_diff(const char *src, const char *dest);
static bool is_file_archived(const char *file, const char *destination, const char *archive_directory);
static void wal_diff_startup(ArchiveModuleState *state);
static bool wal_diff_configured(ArchiveModuleState *state);
static bool wal_diff_archive(ArchiveModuleState *state, const char *file, const char *path);
static void wal_diff_shutdown(ArchiveModuleState *state);


/*
 * All functions we need to copy records from
 * wal segment to wal_diff segment (except those that we worked with)
 */
static void 		copy_file_part(const char* src, const char* dst_name, int dstfd, uint64 size, uint64 src_offset, 
								   char* tmp_buffer, char* xlog_rec_buffer, XLogRecPtr* file_offset);

static int			read_one_xlog_rec(int src_fd, const char* src_file_name, 
									  char* xlog_rec_buffer, char* tmp_buff);

static void			write_one_xlog_rec(int dst_fd, const char* dst_file_name, char* xlog_rec_buffer, uint64* file_offset);
static inline int 	read_file2buff(int fd, char* buffer, uint64 size, uint64 buff_offset, const char* file_name);
static inline int 	write_buff2file(int fd, char* buffer, uint64 size, uint64 buff_offset);

/*
 * Whether XLogRecord fits on the page with
 * given offset
 */
#define XlogRecHdrFitsOnPage(in_page_offset) \
( \
	(in_page_offset) + SizeOfXLogRecord < \
	BLCKSZ * (1 + (in_page_offset) / BLCKSZ) \
)

/*
 * Whether record with given length fits on the
 * page with given offset
 */

#define XlogRecFitsOnPage(in_page_offset, rec_len) \
( \
	(in_page_offset) + (rec_len) < \
	BLCKSZ * (1 + (in_page_offset) / BLCKSZ) \
)

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
	.shutdown_cb 		 = wal_diff_shutdown
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
	uint16 			data_len;

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

/*
 * This three fuctions returns palloced struct
 */
static ChainRecord fetch_insert(XLogReaderState *record);
static ChainRecord fetch_update(XLogReaderState *record);
static ChainRecord fetch_delete(XLogReaderState *record);

static void overlay_update(ChainRecord old_tup, ChainRecord new_tup);
static void XLogDisplayRecord(XLogReaderState *record);

static void continuous_reading_wal_file(XLogReaderState *xlogreader_state, 
										XLogDumpPrivate *private, 
										const char* orig_wal_file_name, 
										const char* wal_diff_file_name,
										XLogRecPtr initial_file_offset);
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
 * Writes 'size' bytes from (char*)(buffer + buff_offset) to file
 */
static inline int
write_buff2file(int fd, char* buffer, uint64 size, uint64 buff_offset)
{
	int nbytes;

	pgstat_report_wait_start(WAIT_EVENT_COPY_FILE_WRITE);

	nbytes = write(fd, (char*) (buffer + buff_offset), size);
	if (nbytes != size)
		ereport(ERROR,
			(errcode_for_file_access(),
			errmsg("could not write to file \"%d\": %m", fd)));
	if (nbytes == 0)
		ereport(ERROR,
			(errcode_for_file_access(),
			errmsg("file descriptor closed for write \"%d\": %m", fd)));

	pgstat_report_wait_end();

	return nbytes;
}

/*
 * Reads 'size' bytes from file to (char*)(buffer + buff_offset)
 */
static inline int
read_file2buff(int fd, char* buffer, uint64 size, uint64 buff_offset, const char* file_name)
{
	int nbytes;

	pgstat_report_wait_start(WAIT_EVENT_COPY_FILE_READ);
	nbytes = read(fd, (char*) (buffer + buff_offset), size);
	if (nbytes < 0)
		ereport(ERROR,
			(errcode_for_file_access(),
			errmsg("could not read file \"%s\": %m", file_name)));
	if (nbytes == 0)
		ereport(WARNING,
			(errcode_for_file_access(),
			errmsg("file descriptor closed for read \"%d\": %m", fd)));
	pgstat_report_wait_end();
	
	return nbytes;
}

static int
read_one_xlog_rec(int src_fd, const char* src_file_name, 
				  char* xlog_rec_buffer, char* tmp_buff)
{
	uint64 		current_file_pos = lseek(src_fd, 0, SEEK_CUR);
	XLogRecord* record;
	uint64 		read_in_total = 0;
	uint64 		xlog_rec_buff_offset = 0;
	bool 		is_first_page = true;

	int 		nbytes;

	if (current_file_pos < 0)
		ereport(ERROR,
			(errcode_for_file_access(),
			errmsg("could not navigate in file \"%s\": %m", src_file_name)));

	while (true)
	{
		if ((current_file_pos + read_in_total) % BLCKSZ == 0)
		{
			XLogPageHeader hdr;
			nbytes = read_file2buff(src_fd, tmp_buff, SizeOfXLogShortPHD, 0, src_file_name);
			if (nbytes == 0)
				break;
			
			read_in_total += nbytes;

			hdr = (XLogPageHeader) tmp_buff;
			if (XLogPageHeaderSize(hdr) > SizeOfXLogShortPHD)
			{
				XLogLongPageHeader long_hdr;
				nbytes = read_file2buff(src_fd, tmp_buff, SizeOfXLogLongPHD - SizeOfXLogShortPHD, read_in_total, src_file_name);
				if (nbytes == 0)
					break;

				long_hdr  = (XLogLongPageHeader) tmp_buff;
				sys_id 	  = long_hdr->xlp_sysid;
				page_addr = hdr->xlp_pageaddr;
				tli 	  = hdr->xlp_tli;
				
				read_in_total += nbytes;
			}

			if (hdr->xlp_rem_len > 0)
			{
				uint64 data_len = 0;

				if (is_first_page)
					ereport(ERROR,
						errmsg("previous record was was not fully read in addr : %X/%X", LSN_FORMAT_ARGS(hdr->xlp_pageaddr)));
				is_first_page = false;

				if (! XlogRecFitsOnPage(current_file_pos + read_in_total, hdr->xlp_rem_len))
					data_len = BLCKSZ * (1 + (current_file_pos + read_in_total) / BLCKSZ) - current_file_pos - read_in_total;
				else
					data_len = hdr->xlp_rem_len;
				
				nbytes = read_file2buff(src_fd, xlog_rec_buffer, data_len, xlog_rec_buff_offset, src_file_name);
				if (nbytes == 0)
					break;
				
				read_in_total += nbytes;
				xlog_rec_buff_offset += nbytes;

				if (data_len == hdr->xlp_rem_len)
				{
					lseek(src_fd, MAXALIGN(hdr->xlp_rem_len) - data_len, SEEK_CUR);
					// read_in_total += MAXALIGN(hdr->xlp_rem_len) - data_len;
					return read_in_total;
				}
				
				else
					continue;
			}
		}

		if (! XlogRecHdrFitsOnPage(current_file_pos + read_in_total))
		{
			uint64 data_len = BLCKSZ * (1 + (current_file_pos + read_in_total) / BLCKSZ) - current_file_pos - read_in_total;
			nbytes = read_file2buff(src_fd, xlog_rec_buffer, data_len, xlog_rec_buff_offset, src_file_name);
			if (nbytes == 0)
				break;
			
			read_in_total += nbytes;
			xlog_rec_buff_offset += nbytes;

			is_first_page = false;
			continue;
		}
		else 
		{
			uint64 data_len;
			nbytes = read_file2buff(src_fd, tmp_buff, SizeOfXLogRecord, 0, src_file_name);
			if (nbytes == 0)
				break;
			
			read_in_total += nbytes;

			record = (XLogRecord*) tmp_buff;
			memcpy((char*) xlog_rec_buffer + xlog_rec_buff_offset, tmp_buff, SizeOfXLogRecord);
			xlog_rec_buff_offset += SizeOfXLogRecord;

			if (! XlogRecFitsOnPage(current_file_pos + read_in_total, record->xl_tot_len - SizeOfXLogRecord))
				data_len = BLCKSZ * (1 + (current_file_pos + read_in_total) / BLCKSZ) - current_file_pos - read_in_total;
			else
				data_len = record->xl_tot_len - SizeOfXLogRecord;
			
			nbytes = read_file2buff(src_fd, xlog_rec_buffer, data_len, xlog_rec_buff_offset, src_file_name);
			if (nbytes == 0)
				break;
			
			read_in_total += nbytes;
			xlog_rec_buff_offset += nbytes;

			if (data_len == (record->xl_tot_len - SizeOfXLogRecord))
			{
				lseek(src_fd, MAXALIGN(record->xl_tot_len) - SizeOfXLogRecord - data_len, SEEK_CUR);
				// read_in_total += MAXALIGN(record->xl_tot_len) - SizeOfXLogRecord - data_len;
				return read_in_total;
			}
			
			is_first_page = false;
			continue;
		}
	}
	return -1;
}

static void
write_one_xlog_rec(int dst_fd, const char* dst_file_name, char* xlog_rec_buffer, uint64* file_offset)
{
	int 		nbytes;
	uint64 		rem_data_len = 0;
	XLogRecord* record;
	pg_crc32c	crc;
	uint64 		already_read = 0;

	/* Use it when we need to put padding into file */
	char 		null_buff[1024];
	memset(null_buff, 0, 1024);

	while (true) 
	{
		if (*file_offset % BLCKSZ == 0)
		{
			if (*file_offset == 0)
			{
				XLogLongPageHeaderData long_hdr;

				long_hdr.xlp_sysid 		  = sys_id;
				long_hdr.xlp_seg_size	  = WalSegSz;
				long_hdr.xlp_xlog_blcksz  = XLOG_BLCKSZ;

				long_hdr.std.xlp_info 	  = 0;
				long_hdr.std.xlp_info 	  |= XLP_LONG_HEADER;
				long_hdr.std.xlp_tli 	  = tli;
				long_hdr.std.xlp_rem_len  = 0;
				long_hdr.std.xlp_magic 	  = XLOG_PAGE_MAGIC;
				long_hdr.std.xlp_pageaddr = page_addr;

				ereport(LOG, errmsg("LONG HDR. ADDR : %ld", long_hdr.std.xlp_pageaddr));

				nbytes = write_buff2file(dst_fd, (char*) &long_hdr, SizeOfXLogLongPHD, 0);
				*file_offset += nbytes;
			}
			else
			{
				XLogPageHeaderData hdr;

				hdr.xlp_info 	 = 0;
				hdr.xlp_info 	|= XLP_BKP_REMOVABLE;
				if (rem_data_len > 0)
					hdr.xlp_info |= XLP_FIRST_IS_CONTRECORD;
				hdr.xlp_rem_len = rem_data_len;
				
				hdr.xlp_tli 	 = tli;
				hdr.xlp_magic 	 = XLOG_PAGE_MAGIC;
				hdr.xlp_pageaddr = page_addr + *file_offset;

				nbytes = write_buff2file(dst_fd, (char*) &hdr, SizeOfXLogShortPHD, 0);
				*file_offset += nbytes;
			}
		}
		if (rem_data_len > 0)
		{
			if (! XlogRecFitsOnPage(*file_offset, rem_data_len))
			{
				uint64 data_len = BLCKSZ * (1 + (*file_offset / BLCKSZ)) - *file_offset;
				nbytes = write_buff2file(dst_fd, xlog_rec_buffer, data_len, already_read);
				rem_data_len = (rem_data_len - data_len);
				
				*file_offset += nbytes;
				already_read += nbytes;

				continue;
			}
			else
			{
				nbytes = write_buff2file(dst_fd, xlog_rec_buffer, rem_data_len, already_read);
				*file_offset += nbytes;

				if (MAXALIGN(record->xl_tot_len) - record->xl_tot_len)
				{
					nbytes = write_buff2file(dst_fd, null_buff, MAXALIGN(record->xl_tot_len) - record->xl_tot_len, 0);
					*file_offset += nbytes;
				}
				return;
			}
		}

		record = (XLogRecord*) xlog_rec_buffer;
		record->xl_prev = prev_record + page_addr;
		prev_record = *file_offset;

		INIT_CRC32C(crc);
		COMP_CRC32C(crc, ((char *) record) + SizeOfXLogRecord, record->xl_tot_len - SizeOfXLogRecord);
		COMP_CRC32C(crc, (char *) record, offsetof(XLogRecord, xl_crc));
		FIN_CRC32C(crc);
		record->xl_crc = crc;

		/* TODO delete it later */
		// ereport(LOG, errmsg("Prev lsn : %X/%X\tTotal len : %d\tTotal len aligned : %ld\t Current LSN : %X/%X", 
		// 						LSN_FORMAT_ARGS(record->xl_prev), 
		// 						record->xl_tot_len, 
		// 						MAXALIGN(record->xl_tot_len),
		// 						LSN_FORMAT_ARGS(*file_offset)));

		if (! XlogRecFitsOnPage(*file_offset, record->xl_tot_len))
		{
			uint64 data_len = BLCKSZ * (1 + (*file_offset / BLCKSZ)) - *file_offset;
			rem_data_len = record->xl_tot_len - data_len;
			nbytes = write_buff2file(dst_fd, xlog_rec_buffer, data_len, 0);

			*file_offset += nbytes;
			already_read += nbytes;
			continue;
		}
		else
		{
			nbytes = write_buff2file(dst_fd, xlog_rec_buffer, record->xl_tot_len, 0);
			*file_offset += nbytes;

			if (MAXALIGN(record->xl_tot_len) - record->xl_tot_len > 0)
			{
				nbytes = write_buff2file(dst_fd, null_buff, MAXALIGN(record->xl_tot_len) - record->xl_tot_len, 0);
				*file_offset += nbytes;
			}
			return;
		}
	}
}

static void 
copy_file_part(const char* src, const char* dst_name, int dstfd, uint64 size, uint64 src_offset, 
			   char* tmp_buffer, char* xlog_rec_buffer, XLogRecPtr* file_offset)
{
	int      	 srcfd;
	int64 		 read_left = size;

	/*
	* Open the file
	*/
	srcfd = OpenTransientFile(src, O_RDONLY | PG_BINARY);
	if (srcfd < 0)
	ereport(ERROR,
		(errcode_for_file_access(),
			errmsg("could not open file \"%s\": %m", src)));

	if (lseek(srcfd, src_offset, SEEK_SET) < 0)
	ereport(ERROR,
		(errcode_for_file_access(),
			errmsg("could not navigate through the file \"%s\": %m", src)));

	/*
	* Do the data copying.
	*/
	while(true)
	{
		int read_len = read_one_xlog_rec(srcfd, src, xlog_rec_buffer, tmp_buffer);
		if (read_len == -1)
			break;
		
		write_one_xlog_rec(dstfd, dst_name, xlog_rec_buffer, file_offset);
		read_left -= read_len;
		if (read_left <= 0)
			break;
	}

	if (CloseTransientFile(srcfd) != 0)
	ereport(ERROR,
		(errcode_for_file_access(),
			errmsg("could not close file \"%d\": %m", dstfd)));
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

	continuous_reading_wal_file(xlogreader_state, &private, path, wal_diff_file, page_addr);

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

			copy_file_part(orig_wal_file_name, wal_diff_file_name, dst_fd, 
						   seg_start - global_offset, 
						   global_offset - initial_file_offset, 
						   tmp_buffer, 
						   xlog_rec_buffer,
						   &wal_diff_file_offset);

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
 * create_wal_diff
 *
 * Creates one WAL-diff file.
 */
static bool 
create_wal_diff(const char *src, const char *dest)
{


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
