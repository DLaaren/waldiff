/*-------------------------------------------------------------------------
 * 
 * Идея пока такая 
 * 
 * Сохраняем файл с логами во временное хранилище
 * Скидываем на диск сервера
 * Обрабатываем копию этого файла -- компрессуем wal записи
 * Скидываем на архив-диск теперь уже wal diff
 *
 * postgres/contrib/basic_archive
 * postgres/include/archive
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <assert.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include "access/xlogreader.h"
#include "access/xlog_internal.h"
#include "archive/archive_module.h"
#include "common/int.h"
#include "miscadmin.h"
#include "common/logging.h"
#include "storage/copydir.h"
#include "storage/fd.h"
#include "utils/guc.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

static char wal_directory[MAXPGPATH];
static char *wal_diff_directory = NULL;
static int	WalSegSz;

static bool check_archive_directory(char **newval, void **extra, GucSource source);
static bool create_wal_diff(const char *file, const char *destination);
static bool compare_files(const char *file1, const char *file2);
static bool is_file_archived(const char *file, const char *destination, const char *archive_directory);
static void wal_diff_startup(ArchiveModuleState *state);
static bool wal_diff_configured(ArchiveModuleState *state);
static bool wal_diff_archive(ArchiveModuleState *state, const char *file, const char *path);
static void wall_diff_shutdown(ArchiveModuleState *state);

typedef struct XLogDumpPrivate
{
	TimeLineID	timeline;
	XLogRecPtr	startptr;
	XLogRecPtr	endptr;
	bool		endptr_reached;
} XLogDumpPrivate;

static const ArchiveModuleCallbacks wal_diff_callbacks = {
    .startup_cb = wal_diff_startup,
	.check_configured_cb = wal_diff_configured,
	.archive_file_cb = wal_diff_archive,
	.shutdown_cb = wall_diff_shutdown
};

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

	snprintf(fpath, MAXPGPATH, "%s/%s", state->segcxt.ws_dir, fname);

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
	strcpy(wal_directory, path);
	memset(wal_directory + (char)(strlen(path) - strlen(file) - 1), 0, strlen(file));
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
	int fd = -1;
	PGAlignedXLogBlock buff; // local variable, holding a page buffer
    int read_count = 0;
    XLogDumpPrivate private;
	XLogPageHeader page_hdr;
	XLogSegNo segno;
	XLogRecPtr first_record;
	XLogReaderState* xlogreader_state;

	// snprintf(wal_diff_destination, MAXPGPATH, "%s/%s", wal_diff_directory, file);

	ereport(LOG, 
			errmsg("archiving file : %s", file));

	if (strlen(wal_directory) == 0)
		getWalDirecotry(wal_directory, path, file);

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
    else {
        ereport(ERROR,
				errmsg("Could not read file \"%s\": %m", path));
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

	if (xlogreader_state == NULL) 
	{
		ereport(FATAL, errmsg("out of memory while allocating a WAL reading processor"));
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
    if (XLogPageHeaderSize(page_hdr) == SizeOfXLogLongPHD)
        ereport(LOG, errmsg("Got long page header"));
    else
        ereport(LOG, errmsg("Got short page header"));

    ereport(LOG, errmsg("Remaining data from a previous page : %d", page_hdr->xlp_rem_len));








	// if (create_wal_diff(path, wal_diff_destination))
	// {
	// 	ereport(LOG, errmsg("created WAL-diff for file \"%s\"", file));
	// 	return true;
	// } 
	// else 
	// {
	// 	ereport(ERROR, errmsg("error while creating WAL-diff"));
	// 	return false;
	// }

	return true;
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
create_wal_diff(const char *file, const char *destination)
{

	
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
	void *data = state->private_data;

	if (data != NULL)
		pfree(data);

	state->private_data = NULL;
}
