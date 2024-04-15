#ifndef _WAL_DIFF_FUNC_H_
#define _WAL_DIFF_FUNC_H_

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

/**********************************************************************
 * Neccessary info for writing wal-diff segment
 **********************************************************************/
typedef struct WalDiffWriterState {
	int 		src_fd;
	int 		dest_fd;

	// decided to store it all as pointers otherwise it requires kinda a lot of memory -> 1024*6
	const char 	*src_path;
	char 		*src_dir;
	char 		*dest_path;
	char 		*dest_dir;
	const char 	*fname;

	int 		wal_segment_size;

	uint64 		sys_id;
	XLogRecPtr  page_addr;
	TimeLineID  tli;

	uint64 		src_curr_offset;
	uint64		dest_curr_offset;

	XLogRecPtr	last_read_rec;
	Size		last_read_rec_len;

	XLogRecPtr	last_written_rec;
	Size		last_written_rec_len;

} WalDiffWriterState;

typedef struct XLogDumpPrivate
{
	TimeLineID	timeline;
	XLogRecPtr	startptr;
	XLogRecPtr	endptr;
	bool		endptr_reached;
} XLogDumpPrivate;

/**********************************************************************
 * All functions we need to copy records from wal segment
 * to wal_diff segment (except those that we worked with)
 **********************************************************************/

extern void 		copy_file_part(uint64 size, uint64 src_offset, char* tmp_buffer, char* xlog_rec_buffer);
extern int			read_one_xlog_rec(int src_fd, const char* src_file_name, char* xlog_rec_buffer, 
									  char* tmp_buff, uint64 read_left, bool* read_only_header);
extern void			write_one_xlog_rec(int dst_fd, const char* dst_file_name, char* xlog_rec_buffer);
extern int 			read_file2buff(int fd, char* buffer, uint64 size, uint64 buff_offset, const char* file_name);
extern int 			write_buff2file(int fd, char* buffer, uint64 size, uint64 buff_offset);

/*
 * Whether XLogRecord fits on the page with given offset
 */
#define XlogRecHdrFitsOnPage(in_page_offset) \
( \
	(in_page_offset) + SizeOfXLogRecord < \
	BLCKSZ * (1 + (in_page_offset) / BLCKSZ) \
)

/*
 * Whether record with given length fits on the page with given offset
 */
#define XlogRecFitsOnPage(in_page_offset, rec_len) \
( \
	(in_page_offset) + (rec_len) < \
	BLCKSZ * (1 + (in_page_offset) / BLCKSZ) \
)

/**********************************************************************
 * Global data
 **********************************************************************/

extern WalDiffWriterState writer_state;

/**********************************************************************
 * All functions we need to init XLogReaderState
 **********************************************************************/

extern int 	WalReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
						XLogRecPtr targetPtr, char *readBuff);
extern void WalOpenSegment(XLogReaderState *state, XLogSegNo nextSegNo, TimeLineID *tli_p);
extern void WalCloseSegment(XLogReaderState *state);
extern void getWalDirecotry(const char *path, const char *file);
extern void XLogDisplayRecord(XLogReaderState *record);

#endif
