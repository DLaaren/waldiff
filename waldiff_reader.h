/*-------------------------------------------------------------------------
 *
 * waldiff_reader.h
 *	  Definitions for the WALDIFF reading facility
 * 
 *-------------------------------------------------------------------------
 */
#ifndef _WALDIFF_READER_H_
#define _WALDIFF_READER_H_

#include "postgres.h"

/* system stuff */
#include <assert.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

/* postgreSQL stuff */
#include "access/heapam_xlog.h"
#include "access/heaptoast.h"
#include "access/htup.h"
#include "access/xlog_internal.h"
#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "access/xlogstats.h"
#include "access/xlogutils.h"
#include "archive/archive_module.h"
#include "catalog/namespace.h"
#include "catalog/pg_control.h"
#include "common/hashfn.h"
#include "common/int.h"
#include "common/logging.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "executor/spi.h"
#include "storage/copydir.h"
#include "storage/fd.h"
#include "storage/large_object.h"
#include "storage/lwlock.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/wait_event.h"
#include "utils/relfilenumbermap.h"
#include "commands/dbcommands.h"
#include "postmaster/bgworker.h"

typedef struct WaldiffReader {
	WALOpenSegment seg;
	WALSegmentContext segcxt;
	uint64 sysid;

	XLogRecPtr	ReadRecPtr;		/* end+1 of last read record */

	char *readBuf;
	uint32 readBufSize;

	char	   *readRestRecordBuf;		/* used when a record crosses a page boundary*/
	uint32		readRestRecordBufSize;
} WaldiffReader;


extern WaldiffReader *WaldiffReaderAllocate(char *wal_dir, 
											int wal_segment_size);

extern void WaldiffReaderFree(WaldiffReader *reader);

extern void 
WaldiffBeginReading(WaldiffReader *reader, uint64 sysid, XLogSegNo segNo, TimeLineID tli);

extern XLogRecord * 
WaldiffReaderRead(WaldiffReader *reader, XLogRecPtr *lsn);

#endif /* _WALDIFF_READER_H_ */
