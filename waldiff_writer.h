/*-------------------------------------------------------------------------
 *
 * waldiff_writer.h
 *	  Definitions for the WALDIFF writing facility
 * 
 *-------------------------------------------------------------------------
 */
#ifndef _WALDIFF_WRITER_H_
#define _WALDIFF_WRITER_H_

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

typedef struct WaldiffWriter {
	WALOpenSegment seg;
	WALSegmentContext segcxt;
	uint64 sysid;

	XLogRecPtr	WriteRecPtr;		/* end+1 of last written record */

	char *writeBuf;
	Offset writeBufSize;

	char	   *writeRestRecordBuf;		/* used when a record crosses a page boundary*/
	uint32		writeRestRecordBufSize;
} WaldiffWriter;


extern WaldiffWriter *
WaldiffWriterAllocate(char *waldiff_dir, int wal_segment_size);

extern void 
WaldiffWriterFree(WaldiffWriter *writer);

extern void 
WaldiffBeginWriting(WaldiffWriter *writer, uint64 sysid, 
					XLogSegNo segNo, TimeLineID tli);

extern void 
WaldiffWriterWrite(WaldiffWriter *writer, XLogRecord *record);


#define DoesWaldiffWriterFinishedSegment(writer) 					\
({																	\
	((writer)->WriteRecPtr % (writer)->segcxt.ws_segsize == 0) ?	\
		true : false;												\
})

#endif /* _WALDIFF_WRITER_H_ */
