/*-------------------------------------------------------------------------
 *
 * waldiff.h
 *	  Primary include file for WALDIFF extenstion .c files
 * 
 *-------------------------------------------------------------------------
 */
#ifndef _WALDIFF_H_
#define _WALDIFF_H_

#include "postgres.h"

/* system stuff */
#include <assert.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

/* postgreSQL stuff */
#include "access/heapam_xlog.h"
#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "access/xlogstats.h"
#include "access/xlogutils.h"
#include "access/xlog_internal.h"
#include "archive/archive_module.h"
#include "common/int.h"
#include "common/logging.h"
#include "common/hashfn.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "storage/copydir.h"
#include "storage/fd.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/hsearch.h"
#include "utils/wait_event.h"

/* Structure with old and new contexts */
typedef struct MemoryContextStorage
{
	MemoryContext *old;
	MemoryContext *current;
} MemoryContextStorage;

/* WALDIFFSegment represents a WALDIFF segment being written */
typedef struct WALDIFFOpenSegment
{
	int			wds_fd; 		/* segment file descriptor */
	XLogSegNo	wds_segno;		/* segment number */
	TimeLineID	wds_tli;		/* timeline ID of the currently open file */
} WALDIFFOpenSegment;

/* WALDIFFSegmentContext carries context information about WALDIFF segments */
typedef struct WALDIFFSegmentContext
{
	char		*wds_dir;
	int			wds_segsize;
} WALDIFFSegmentContext;

/* Structure representing folded WAL records */
typedef struct WALDIFFRecordData
{

} WALDIFFRecordData;

typedef WALDIFFRecordData *WALDIFFRecord;


#endif /* _WALDIFF_H_ */
