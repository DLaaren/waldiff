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


#define MAX_ERRORMSG_LEN 1024

/* Structure with old and new contexts */
typedef struct MemoryContextStorage
{
	MemoryContext old;
	MemoryContext current;
} MemoryContextStorage;

/* WALDIFFSegment represents a WALDIFF segment being written */
typedef struct WALDIFFSegment
{
	int		fd; 		/* segment file descriptor */
	XLogSegNo	segno;		/* segment number */
	TimeLineID	tli;		/* timeline ID of the currently open file */
} WALDIFFSegment;

/* WALDIFFSegmentContext carries context information about WALDIFF segments */
typedef struct WALDIFFSegmentContext
{
	char		*dir;
	int			segsize;
} WALDIFFSegmentContext;

/* Structure representing XLogRecord block data */
typedef struct WALDIFFBlock {
	
	XLogRecordBlockHeader blk_hdr;

	/* Identify the block this refers to */
	RelFileLocator file_loc;
	ForkNumber	   forknum;
	BlockNumber    blknum;

	/* we are not working with images */

	/* Buffer holding the rmgr-specific data associated with this block */
	bool		has_data;
	char	   *block_data;
	uint16		block_data_len;

} WALDIFFBlock;

/* Structure representing folded WAL records */
typedef struct WALDIFFRecordData
{
	uint8 			type;

	XLogRecPtr	    lsn;
	XLogRecord      rec_hdr;	

	/* 
	 * If some of them not used (for example. insert does not need t_xmax), 
	 * they will be NULL during fetching.
	 */
	TransactionId   t_xmin;
	TransactionId   t_xmax;
	CommandId	    t_cid;

	/* Pointer to latest tuple version */
	ItemPointerData current_t_ctid;
	/* In delete/update case this is the pointer on deleted tuple version */
	ItemPointerData prev_t_ctid;	

	xl_heap_update   *main_data;
	uint32 main_data_len;

	/* highest block_id (-1 if none) */
	int          max_block_id;
	WALDIFFBlock blocks[FLEXIBLE_ARRAY_MEMBER];

} WALDIFFRecordData;

typedef WALDIFFRecordData *WALDIFFRecord;

#define SizeOfWALDIFFRecord offsetof(WALDIFFRecordData, blocks)


#endif /* _WALDIFF_H_ */
