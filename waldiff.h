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
	int			fd; 		/* segment file descriptor */
	XLogSegNo	segno;		/* segment number */
	TimeLineID	tli;		/* timeline ID of the currently open file */
} WALDIFFSegment;

/* WALDIFFSegmentContext carries context information about WALDIFF segments */
typedef struct WALDIFFSegmentContext
{
	char		*dir;
	int			segsize;
} WALDIFFSegmentContext;

/* Structure representing folded WAL records */
typedef struct WALDIFFRecordData
{
	XLogRecord      rec_hdr;	

	TransactionId   t_xmin;
	TransactionId   t_xmax;
	CommandId	    t_cid;

	/* Pointer to latest tuple version */
	ItemPointerData current_t_ctid;

	/* In delete/update case this is the pointer on deleted tuple version */
	ItemPointerData prev_t_ctid;	

	ForkNumber 		forknum;	
	RelFileLocator 	file_loc;
	BlockNumber		blck_num;

	/* Offset to user data */
	uint8			t_hoff;
	uint16			t_infomask;	
	uint16			t_infomask2;
	/* Size of bitmap + padding + header + prefix length + suffix length + user_data */
	uint32 			data_len;

	/*
	 * Here comes [bitmap] + [padding] and then header + user_data.
	 * In update case 'user_data' also includes prefix and suffix lengths
	 * that comes right after 'xl_heap_update'
	 */
	bits8			t_bits[FLEXIBLE_ARRAY_MEMBER];

} WALDIFFRecordData;

typedef WALDIFFRecordData *WALDIFFRecord;

#define SizeOfChainRecord offsetof(WALDIFFRecordData, t_bits)


#endif /* _WALDIFF_H_ */
