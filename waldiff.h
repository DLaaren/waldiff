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

#define WALDIFF_DEBUG

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
	uint32   		chain_length;
	uint8 			type;

	XLogRecPtr	    lsn;
	XLogRecord      rec_hdr;	

	/* 
	 * If some of them not used (for example. insert does not need t_xmax), 
	 * they will be NULL during fetching.
	 */
	TransactionId   t_xmin;
	TransactionId   t_xmax;
	// TODO get t_cid from decoded record 
	CommandId		t_cid;

	/* Pointer to latest tuple version */
	ItemPointerData current_t_ctid;
	/* In delete/update case this is the pointer on deleted tuple version */
	ItemPointerData prev_t_ctid;	

	char   *main_data;
	uint32 main_data_len;

	/* highest block_id (-1 if none) */
	int          max_block_id;
	WALDIFFBlock blocks[FLEXIBLE_ARRAY_MEMBER];

} WALDIFFRecordData;

typedef WALDIFFRecordData *WALDIFFRecord;

#define SizeOfWALDIFFRecord offsetof(WALDIFFRecordData, blocks)


/*-------------------Functionality for hash keys-------------------*/
#define ROTL32(x, y) ((x << y) | (x >> (32 - y)))

inline uint32_t 
_hash_combine(uint32_t seed, uint32_t value)
{
    return seed ^ (ROTL32(value, 15) + 0x9e3779b9 + (seed << 6) + (seed >> 2));
}

#define GetHashKeyOfWALDIFFRecord(record) 													\
({ 																							\
    uint32_t key = 0; 																		\
    key = _hash_combine(key, (record)->blocks[0].file_loc.spcOid); 							\
    key = _hash_combine(key, (record)->blocks[0].file_loc.dbOid); 							\
    key = _hash_combine(key, (record)->blocks[0].file_loc.relNumber); 						\
	key = _hash_combine(key, BlockIdGetBlockNumber(&((record)->current_t_ctid.ip_blkid)));	\
    key = _hash_combine(key, (record)->current_t_ctid.ip_posid); 							\
    key + 1; 																				\
})

#define GetHashKeyOfPrevWALDIFFRecord(record) 											\
({ 																						\
    uint32_t key = 0; 																	\
    key = _hash_combine(key, (record)->blocks[0].file_loc.spcOid); 						\
    key = _hash_combine(key, (record)->blocks[0].file_loc.dbOid); 						\
    key = _hash_combine(key, (record)->blocks[0].file_loc.relNumber); 					\
    key = _hash_combine(key, BlockIdGetBlockNumber(&((record)->prev_t_ctid.ip_blkid)));	\
    key = _hash_combine(key, (record)->prev_t_ctid.ip_posid); 							\
    key + 1; 																			\
})

#define GetHashKeyOfWALRecord(record) 															\
({ 																								\
    uint32_t key = 0; 																			\
    key = _hash_combine(key, (record)->blocks[0].rlocator.spcOid); 								\
    key = _hash_combine(key, (record)->blocks[0].rlocator.dbOid); 								\
    key = _hash_combine(key, (record)->blocks[0].rlocator.relNumber); 							\
    key = _hash_combine(key, (((record)->blocks[0].blkno)));									\
	switch (record->header.xl_info & XLOG_HEAP_OPMASK)											\
	{																							\
		case XLOG_HEAP_INSERT:																	\
			key = _hash_combine(key, ((xl_heap_insert *)((record)->main_data))->offnum); 	 	\
			break;																				\
		case XLOG_HEAP_HOT_UPDATE:																\
			key = _hash_combine(key, ((xl_heap_update *)((record)->main_data))->new_offnum);	\
			break;																				\
		default:																				\
			ereport(ERROR, errmsg("Unknown record type"));										\
			break;																				\
	}																							\
    key + 1; 																					\
})

#endif /* _WALDIFF_H_ */
