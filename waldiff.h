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
#include "utils/lsyscache.h"

/*--------------------------Public defines-------------------------*/
typedef struct WaldiffBlock 
{
	XLogRecordBlockHeader blk_hdr;

	RelFileLocator file_loc;
	ForkNumber	   forknum;
	BlockNumber    blknum;

	bool		has_data;
	char	   *block_data;
	uint16		block_data_len;
} WaldiffBlock;

typedef struct WaldiffRecordData
{
	uint32   		chain_length; // for debug and test purposes
	XLogRecPtr	    chain_start_lsn;

	XLogRecord      rec_hdr;	

	/* 
	 * If some of them not used (for example. insert does not need t_xmax), 
	 * they will be NULL during fetching.
	 */
	TransactionId   t_xmin;
	TransactionId   t_xmax;

	/* Pointer to latest tuple version */
	ItemPointerData current_t_ctid;
	/* In delete/update case this is the pointer on deleted tuple version */
	ItemPointerData prev_t_ctid;	

	bool	has_main_data;
	bool	is_long_header;
	char   *main_data;
	uint32  main_data_len;

	/* highest block_id (-1 if none) */
	int          max_block_id;
	WaldiffBlock blocks[FLEXIBLE_ARRAY_MEMBER];

} WaldiffRecordData;

typedef WaldiffRecordData *WaldiffRecord;

#define SizeOfWaldiffRecord offsetof(WaldiffRecordData, blocks)
#define GetRecordLength(WaldiffRec) ((WaldiffRec)->rec_hdr.xl_tot_len)
#define GetRecordType(WaldiffRec) ((WaldiffRec)->rec_hdr.xl_info & XLOG_HEAP_OPMASK)

/*-------------------Functionality for hash keys-------------------*/
#define ROTL32(x, y) ((x << y) | (x >> (32 - y)))

inline uint32_t 
_hash_combine(uint32_t seed, uint32_t value)
{
    return seed ^ (ROTL32(value, 15) + 0x9e3779b9 + (seed << 6) + (seed >> 2));
}

#define GetHashKeyOfWaldiffRecord(record) 													\
({ 																							\
    uint32_t key = 0;																		\
	Assert(record != NULL);																	\
    key = _hash_combine(key, (record)->blocks[0].file_loc.spcOid); 							\
    key = _hash_combine(key, (record)->blocks[0].file_loc.dbOid); 							\
    key = _hash_combine(key, (record)->blocks[0].file_loc.relNumber); 						\
	key = _hash_combine(key, (record)->blocks[0].forknum); 									\
	key = _hash_combine(key, BlockIdGetBlockNumber(&((record)->current_t_ctid.ip_blkid)));	\
    key = _hash_combine(key, (record)->current_t_ctid.ip_posid); 							\
    key + 1; 																				\
})

#define GetHashKeyOfPrevWaldiffRecord(record) 											\
({ 																						\
    uint32_t key = 0; 																	\
	Assert(record != NULL);																\
    key = _hash_combine(key, (record)->blocks[0].file_loc.spcOid); 						\
    key = _hash_combine(key, (record)->blocks[0].file_loc.dbOid); 						\
    key = _hash_combine(key, (record)->blocks[0].file_loc.relNumber); 					\
	key = _hash_combine(key, (record)->blocks[0].forknum); 								\
    key = _hash_combine(key, BlockIdGetBlockNumber(&((record)->prev_t_ctid.ip_blkid)));	\
    key = _hash_combine(key, (record)->prev_t_ctid.ip_posid); 							\
    key + 1; 																			\
})

#endif /* _WALDIFF_H_ */
