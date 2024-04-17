#ifndef _WALDIFF_RMGR_H_
#define _WALDIFF_RMGR_H_

#include "postgres.h"

#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/visibilitymapdefs.h"
#include "access/visibilitymap.h"
#include "access/xlogutils.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "storage/freespace.h"
#include "fmgr.h"
#include "utils/pg_lsn.h"
#include "varatt.h"

#define WALDIFF_RM_ID       RM_EXPERIMENTAL_ID
#define WALDIFF_RM_NAME     "wal_diff_rmgr"
#define XLOG_TEST_CUSTOM_RMGRS_MESSAGE	0x00

extern RmgrData waldiff_rmgr;

extern void 		waldiff_rmgr_redo(XLogReaderState *record);
extern void 		waldiff_rmgr_desc(StringInfo buf, XLogReaderState *record);
extern const char* 	waldiff_rmgr_identify(uint8 info);

#define getRmIdentity(xlog_record_header) ( (xlog_record_header.xl_info & ~XLR_INFO_MASK) & \
                                             XLOG_HEAP_OPMASK)

#define setRmId(xlog_record_header, rm_id) ( xlog_record_header.xl_rmid = rm_id)
// XLOG_HEAP_OPMASK = 01110000   
// зануляем биты отведенные на identity -> ~XLOG_HEAP_OPMASK = 10001111
#define setRmIdentity(xlog_record_header, rm_identity) \
( \
	xlog_record_header.xl_info = \
	xlog_record_header.xl_info & \
	~XLOG_HEAP_OPMASK | rm_identity \
)

#endif
