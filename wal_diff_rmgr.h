#ifndef _WALDIFF_RMGR_H_
#define _WALDIFF_RMGR_H_

#include "postgres.h"

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
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

#endif
