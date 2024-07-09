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
#define WALDIFF_RM_NAME     "waldiff_rmgr"
#define WALDIFF_RMGR_PLUG   0x00
#define XLOG_TEST_CUSTOM_RMGRS_MESSAGE	0x00

extern RmgrData waldiff_rmgr;

extern void 		waldiff_rmgr_redo(XLogReaderState *record);
extern void 		waldiff_rmgr_desc(StringInfo buf, XLogReaderState *record);
extern const char* 	waldiff_rmgr_identify(uint8 info);

typedef struct waldiff_rmgr_plug
{
    unsigned offset:15,
             flag:1;
} waldiff_rmgr_plug;

#endif /* _WALDIFF_RMGR_H_ */