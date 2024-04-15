#include "wal_diff_rmgr.h"

/*
 * Redo is just a noop for this module, because we aren't testing recovery of
 * any real structure.
 */
void
waldiff_rmgr_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	XLogRecord main_data = record->record->header;
}

/*
 * No information on custom resource managers; just print the ID.
 */
void
waldiff_rmgr_desc(StringInfo buf, XLogReaderState *record)
{
	appendStringInfo(buf, "rmid: %d", XLogRecGetRmid(record));
}

/*
 * No information on custom resource managers; just return NULL and let the
 * caller handle it.
 */
const char *
waldiff_rmgr_identify(uint8 info)
{
	return NULL;
}
