#include "waldiff_reader.h"

WaldiffReader *WaldiffReaderAllocate(char *wal_dir, 
									 int wal_segment_size);

void WaldiffReaderFree(WaldiffReader *reader);

void 
WaldiffBeginReading(WaldiffReader *reader, XLogSegNo segNo, TimeLineID tli);

void 
WaldiffReaderRead(WaldiffReader *reader);

WALDIFFRecord
WalDiffDecodeRecord(char *record, XLogRecPtr lsn);


WaldiffReader *
WaldiffReaderAllocate(char *wal_dir, int wal_segment_size)
{
    WaldiffReader *reader;

	reader = (WaldiffReader *)
		palloc_extended(sizeof(WaldiffReader),
						MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);

	if (!reader)
		return NULL;

	reader->readBuf = (char *) palloc_extended(XLOG_BLCKSZ,
											    MCXT_ALLOC_NO_OOM);
	if (!reader->readBuf)
	{
		pfree(reader);
		return NULL;
	}

	reader->ReadRecPtr = 0;
	reader->readBufSize = 0;
	memcpy(reader->segcxt.ws_dir, wal_dir, strlen(wal_dir));
	reader->segcxt.ws_segsize = wal_segment_size;  

	allocate_rest_record_buf(reader, 0);
	reader->readRestRecordBufSize = 0;
	return reader;
}

void 
WaldiffReaderFree(WaldiffReader *reader)
{
    if (reader->seg.ws_file != -1)
		WaldiffCloseSegment(reader);

	pfree(reader->readBuf);
	pfree(reader);
}

void 
WaldiffBeginReading(WaldiffReader *reader, XLogSegNo segNo, TimeLineID tli)
{
    if (reader->seg.ws_file != -1)
		WaldiffCloseSegment(reader);

	Assert(reader->readBufSize == 0);

	XLogSegNoOffsetToRecPtr(segNo, 0, reader->segcxt.ws_segsize, reader->ReadRecPtr);

	WaldiffOpenSegment(reader, segNo, &tli);
}

void 
WaldiffReaderRead(WaldiffReader *reader)
{
    
	// add validation of crc
}



void 
WalOpenSegment(XLogReaderState *reader,
			   XLogSegNo nextSegNo,
			   TimeLineID *tli_p)
{
	TimeLineID tli = *tli_p;
    char fname[XLOG_FNAME_LEN];
	char fpath[MAXPGPATH];

    XLogFileName(fname, tli, nextSegNo, reader->segcxt.ws_segsize);

	if (snprintf(fpath, MAXPGPATH, "%s/%s", reader->segcxt.ws_dir, fname) == -1)
		ereport(ERROR,
				errmsg("WALDIFF: error during reading WAL absolute path : %s/%s", reader->segcxt.ws_dir, fname));

	reader->seg.ws_file = OpenTransientFile(fpath, PG_BINARY | O_RDONLY);
	if (reader->seg.ws_file == -1)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("WALDIFF: could not open WAL segment \"%s\": %m", fpath)));
}

int 
WalReadPage(XLogReaderState *reader, XLogRecPtr targetPagePtr, int reqLen,
			XLogRecPtr targetPtr, char *readBuff)
{
	XLogReaderPrivate *private = reader->private_data;
	int				  count = XLOG_BLCKSZ;
	WALReadError 	  errinfo;

	if (private->endptr != InvalidXLogRecPtr)
	{
		if (targetPagePtr + XLOG_BLCKSZ <= private->endptr)
			count = XLOG_BLCKSZ;
		else if (targetPagePtr + reqLen <= private->endptr)
			count = private->endptr - targetPagePtr;
		else
		{
			private->endptr_reached = true;
			return -1;
		}
	}

	if (!WALRead(reader, readBuff, targetPagePtr, count, private->timeline,
				 &errinfo))
	{
		WALOpenSegment *seg = &errinfo.wre_seg;
		char		   fname[MAXPGPATH];

		XLogFileName(fname, seg->ws_tli, seg->ws_segno,
					 reader->segcxt.ws_segsize);

		if (errinfo.wre_errno != 0)
		{
			errno = errinfo.wre_errno;
			ereport(ERROR, 
					errmsg("WALDIFF: could not read from file %s, offset %d: %m",
					fname, errinfo.wre_off));
		}
		else
			ereport(ERROR,
					errmsg("WALDIFF: could not read from file %s, offset %d: read %d of %d",
					fname, errinfo.wre_off, errinfo.wre_read,
					errinfo.wre_req));
	}

	return count;
}