#include "waldiff_writer.h"


WALDIFFWriterState *
WALDIFFWriterAllocate(int wal_segment_size,
					  char *waldiff_dir,
					  WALDIFFWriterRoutine *routine)
{
	WALDIFFWriterState *writer;
	writer = (WALDIFFWriterState *)
		palloc_extended(sizeof(WALDIFFWriterState),
						MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
	if (!writer)
		return NULL;

	writer->routine = *routine;

	writer->writeBuf =(char *) palloc_extended(XLogRecordMaxSize,
											  MCXT_ALLOC_NO_OOM);

	if (!writer->writeBuf)
	{
		pfree(writer);
		return NULL;
	}					

	writer->seg.fd = -1;
	writer->seg.segno = 0;
	writer->seg.tli = 0;

	writer->segcxt.segsize = wal_segment_size;
	if (waldiff_dir)
		writer->segcxt.dir = waldiff_dir;

	/* ReadRecPtr, EndRecPtr and readLen initialized to zeroes above */
	writer->errormsg_buf = palloc_extended(MAX_ERRORMSG_LEN + 1,
										   MCXT_ALLOC_NO_OOM);
	if (!writer->errormsg_buf)
	{
		pfree(writer->writeBuf);
		pfree(writer);
		return NULL;
	}
	writer->errormsg_buf[0] = '\0';

	if (writer->writeBuf)
		pfree(writer->writeBuf);
	writer->writeBuf = (char *) palloc(BLCKSZ);
	writer->writeBuf[0] = '\0';
	writer->writeBufSize = 0;

	return writer;						
}

void 
WALDIFFWriterFree(WALDIFFWriterState *writer)
{
	if (writer->writeBufSize > 0)
		WALDIFFFlushBuffer(writer);

	if (writer->seg.fd != -1)
		writer->routine.segment_close(&(writer->seg));

	pfree(writer->errormsg_buf);
	pfree(writer->writeBuf);
	pfree(writer);
}

void 
WALDIFFBeginWrite(WALDIFFWriterState *writer,
				  XLogRecPtr RecPtr, 
				  XLogSegNo segNo, 
				  TimeLineID tli)
{
	Assert(!XLogRecPtrIsInvalid(RecPtr));

	writer->StartRecPtr = InvalidXLogRecPtr;
	writer->EndRecPtr = RecPtr;

	writer->seg.segno = segNo;
	writer->seg.tli = tli;

	writer->routine.segment_open(&(writer->segcxt), &(writer->seg));
}

WALDIFFRecordWriteResult 
WALDIFFFlushBuffer(WALDIFFWriterState *writer)
{
	int written_bytes = 0;

	Assert(WALDIFFWriterGetBufSize(writer) >= 0);

	ereport(LOG, errmsg("Writing to WALDIFF segment; curr buff size: %u", writer->writeBufSize));

	written_bytes = write(writer->seg.fd, WALDIFFWriterGetBuf(writer), WALDIFFWriterGetBufSize(writer));

	if (written_bytes != WALDIFFWriterGetBufSize(writer))
	{
		ereport(ERROR, 
				(errcode_for_file_access(),
				errmsg("error while writing to WALDIFF segment in WALDIFFFlushBuffer() : %m")));
		snprintf(writer->errormsg_buf, MAX_ERRORMSG_LEN, 
				 "write() returns: %d, but expected: %d",
				 written_bytes, WALDIFFWriterGetBufSize(writer));
		return WALDIFFWRITE_FAIL;
	}
	
	ereport(LOG, errmsg("Wrote %d bytes to WALDIFF segment", written_bytes));

	pg_fsync(writer->seg.fd);

	ereport(LOG, errmsg("WALDIFF old StartRecPtr: %lu", writer->StartRecPtr));
	ereport(LOG, errmsg("WALDIFF old EndRecPtr: %lu", writer->EndRecPtr));

	writer->StartRecPtr = writer->EndRecPtr;
	writer->EndRecPtr += written_bytes;

	ereport(LOG, errmsg("WALDIFF new StartRecPtr: %lu", writer->StartRecPtr));
	ereport(LOG, errmsg("WALDIFF new EndRecPtr: %lu", writer->EndRecPtr));

	return WALDIFFWRITE_SUCCESS;
}
