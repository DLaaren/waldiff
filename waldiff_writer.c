#include "waldiff_writer.h"


WALDIFFWriterState *
WALDIFFWriterAllocate(int wal_segment_size,
					  char *waldiff_dir,
					  char* wal_dir,
					  WALDIFFWriterRoutine *routine,
					  Size buffer_capacity)
{
	WALDIFFWriterState *writer;
	writer = (WALDIFFWriterState*) palloc0(sizeof(WALDIFFWriterState));

	writer->routine = *routine;

	writer->buffer = (char*) palloc0(buffer_capacity);
	writer->buffer_fullness = 0;
	writer->already_written = 0;

	writer->wal_seg.fd = writer->waldiff_seg.fd = -1;
	writer->wal_seg.segno = writer->waldiff_seg.segno = 0;
	writer->wal_seg.tli = writer->waldiff_seg.tli= 0;

	writer->first_page_addr = 0;
	writer->last_record_written = InvalidXLogRecPtr;

	writer->wal_seg.segsize = writer->waldiff_seg.segsize= wal_segment_size;

	if (waldiff_dir)
	{
		int dir_name_len = strlen(waldiff_dir);
		writer->waldiff_seg.dir = (char*) palloc0(sizeof(char) * (dir_name_len + 1));
		memcpy(writer->waldiff_seg.dir, waldiff_dir, dir_name_len);
		writer->waldiff_seg.dir[dir_name_len] = '\0';
	}
	else
		writer->waldiff_seg.dir = NULL;
	
	if (wal_dir)
	{
		int dir_name_len = strlen(wal_dir);
		writer->wal_seg.dir = (char*) palloc0(sizeof(char) * (dir_name_len + 1));
		memcpy(writer->wal_seg.dir, wal_dir, dir_name_len);
		writer->wal_seg.dir[dir_name_len] = '\0';
	}
	else
		writer->wal_seg.dir = NULL;

	writer->errormsg_buf = palloc0(MAX_ERRORMSG_LEN + 1);

	return writer;						
}

void 
WALDIFFWriterFree(WALDIFFWriterState *writer)
{
	if (writer->buffer_fullness > 0)
		WALDIFFFlushBuffer(writer);

	if (writer->wal_seg.fd != -1)
		writer->routine.segment_close(&(writer->wal_seg));
	
	if (writer->waldiff_seg.fd != -1)
		writer->routine.segment_close(&(writer->waldiff_seg));
	
	if (writer->wal_seg.dir != NULL)
		pfree(writer->wal_seg.dir);

	if (writer->waldiff_seg.dir != NULL)
		pfree(writer->waldiff_seg.dir);

	pfree(writer->errormsg_buf);
	pfree(writer->buffer);
	pfree(writer);
}

void 
WALDIFFBeginWrite(WALDIFFWriterState *writer,
				  XLogSegNo segNo, 
				  TimeLineID tli)
{
	writer->wal_seg.segno = writer->waldiff_seg.segno = segNo;
	writer->wal_seg.tli = writer->waldiff_seg.tli = tli;

	writer->routine.segment_open(&(writer->wal_seg));
	writer->routine.segment_open(&(writer->waldiff_seg));
}

WALDIFFRecordWriteResult 
WALDIFFFlushBuffer(WALDIFFWriterState *writer)
{
	int written_bytes = 0;

	Assert(writer->buffer_fullness >= 0);

	ereport(LOG, errmsg("Writing to WALDIFF segment; curr buff size: %lu", writer->buffer_fullness));

	written_bytes = write(writer->waldiff_seg.fd, writer->buffer, writer->buffer_fullness);

	if (written_bytes != writer->buffer_fullness)
	{
		ereport(ERROR, 
				(errcode_for_file_access(),
				errmsg("error while writing to WALDIFF segment in WALDIFFFlushBuffer() : %m")));
		snprintf(writer->errormsg_buf, MAX_ERRORMSG_LEN, 
				 "write() returns: %d, but expected: %ld",
				 written_bytes, writer->buffer_fullness);
		return WALDIFFWRITE_FAIL;
	}
	
	ereport(LOG, errmsg("Wrote %d bytes to WALDIFF segment", written_bytes));

	pg_fsync(writer->waldiff_seg.fd);

	return WALDIFFWRITE_SUCCESS;
}
