#include "waldiff_writer.h"


WALDIFFWriterState *
WALDIFFWriterAllocate(int wal_segment_size,
					  char *waldiff_dir,
					  WALDIFFWriterRoutine *routine,
					  Size buffer_capacity)
{
	WALDIFFWriterState *writer;
	writer = (WALDIFFWriterState*) palloc0(sizeof(WALDIFFWriterState));

	writer->routine = *routine;

	writer->already_written = 0;

	writer->waldiff_seg.fd = -1;
	writer->waldiff_seg.segno = 0;
	writer->waldiff_seg.tli= 0;

	writer->first_page_addr = 0;

	writer->waldiff_seg.current_offset = 0;
	writer->waldiff_seg.last_processed_record = InvalidXLogRecPtr;

	writer->waldiff_seg.segsize= wal_segment_size;

	if (waldiff_dir)
	{
		int dir_name_len = strlen(waldiff_dir);
		writer->waldiff_seg.dir = (char*) palloc0(sizeof(char) * (dir_name_len + 1));
		memcpy(writer->waldiff_seg.dir, waldiff_dir, dir_name_len);
		writer->waldiff_seg.dir[dir_name_len] = '\0';
	}
	else
		writer->waldiff_seg.dir = NULL;

	writer->errormsg_buf = palloc0(MAX_ERRORMSG_LEN + 1);

	return writer;						
}

void 
WALDIFFWriterFree(WALDIFFWriterState *writer)
{
	if (writer->waldiff_seg.fd != -1)
		writer->routine.segment_close(&(writer->waldiff_seg));

	if (writer->waldiff_seg.dir != NULL)
		pfree(writer->waldiff_seg.dir);

	pfree(writer->errormsg_buf);
	pfree(writer);
}

void 
WALDIFFBeginWrite(WALDIFFWriterState *writer,
				  XLogSegNo segNo, 
				  TimeLineID tli)
{
	writer->waldiff_seg.segno = segNo;
	writer->waldiff_seg.tli = tli;

	writer->routine.segment_open(&(writer->waldiff_seg), PG_BINARY | O_RDWR | O_CREAT);
}

/*
 * Writes data_size bytes of given data to WALDIFF segment
 */
int 
write_data_to_file(WALDIFFWriterState* writer, char* data, uint64 data_size)
{
	int nbytes;

	pgstat_report_wait_start(WAIT_EVENT_COPY_FILE_WRITE);
	nbytes = write(writer->waldiff_seg.fd, (void*) data, data_size);
	pgstat_report_wait_end();

	if (nbytes != data_size)
		ereport(ERROR,
			(errcode_for_file_access(),
			errmsg("could not write to file \"%d\": %m", writer->waldiff_seg.fd)));
	if (nbytes == 0)
		ereport(WARNING,
			(errcode_for_file_access(),
			errmsg("file descriptor closed for write \"%d\": %m", writer->waldiff_seg.fd)));

	if (nbytes > 0)
		writer->already_written += nbytes;

	return nbytes;
}