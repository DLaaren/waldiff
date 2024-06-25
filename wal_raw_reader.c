#include "wal_raw_reader.h"


WALRawReaderState *
WALDIFFWriterAllocate(int wal_segment_size,
					  char *wal_dir,
					  WALRawReaderRoutine *routine,
					  Size buffer_capacity)
{
	WALRawReaderState *reader;
	reader = (WALRawReaderState*) palloc0(sizeof(WALRawReaderState));

	reader->routine = *routine;

	reader->buffer = (char*) palloc0(buffer_capacity);
	reader->buffer_fullness = 0;
	reader->already_read = 0;
	reader->buffer_capacity = buffer_capacity;

    reader->tmp_buffer = (char*) palloc0(TMP_BUFFER_CAPACITY);
	reader->tmp_buffer_fullness = 0;

	reader->wal_seg.fd = -1;
	reader->wal_seg.segno = 0;
	reader->wal_seg.tli= 0;

	reader->first_page_addr = 0;

	reader->wal_seg.current_offset = 0;
	reader->wal_seg.last_processed_record = InvalidXLogRecPtr;

	reader->wal_seg.segsize= wal_segment_size;

	if (wal_dir)
	{
		int dir_name_len = strlen(wal_dir);
		reader->wal_seg.dir = (char*) palloc0(sizeof(char) * (dir_name_len + 1));
		memcpy(reader->wal_seg.dir, wal_dir, dir_name_len);
		reader->wal_seg.dir[dir_name_len] = '\0';
	}
	else
		reader->wal_seg.dir = NULL;

	reader->errormsg_buf = palloc0(MAX_ERRORMSG_LEN + 1);

	return reader;						
}

void
WALReaderFree(WALRawReaderState *reader)
{
	if (reader->buffer_fullness > 0)
		ereport(LOG, errmsg("WALWriter still has some data in buffer. Remain data length : %ld", reader->buffer_fullness));
	
	if (reader->wal_seg.fd != -1)
		reader->routine.segment_close(&(reader->wal_seg));

	if (reader->wal_seg.dir != NULL)
		pfree(reader->wal_seg.dir);

	pfree(reader->errormsg_buf);
	pfree(reader->buffer);
    pfree(reader->tmp_buffer);
	pfree(reader);
}

void 
WALBeginRead(WALRawReaderState *reader,
				  XLogSegNo segNo, 
				  TimeLineID tli)
{
	reader->wal_seg.segno = segNo;
	reader->wal_seg.tli = tli;

	reader->routine.segment_open(&(reader->wal_seg));
}

/*
 * Read size bytes from WAL file and append it to buffer
 * Returns number of successfully read bytes from file 
 */
int 
append_to_buff(WALRawReaderState* raw_reader, uint64 size)
{
	int nbytes;

	if (size > WALRawReaderGetRestBufferCapacity(raw_reader))
	{
		ereport(ERROR, 
				errmsg("cannot read %ld bytes from file to buffer with rest capacity %ld", 
						size, WALRawReaderGetRestBufferCapacity(raw_reader)));
		return -1;
	}

	pgstat_report_wait_start(WAIT_EVENT_COPY_FILE_READ);
	nbytes = read(raw_reader->wal_seg.fd, (void*) (raw_reader->buffer + raw_reader->buffer_fullness), size);
	pgstat_report_wait_end();
	
	if (nbytes < 0)
		ereport(ERROR,
			(errcode_for_file_access(),
			errmsg("could not read WAL file : %m")));
	if (nbytes == 0)
		ereport(WARNING,
			(errcode_for_file_access(),
			errmsg("file descriptor closed for read \"%d\": %m", raw_reader->wal_seg.fd)));
	
	if (nbytes > 0)
	{
		raw_reader->already_read += nbytes;
		raw_reader->buffer_fullness += nbytes;
	}
	
	return nbytes;
}

/*
 * Clear buffer and set fullness = 0
 */
void 
reset_buff(WALRawReaderState* raw_reader)
{
	memset(raw_reader->buffer, 0, raw_reader->buffer_capacity);
	raw_reader->buffer_fullness = 0;
}

/*
 * Read size bytes from WAL file and append it to tmp buffer
 * Returns number of successfully read bytes from file 
 */
int 
append_to_tmp_buff(WALRawReaderState* raw_reader, uint64 size)
{
	int nbytes;

	if (size > WALRawReaderGetRestTmpBufferCapacity(raw_reader))
	{
		ereport(ERROR, 
				errmsg("cannot read %ld bytes from file to tmp buffer with rest capacity %ld", 
						size, WALRawReaderGetRestTmpBufferCapacity(raw_reader)));
		return -1;
	}

	pgstat_report_wait_start(WAIT_EVENT_COPY_FILE_READ);
	nbytes = read(raw_reader->wal_seg.fd, (void*) (raw_reader->tmp_buffer + raw_reader->tmp_buffer_fullness), size);
	pgstat_report_wait_end();
	
	if (nbytes < 0)
		ereport(ERROR,
			(errcode_for_file_access(),
			errmsg("could not read WAL file : %m")));
	if (nbytes == 0)
		ereport(WARNING,
			(errcode_for_file_access(),
			errmsg("file descriptor closed for read \"%d\": %m", raw_reader->wal_seg.fd)));

	if (nbytes > 0)
	{
		raw_reader->already_read += nbytes;
		raw_reader->tmp_buffer_fullness += nbytes;
	}
	
	return nbytes;
}

/*
 * Clear tmp buffer and set fullness = 0
 */
void 
reset_tmp_buff(WALRawReaderState* raw_reader)
{
	memset(raw_reader->tmp_buffer, 0, TMP_BUFFER_CAPACITY);
	raw_reader->tmp_buffer_fullness = 0;
}
