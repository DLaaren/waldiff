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

    reader->tmp_buffer = (char*) palloc0(BLCKSZ * 2);

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
