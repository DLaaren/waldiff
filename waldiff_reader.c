#include "waldiff_reader.h"

static void 
WaldiffOpenSegment(WaldiffReader *reader,
			       XLogSegNo nextSegNo,
			       TimeLineID *tli_p);

static void 
WaldiffCloseSegment(WaldiffReader *reader);

static void
WaldiffReadToBuffer(WaldiffReader *reader);

static void
ValidXLogRecord(WaldiffReader *reader, XLogRecord *record);


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

	reader->seg.ws_file = -1;
	reader->ReadRecPtr = 0;
	reader->readBufSize = XLOG_BLCKSZ;
	memcpy(reader->segcxt.ws_dir, wal_dir, strlen(wal_dir));
	reader->segcxt.ws_segsize = wal_segment_size;  

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
WaldiffBeginReading(WaldiffReader *reader, uint64 sysid, XLogSegNo segNo, TimeLineID tli)
{
	WaldiffCloseSegment(reader);

	Assert(reader->readBufSize == XLOG_BLCKSZ);

	XLogSegNoOffsetToRecPtr(segNo, 0, reader->segcxt.ws_segsize, reader->ReadRecPtr);
	reader->sysid = sysid;

	WaldiffOpenSegment(reader, segNo, &tli);
	WaldiffReadToBuffer(reader);
}

static void
ValidXLogRecord(WaldiffReader *reader, XLogRecord *record)
{
	pg_crc32c	crc;

	Assert(record->xl_tot_len >= SizeOfXLogRecord);

	/* Calculate the CRC */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, ((char *) record) + SizeOfXLogRecord, record->xl_tot_len - SizeOfXLogRecord);
	/* include the record header last */
	COMP_CRC32C(crc, (char *) record, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(crc);

	if (!EQ_CRC32C(record->xl_crc, crc))
		ereport(ERROR, errmsg("incorrect resource manager data checksum in record"));
}

/* Before the first reading WaldiffBeginReading() must be called
   Returned record must be freed one day */
XLogRecord * 
WaldiffReaderRead(WaldiffReader *reader)
{
    XLogRecord *record 			= NULL;
	XLogRecord  record_hdr;
	uint32 		curr_record_len = 0;
	uint32 		record_len		= 0;
	uint32 		rest_record_len = 0;
	long int 	non_read_space  = 0;

	while (true)
	{
		non_read_space = XLOG_BLCKSZ - MAXALIGN(reader->readBufSize);
		if (non_read_space == 0) 
		{
			WaldiffReadToBuffer(reader);
			continue;
		}

		if (reader->readBufSize == 0)
		{
			XLogPageHeader page_hdr = palloc(sizeof(XLogPageHeaderData));

			memcpy(page_hdr, reader->readBuf, sizeof(XLogPageHeaderData));
			
			if (page_hdr->xlp_info & XLP_LONG_HEADER)
			{
				XLogLongPageHeader long_page_hdr = palloc(sizeof(XLogLongPageHeaderData));

				memcpy(long_page_hdr, reader->readBuf, sizeof(XLogLongPageHeaderData));
				reader->readBufSize += SizeOfXLogLongPHD;

				Assert(long_page_hdr->xlp_seg_size == reader->segcxt.ws_segsize);
				Assert(long_page_hdr->xlp_sysid == reader->sysid);

				page_hdr = &(long_page_hdr->std);
			}
			else
			{
				reader->readBufSize += SizeOfXLogShortPHD;
			}

			Assert(page_hdr->xlp_magic == XLOG_PAGE_MAGIC);
			Assert(reader->seg.ws_tli == page_hdr->xlp_tli);
			Assert(reader->ReadRecPtr - XLOG_BLCKSZ == page_hdr->xlp_pageaddr);

			if (page_hdr->xlp_info & XLP_FIRST_IS_CONTRECORD)
				rest_record_len = page_hdr->xlp_rem_len;

			Assert((page_hdr->xlp_info & XLP_FIRST_IS_CONTRECORD && rest_record_len > 0) ||
				   (!(page_hdr->xlp_info & XLP_FIRST_IS_CONTRECORD) && rest_record_len == 0));
		}

		if (rest_record_len > 0)
		{
			if (reader->readRestRecordBufSize == 0)
			{
				reader->readBufSize += rest_record_len;
				continue;
			}

			non_read_space = XLOG_BLCKSZ - MAXALIGN(reader->readBufSize);
			if (rest_record_len > non_read_space)
			{
				reader->readRestRecordBuf = 
					repalloc_array(reader->readRestRecordBuf, 
								   char, 
								   reader->readRestRecordBufSize + non_read_space);

				memcpy(reader->readRestRecordBuf + reader->readRestRecordBufSize,
					   reader->readBuf + reader->readBufSize,
					   non_read_space);
				reader->readRestRecordBufSize += non_read_space;
				reader->readBufSize +=	non_read_space;

				rest_record_len -= non_read_space;		

				WaldiffReadToBuffer(reader);

				continue;
			}
			else 
			{
				Assert(record != NULL);

				memcpy((char *) record + curr_record_len,  
					   reader->readRestRecordBuf, 
					   reader->readRestRecordBufSize);
				curr_record_len += reader->readRestRecordBufSize;

				memcpy((char *) record + curr_record_len,  reader->readBuf + reader->readBufSize, rest_record_len);
				curr_record_len += rest_record_len;
				
				reader->readRestRecordBufSize = 0;
				reader->readBufSize += MAXALIGN(rest_record_len);

				ValidXLogRecord(reader, record);
				return record;
			}
		}

		if (record == NULL)
		{
			memcpy(&record_hdr, reader->readBuf + reader->readBufSize, SizeOfXLogRecord);

			record_len = record_hdr.xl_tot_len;
			if (record_len == 0)
				return NULL;
			record = palloc0(record_len);
		}

		if (record_len > non_read_space)
		{
			reader->readRestRecordBuf = palloc(non_read_space);
			memcpy(reader->readRestRecordBuf, reader->readBuf + reader->readBufSize, non_read_space);
			reader->readBufSize += non_read_space;
			reader->readRestRecordBufSize = non_read_space; 
			record_len = record_len - non_read_space;

			WaldiffReadToBuffer(reader);

			continue;
		}
		else
		{
			memcpy((char *) record + curr_record_len,  reader->readBuf + reader->readBufSize, record_len);
			curr_record_len += record_len;
			reader->readBufSize += MAXALIGN(record_len);
			
			ValidXLogRecord(reader, record);
			return record;
		}
	}

	return record;
}

static void 
WaldiffOpenSegment(WaldiffReader *reader,
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
		ereport(ERROR, errmsg("WALDIFF: could not open WAL segment \"%s\": %m", fpath));

	ereport(LOG, errmsg("READER: openning file"));
	
	reader->seg.ws_tli = tli;
	reader->seg.ws_segno = nextSegNo;
}

static void 
WaldiffCloseSegment(WaldiffReader *reader)
{
	ereport(LOG, errmsg("READER: closing file"));
	if (reader->seg.ws_file != -1)
		close(reader->seg.ws_file);
	reader->seg.ws_file = -1;
}

static void
WaldiffReadToBuffer(WaldiffReader *reader)
{
	int read_bytes;

	Assert(reader->seg.ws_file != -1);

	pgstat_report_wait_start(WAIT_EVENT_COPY_FILE_READ);
	read_bytes = pg_pread(reader->seg.ws_file, 
						  (void*) reader->readBuf, 
						  XLOG_BLCKSZ, 
						  reader->ReadRecPtr % reader->segcxt.ws_segsize);
	pgstat_report_wait_end();

	if (read_bytes != XLOG_BLCKSZ)
		ereport(ERROR, errmsg("read %d of %d bytes from file \"%d\": %m", read_bytes, XLOG_BLCKSZ, reader->seg.ws_file));
	else if (read_bytes == 0)
		ereport(ERROR, errmsg("file descriptor closed for read \"%d\": %m", reader->seg.ws_file));

	reader->ReadRecPtr += read_bytes;
	reader->readBufSize = 0;
}
