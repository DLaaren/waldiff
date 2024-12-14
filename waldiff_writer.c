#include "waldiff_writer.h"

static void 
WaldiffOpenSegment(WaldiffWriter *writer,
			   	   XLogSegNo nextSegNo,
			   	   TimeLineID *tli_p);

static void 
WaldiffCloseSegment(WaldiffWriter *writer);

// TODO write and add to CloseSegment
static void 
WaldiffFinishSegmentToSegmentSize(WaldiffWriter *writer);

static void
WaldiffWriteBufferToDisk(WaldiffWriter *writer);

void static
write_aligned(char *dest, uint32 *dest_offset, char *src, Size size);


WaldiffWriter *
WaldiffWriterAllocate(char *waldiff_dir,
					  int wal_segment_size) 
{
	WaldiffWriter *writer;

	writer = (WaldiffWriter *)
		palloc_extended(sizeof(WaldiffWriter),
						MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);

	if (!writer)
		return NULL;

	writer->writeBuf = (char *) palloc_extended(XLOG_BLCKSZ,
											    MCXT_ALLOC_NO_OOM);
	if (!writer->writeBuf)
	{
		pfree(writer);
		return NULL;
	}

	writer->seg.ws_file = -1;
	writer->WriteRecPtr = 0;
	writer->writeBufSize = 0;
	writer->sysid = 0;
	memcpy(writer->segcxt.ws_dir, waldiff_dir, strlen(waldiff_dir));
	writer->segcxt.ws_segsize = wal_segment_size;  

	writer->writeRestRecordBufSize = 0;
	return writer;
}

void 
WaldiffWriterFree(WaldiffWriter *writer)
{
	if (writer->seg.ws_file != -1)
		WaldiffCloseSegment(writer);

	pfree(writer->writeBuf);
	pfree(writer);
}

void 
WaldiffBeginWriting(WaldiffWriter *writer, uint64 sysid, XLogSegNo segNo, TimeLineID tli)
{
	WaldiffCloseSegment(writer);

	Assert(writer->writeBufSize == 0);

	XLogSegNoOffsetToRecPtr(segNo, 0, writer->segcxt.ws_segsize, writer->WriteRecPtr);
	writer->sysid = sysid;

	WaldiffOpenSegment(writer, segNo, &tli);
}

void static
write_aligned(char *dest, uint32 *dest_offset, char *src, Size size) 
{
	uint32 padding = 0;

	memcpy(dest + *dest_offset, src, size);
	*dest_offset += size;

	padding = MAXALIGN(*dest_offset) - *dest_offset;

	if (padding > 0) {
		memset(dest + *dest_offset, 0, padding);
		*dest_offset += padding;
	}		
}

/* Before the first writing WaldiffBegining() must be called
   Passed record must be freed one day */
void 
WaldiffWriterWrite(WaldiffWriter *writer,
				   XLogRecord *record)
{
	uint32 *record_len = &(record->xl_tot_len);
	uint32  free_page_space = 0;

	while (true) 
	{
		free_page_space = XLOG_BLCKSZ - MAXALIGN(writer->writeBufSize);
		if (free_page_space == 0) 
		{
			WaldiffWriteBufferToDisk(writer);
			continue;
		}

		if (writer->writeBufSize == 0)
		{
			XLogPageHeaderData page_hdr = {0};

			page_hdr.xlp_magic = XLOG_PAGE_MAGIC;
			page_hdr.xlp_info = 0;
			page_hdr.xlp_tli = writer->seg.ws_tli;
			page_hdr.xlp_pageaddr = writer->WriteRecPtr;
			page_hdr.xlp_rem_len = writer->writeRestRecordBufSize;

			// page_hdr.xlp_info |= XLP_BKP_REMOVABLE;

			if (page_hdr.xlp_rem_len > 0)
				page_hdr.xlp_info |= XLP_FIRST_IS_CONTRECORD;

			if (writer->WriteRecPtr == writer->segcxt.ws_segsize) 
			{
				XLogLongPageHeaderData long_page_hdr = {0};

				page_hdr.xlp_info |= XLP_LONG_HEADER;

				memcpy(&(long_page_hdr.std), &page_hdr, sizeof(XLogPageHeaderData));
				long_page_hdr.xlp_sysid = writer->sysid;
				long_page_hdr.xlp_seg_size = writer->segcxt.ws_segsize;	
				long_page_hdr.xlp_xlog_blcksz = XLOG_BLCKSZ;

				write_aligned(writer->writeBuf, &(writer->writeBufSize), (char *) &long_page_hdr, sizeof(XLogLongPageHeaderData));
			}
			else {
				write_aligned(writer->writeBuf, &(writer->writeBufSize), (char *) &page_hdr, sizeof(XLogPageHeaderData));
			}

		}

		if (writer->writeRestRecordBufSize > 0) 
		{
			uint32 rest_record_len = writer->writeRestRecordBufSize;

			free_page_space = XLOG_BLCKSZ - MAXALIGN(writer->writeBufSize);
			if (rest_record_len > free_page_space) 
			{
				int rest_rest_record_len = rest_record_len - free_page_space;

				write_aligned(writer->writeBuf, 
				 			  &(writer->writeBufSize),
					    	  writer->writeRestRecordBuf, 
					   		  free_page_space); 
				WaldiffWriteBufferToDisk(writer);

				memmove(writer->writeRestRecordBuf, 
					    writer->writeRestRecordBuf + free_page_space, 
					    rest_rest_record_len);
				writer->writeRestRecordBufSize = rest_rest_record_len;

				continue;
			}
			else 
			{
				write_aligned(writer->writeBuf, 
					   &(writer->writeBufSize),
					   writer->writeRestRecordBuf, 
					   rest_record_len); 
				writer->writeRestRecordBufSize = 0;
			}
		}

		if (*record_len == 0)
			return;

		if (*record_len > free_page_space)
		{
			long int rest_record_len = *record_len - free_page_space;

			Assert(writer->writeRestRecordBufSize == 0);

			write_aligned(writer->writeBuf, 
						  &(writer->writeBufSize),
				  		  (char *) record, 
				   		  free_page_space); 
			WaldiffWriteBufferToDisk(writer);

			writer->writeRestRecordBuf = palloc(rest_record_len);
			memcpy(writer->writeRestRecordBuf, 
				   (char *) record + free_page_space, 
				   rest_record_len);
			writer->writeRestRecordBufSize = rest_record_len;
			record_len = &(writer->writeRestRecordBufSize);

			continue;
		}
		else {
			write_aligned(writer->writeBuf, 
							&(writer->writeBufSize),
							(char *) record, 
							*record_len); 

			return;
		}
		
	}
}

static void 
WaldiffOpenSegment(WaldiffWriter *writer,
			   	   XLogSegNo nextSegNo,
			   	   TimeLineID *tli_p)
{
	TimeLineID tli = *tli_p;
    char fname[XLOG_FNAME_LEN];
	char fpath[MAXPGPATH];

    XLogFileName(fname, tli, nextSegNo, writer->segcxt.ws_segsize);

	if (snprintf(fpath, MAXPGPATH, "%s/%s", writer->segcxt.ws_dir, fname) == -1)
		ereport(ERROR,
				errmsg("WALDIFF: error during reading WAL absolute path : %s/%s", writer->segcxt.ws_dir, fname));

	writer->seg.ws_file = OpenTransientFile(fpath, PG_BINARY | O_WRONLY | O_CREAT | O_APPEND);
	if (writer->seg.ws_file == -1)
		ereport(ERROR, errmsg("WALDIFF: could not open WAL segment \"%s\": %m", fpath));

	writer->seg.ws_tli = tli;
	writer->seg.ws_segno = nextSegNo;
}

static void 
WaldiffCloseSegment(WaldiffWriter *writer)
{
// TODO finish with nulls?

	if (writer->seg.ws_file != -1)
		close(writer->seg.ws_file);
	writer->seg.ws_file = -1;
}

static void
WaldiffWriteBufferToDisk(WaldiffWriter *writer)
{
	int written_bytes;

	Assert(writer->seg.ws_file != -1);

	pgstat_report_wait_start(WAIT_EVENT_COPY_FILE_WRITE);
	written_bytes = pg_pwrite(writer->seg.ws_file, 
							  (void*) writer->writeBuf, 
							  XLOG_BLCKSZ, 
							  writer->WriteRecPtr % writer->segcxt.ws_segsize);
	pgstat_report_wait_end();

	if (written_bytes != XLOG_BLCKSZ)
		ereport(ERROR, errmsg("write %d of %d bytes to file \"%d\": %m", written_bytes, XLOG_BLCKSZ, writer->seg.ws_file));
	if (written_bytes == 0)
		ereport(ERROR, errmsg("file descriptor closed for write \"%d\": %m", writer->seg.ws_file));

	writer->WriteRecPtr += written_bytes;
	writer->writeBufSize = 0;
}
