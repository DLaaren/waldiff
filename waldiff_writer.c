#include "waldiff_writer.h"
#include "wal_raw_reader.h"

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

/*
 * Update writer's internal WALDIFF segment information and open file with flags
 */
void 
WALDIFFBeginWrite(WALDIFFWriterState *writer,
				  XLogSegNo segNo, 
				  TimeLineID tli,
				  int flags)
{
	writer->waldiff_seg.segno = segNo;
	writer->waldiff_seg.tli = tli;

	writer->routine.segment_open(&(writer->waldiff_seg), flags);
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

/* 
 * Writes given record to WAL segment. Prameter record must contain only
 * record's data, without page headers
 */
WALDIFFRecordWriteResult 
WALDIFFWriteRecord(WALDIFFWriterState *writer, char *record)
{
	Size rem_data_len = 0;
	pg_crc32c	crc;
	int	nbytes = 0;
	char* record_copy = record; /* we need second pointer for navigation through record */
	XLogRecord* record_hdr = (XLogRecord*) record;

	/* We use it when we need to put padding into file */
	char 		null_buff[1024];
	memset(null_buff, 0, 1024);

	/*
	 * If we are testing this function, writer
	 * still does not contain this values
	 */
	if (writer->first_page_addr == 0)
	{
		XLogLongPageHeader long_hdr = (XLogLongPageHeader) record;
		writer->system_identifier = long_hdr->xlp_sysid;
		writer->first_page_addr = long_hdr->std.xlp_pageaddr;
	}

	while (true)
	{
		/* check whether we are in start of block */
		if (writer->already_written % BLCKSZ == 0)
		{
			/* check whether we are in start of segment */
			if (writer->already_written == 0)
			{
				/*
				 * We must create long page header and write it to WALDIFF segment
				 */
				XLogLongPageHeaderData long_hdr = {0};

				long_hdr.xlp_sysid 		  = writer->system_identifier;
				long_hdr.xlp_seg_size	  = writer->waldiff_seg.segsize;
				long_hdr.xlp_xlog_blcksz  = XLOG_BLCKSZ;

				long_hdr.std.xlp_info 	  = 0;
				long_hdr.std.xlp_info 	  |= XLP_LONG_HEADER;
				long_hdr.std.xlp_tli 	  = writer->waldiff_seg.tli;
				long_hdr.std.xlp_rem_len  = 0;
				long_hdr.std.xlp_magic 	  = XLOG_PAGE_MAGIC;
				long_hdr.std.xlp_pageaddr = writer->first_page_addr;

				ereport(LOG, errmsg("WRITE LONG HDR. ADDR : %ld", long_hdr.std.xlp_pageaddr));
				
				nbytes = write_data_to_file(writer, (char*)&long_hdr, SizeOfXLogLongPHD);
				if (nbytes == 0)
					return WALDIFFWRITE_EOF;
			}
			else
			{
				/*
				 * We must create short page header and write it to WALDIFF segment
				 */
				XLogPageHeaderData hdr;

				hdr.xlp_info 	 = 0;
				hdr.xlp_info 	 |= XLP_BKP_REMOVABLE;

				if (rem_data_len > 0)
					hdr.xlp_info |= XLP_FIRST_IS_CONTRECORD;
				
				hdr.xlp_rem_len  = rem_data_len;
				
				hdr.xlp_tli 	 = writer->waldiff_seg.tli;
				hdr.xlp_magic 	 = XLOG_PAGE_MAGIC;
				hdr.xlp_pageaddr = writer->first_page_addr + writer->already_written;

				nbytes = write_data_to_file(writer, (char*)&hdr, SizeOfXLogShortPHD);
				if (nbytes == 0)
					return WALDIFFWRITE_EOF;
			}
		}

		if (rem_data_len > 0)
		{
			/*
			 * Record may not fit on the rest of the current page
			 */
			if (! XlogRecFitsOnPage(writer->already_written, rem_data_len))
			{
				uint64 data_len = BLCKSZ * (1 + (writer->already_written / BLCKSZ)) - writer->already_written; /* rest of current page */
				nbytes = write_data_to_file(writer, record_copy, data_len);
				rem_data_len = rem_data_len - data_len;
				record_copy += data_len; /* we don't need this part of record anymore */

				continue; /* remaining part of record will be written in next iterations */
			}
			else
			{
				nbytes = write_data_to_file(writer, record_copy, rem_data_len);

				/*
				 * Records are maxaligned, so we must write all padding too
				 */
				if (MAXALIGN(record_hdr->xl_tot_len) - record_hdr->xl_tot_len)
				{
					nbytes = write_data_to_file(writer, null_buff, MAXALIGN(record_hdr->xl_tot_len) - record_hdr->xl_tot_len);
					if (nbytes == 0)
						return WALDIFFWRITE_EOF;
				}

				return WALDIFFWRITE_SUCCESS;
			}
		}

		/*
		 * First record in first segment has no previous records
		 */
		if (writer->already_written == SizeOfXLogLongPHD && writer->waldiff_seg.segno == 1)
			record_hdr->xl_prev = 0;
		else
			record_hdr->xl_prev = writer->waldiff_seg.last_processed_record + writer->first_page_addr;


		writer->waldiff_seg.last_processed_record = writer->already_written;

		memset((void*) (record_hdr + 18), 0, 2); /* padding in struct must be set to zero */

		/*
		 * Creating checksum (we possibly change xl_prev, so checksum also must be changed)
		 */
		INIT_CRC32C(crc);
		COMP_CRC32C(crc, (char*) (record_hdr + SizeOfXLogRecord), record_hdr->xl_tot_len - SizeOfXLogRecord);
		COMP_CRC32C(crc, record_hdr, offsetof(XLogRecord, xl_crc));
		FIN_CRC32C(crc);
		record_hdr->xl_crc = crc;

		/*
		 * We take into account that header may not fit on the rest of the current page
		 */

		if (! XlogRecFitsOnPage(writer->already_written, record_hdr->xl_tot_len))
		{
			uint64 data_len = BLCKSZ * (1 + (writer->already_written / BLCKSZ)) - writer->already_written; /* rest of current page */
			rem_data_len = record_hdr->xl_tot_len - data_len;

			nbytes = write_data_to_file(writer, record_copy, data_len); /* write to file as much as we can */
			if (nbytes == 0)
				return WALDIFFWRITE_EOF;
			
			record_copy += data_len;
			continue; /* remaining part of record will be written in next iterations */
		}
		else
		{
			nbytes = write_data_to_file(writer, record_copy, record_hdr->xl_tot_len);
			if (nbytes == 0)
				return WALDIFFWRITE_EOF;

			/*
			 * Records are maxaligned, so we must write all padding too
			 */
			if (MAXALIGN(record_hdr->xl_tot_len) - record_hdr->xl_tot_len > 0)
			{
				nbytes = write_data_to_file(writer, null_buff, MAXALIGN(record_hdr->xl_tot_len) - record_hdr->xl_tot_len);
				if (nbytes == 0)
					return WALDIFFWRITE_EOF;
			}
			return WALDIFFWRITE_SUCCESS;
		}
	}

	return WALDIFFWRITE_FAIL;
}

/*
 * Opens new WAL segment file with specified flags. Also can be used
 * for opening WALDIFF segment
 */
void 
WALDIFFOpenSegment(WALSegment *seg, int flags)
{
	char fname[XLOG_FNAME_LEN];
	char fpath[MAXPGPATH];

	XLogFileName(fname, seg->tli, seg->segno, seg->segsize);

	if (snprintf(fpath, MAXPGPATH, "%s/%s", seg->dir, fname) == -1)
		ereport(ERROR,
				errmsg("error during reading WALDIFF absolute path: %s/%s", seg->dir, fname));
	
	seg->fd = OpenTransientFile(fpath, flags);
	if (seg->fd == -1)
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("could not open WALDIFF segment \"%s\": %m", fpath)));
}

/*
 * Closes WAL segment and set file descripor = -1.
 * Also can be used for closing WALDIFF segment
 */
void 
WALDIFFCloseSegment(WALSegment *seg)
{
	Assert(seg->fd != -1);
	CloseTransientFile(seg->fd);
	seg->fd = -1;
}