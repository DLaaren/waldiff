#include "wal_raw_reader.h"


WALRawReaderState *
WALRawReaderAllocate(int wal_segment_size,
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

	reader->wal_seg.segsize = wal_segment_size;

	if (wal_dir)
	{
		int dir_name_len = strlen(wal_dir);
		reader->wal_seg.dir = (char*) palloc0(sizeof(char) * (dir_name_len + 1));
		memcpy(reader->wal_seg.dir, wal_dir, dir_name_len);
		reader->wal_seg.dir[dir_name_len] = '\0';
	}
	else
		reader->wal_seg.dir = NULL;

	reader->needless_lsn_list = NeedlessLsnListAllocate(2e16 * sizeof(XLogRecPtr));

	reader->errormsg_buf = palloc0(MAX_ERRORMSG_LEN + 1);

	return reader;						
}

void
WALRawReaderFree(WALRawReaderState *reader)
{
	if (reader->buffer_fullness > 0)
		ereport(WARNING, errmsg("WALRawReader still has some data in buffer. Remain data length : %ld", reader->buffer_fullness));

	if (reader->wal_seg.fd != -1)
		reader->routine.segment_close(&(reader->wal_seg));

	if (reader->wal_seg.dir != NULL)
		pfree(reader->wal_seg.dir);

	if (reader->needless_lsn_list != NULL)
		NeedlessLsnListFree(reader->needless_lsn_list);

	pfree(reader->errormsg_buf);
	pfree(reader->buffer);
    pfree(reader->tmp_buffer);
	pfree(reader);
}

/*
 * Update readers's internal WAL segment information and open file with flags
 */
void 
WALRawBeginRead(WALRawReaderState *reader,
				  XLogSegNo segNo, 
				  TimeLineID tli,
				  int flags)
{
	reader->wal_seg.segno = segNo;
	reader->wal_seg.tli = tli;

	reader->routine.segment_open(&(reader->wal_seg), flags);
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
				errmsg("append_to_buf():\ncannot read %lu bytes from WAL file to buffer with rest capacity %lu\nbuffer_capacity: %lu\nbuffer_fullness: %lu", 
						size, WALRawReaderGetRestBufferCapacity(raw_reader), raw_reader->buffer_capacity, raw_reader->buffer_fullness));
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
		ereport(DEBUG1,
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
				errmsg("append_to_tmp_buff():\ncannot read %lu bytes from file to tmp buffer with rest capacity %lu", 
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
		ereport(DEBUG1,
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
	raw_reader->tmp_buffer_fullness = 0;
}

/*
 * Skip next record in WAL segment. You must reset raw_reader's buffers before 
 * calling this function. raw_reader's buffers will not contain skipped record
 */
WALRawRecordSkipResult
WALSkipRawRecord(WALRawReaderState *raw_reader, XLogRecord *target)
{
	int	   nbytes = 0;

	while (true)
	{
		/* check whether we are in start of block */
		if ((raw_reader->already_read) % BLCKSZ == 0)
		{
			XLogPageHeader hdr;
			reset_tmp_buff(raw_reader);
			nbytes = append_to_tmp_buff(raw_reader, SizeOfXLogShortPHD); /* skip short header */
			if (nbytes == 0)
				return WALSKIP_EOF;
			
			/*
			 * Check whether we are in start of segment and skip rest of header if needed
			 */
			hdr = (XLogPageHeader) raw_reader->tmp_buffer;
			if (XLogPageHeaderSize(hdr) > SizeOfXLogShortPHD)
			{
				lseek(raw_reader->wal_seg.fd, SizeOfXLogLongPHD - SizeOfXLogShortPHD, SEEK_CUR);
				raw_reader->already_read += SizeOfXLogLongPHD - SizeOfXLogShortPHD;
			}
			
			if (hdr->xlp_rem_len > 0)
			{
				/*
				 * Even rest of record may be so large that it  will not fit on the page. 
				 * data_len - part of record that is in the current page
				 */
				uint64 data_len = 0;

				if (! XlogRecFitsOnPage(raw_reader->already_read, hdr->xlp_rem_len))
					data_len = BLCKSZ * (1 + (raw_reader->already_read / BLCKSZ)) - raw_reader->already_read; /* rest of current page */
				else
					data_len = hdr->xlp_rem_len;
				
				lseek(raw_reader->wal_seg.fd, data_len, SEEK_CUR);
				raw_reader->already_read += data_len;

				if (data_len == hdr->xlp_rem_len) /* if record remains fit on the current page */
				{
					lseek(raw_reader->wal_seg.fd, MAXALIGN(hdr->xlp_rem_len) - data_len, SEEK_CUR); /* Records and maxaligned, so skip padding */
					raw_reader->already_read += MAXALIGN(hdr->xlp_rem_len) - data_len;

					return WALSKIP_SUCCESS; /* full record skipped, so return success */
				}
				else /* remaining part of record will be skipped in next iterations */
					continue;
			}
		}

		/*
		 * We are not in the start of the page, so try read record header. 
		 * We take into account that header may not fit on the rest of the current page
		 */

		if (! XlogRecHdrFitsOnPage(raw_reader->already_read))
		{
			uint64 data_len = BLCKSZ * (1 + (raw_reader->already_read) / BLCKSZ) - raw_reader->already_read; /* rest of current page */

			/*
			 * Skip as much as we can. Remaining part of record will be read in next iterations
			 */
			lseek(raw_reader->wal_seg.fd, data_len, SEEK_CUR);
			raw_reader->already_read += data_len;
			
			continue;
		}
		else 
		{
			uint64 		data_len; /* data_len - part of record that is in the current page. It may be full record length */
			XLogRecord* record;

			reset_tmp_buff(raw_reader); /* we want tmp buffer to contain only record header */
			nbytes = append_to_tmp_buff(raw_reader, SizeOfXLogRecord);
			if (nbytes == 0)
				return WALSKIP_EOF;
			
			record = (XLogRecord*) raw_reader->tmp_buffer;
			raw_reader->wal_seg.last_processed_record = raw_reader->already_read;

			/*
			 * Record may not fit on the rest of the current page
			 */
			if (! XlogRecFitsOnPage(raw_reader->already_read, record->xl_tot_len - SizeOfXLogRecord))
				data_len = BLCKSZ * (1 + (raw_reader->already_read) / BLCKSZ) - raw_reader->already_read; /* rest of current page */
			else
				data_len = record->xl_tot_len - SizeOfXLogRecord;

			lseek(raw_reader->wal_seg.fd, data_len, SEEK_CUR);
			raw_reader->already_read += data_len;
			
			if (data_len == (record->xl_tot_len - SizeOfXLogRecord)) /* If record fit on the current page */
			{
				lseek(raw_reader->wal_seg.fd, MAXALIGN(record->xl_tot_len) - SizeOfXLogRecord - data_len, SEEK_CUR); /* Records and maxaligned, so skip padding */
				raw_reader->already_read += MAXALIGN(record->xl_tot_len) - SizeOfXLogRecord - data_len;

				return WALSKIP_SUCCESS; /* full record skipped, so return success */
			}
			else /* remaining part of record will be skipped in next iterations */
				continue;
		}
	}
	return WALSKIP_FAIL;
}

/*
 * Read next record in WAL segment. You must reset raw_reader's buffers before 
 * calling this function
 * After execution, you can get pointer to raw record via WALRawReaderGetLastRecordRead
 */
WALRawRecordReadResult 
WALReadRawRecord(WALRawReaderState *raw_reader, XLogRecord *target)
{
	int	   nbytes = 0;

	while (true)
	{
		/* check whether we are in start of block */
		if ((raw_reader->already_read) % BLCKSZ == 0)
		{
			XLogPageHeader hdr;
			reset_tmp_buff(raw_reader); /* we want tmp buff to only contain page header */
			nbytes = append_to_tmp_buff(raw_reader, SizeOfXLogShortPHD); /* skip short header */
			if (nbytes == 0)
				return WALREAD_EOF;
			
			hdr = (XLogPageHeader) raw_reader->tmp_buffer;
			if (XLogPageHeaderSize(hdr) > SizeOfXLogShortPHD) /* check whether we are in start of segment */
			{
				XLogLongPageHeader long_hdr;
				nbytes = append_to_tmp_buff(raw_reader, SizeOfXLogLongPHD - SizeOfXLogShortPHD); /* skip rest of long header */
				if (nbytes == 0)
					return WALREAD_EOF;

				long_hdr  = (XLogLongPageHeader) raw_reader->tmp_buffer;

				/*
				 * If we are testing this function, raw_reader
				 * still does not contain this values
				 */
				if (raw_reader->first_page_addr == 0)
				{
					raw_reader->system_identifier = long_hdr->xlp_sysid;
					raw_reader->first_page_addr   = hdr->xlp_pageaddr;
				}
				
				ereport(LOG, errmsg("READ LONG HDR. ADDR : %ld", long_hdr->std.xlp_pageaddr));
			}

			if (hdr->xlp_rem_len > 0)
			{
				/*
				 * Even rest of record may be so large that it  will not fit on the page. 
				 * data_len - part of record that is in the current page
				 */
				uint64 data_len = 0;

				if (! XlogRecFitsOnPage(raw_reader->already_read, hdr->xlp_rem_len))
					data_len = BLCKSZ * (1 + (raw_reader->already_read / BLCKSZ)) - raw_reader->already_read; /* rest of current page */
				else
					data_len = hdr->xlp_rem_len;
				
				nbytes = append_to_buff(raw_reader, data_len);
				if (nbytes == 0 && data_len > 0)
					return WALREAD_EOF;

				if (data_len == hdr->xlp_rem_len) /* if record remains fit on the current page */
				{
					lseek(raw_reader->wal_seg.fd, MAXALIGN(hdr->xlp_rem_len) - data_len, SEEK_CUR); /* Records and maxaligned, so skip padding */
					raw_reader->already_read += MAXALIGN(hdr->xlp_rem_len) - data_len;
					return WALREAD_SUCCESS; /* full record is in out buffer, so return success */
				}
				else /* remaining part of record will be read in next iterations */
					continue;
			}
		}

		/*
		 * We are not in the start of the page, so try read record header. 
		 * We take into account that header may not fit on the rest of the current page
		 */

		if (! XlogRecHdrFitsOnPage(raw_reader->already_read))
		{
			uint64 data_len = BLCKSZ * (1 + (raw_reader->already_read) / BLCKSZ) - raw_reader->already_read; /* rest of current page */

			/*
			 * Read as much as we can. Remaining part of record will be read in next iterations
			 */
			nbytes = append_to_buff(raw_reader, data_len);
			if (nbytes == 0 && data_len > 0)
				return WALREAD_EOF;
			
			continue;
		}
		else 
		{
			uint64 		data_len; /* data_len - part of record that is in the current page. It may be full record length */
			XLogRecord* record;

			reset_tmp_buff(raw_reader); /* we want tmp buffer to contain only record header */
			nbytes = append_to_tmp_buff(raw_reader, SizeOfXLogRecord);
			if (nbytes == 0)
				return WALREAD_EOF;
			
			record = (XLogRecord*) raw_reader->tmp_buffer;
			raw_reader->wal_seg.last_processed_record = raw_reader->already_read;

			Assert(record->xl_tot_len >= 0);

			if (record->xl_tot_len == 0)
			{
				ereport(DEBUG1, errmsg("WALReadRawRecord(): read record with xl_tot_len = 0"));
				return WALREAD_EOF;
			}

			/*
			 * avoid OOM
			 */
			if (SizeOfXLogRecord > WALRawReaderGetRestBufferCapacity(raw_reader))
			{
				ereport(WARNING, 
						errmsg("WALReadRawRecord():\ncannot write xlog header to buffer with rest capacity %ld", 
								WALRawReaderGetRestBufferCapacity(raw_reader)));
				return WALREAD_FAIL;
			}

			/*
			 * If we did everything right before, buffer is empty and we write header into it
			 */
			memcpy((void*) (raw_reader->buffer + raw_reader->buffer_fullness), raw_reader->tmp_buffer, SizeOfXLogRecord);
			raw_reader->buffer_fullness += SizeOfXLogRecord;

			/*
			 * Record may not fit on the rest of the current page
			 */
			if (! XlogRecFitsOnPage(raw_reader->already_read, record->xl_tot_len - SizeOfXLogRecord))
			{
				data_len = BLCKSZ * (1 + (raw_reader->already_read) / BLCKSZ) - raw_reader->already_read; /* rest of current page */
			}
			else
			{
				data_len = record->xl_tot_len - SizeOfXLogRecord;
			}

			nbytes = append_to_buff(raw_reader, data_len);
			if (nbytes == 0 && data_len > 0)
				return WALREAD_EOF;
			
			if (data_len == (record->xl_tot_len - SizeOfXLogRecord)) /* If record fits on the current page */
			{
				lseek(raw_reader->wal_seg.fd, MAXALIGN(record->xl_tot_len) - SizeOfXLogRecord - data_len, SEEK_CUR); /* Records and maxaligned, so skip padding */
				raw_reader->already_read += MAXALIGN(record->xl_tot_len) - SizeOfXLogRecord - data_len;

				return WALREAD_SUCCESS; /* full record is in out buffer, so return success */
			}
			else /* remaining part of record will be read in next iterations */
				continue;
		}
	}
	return WALREAD_FAIL;
}

NeedlessLsnList* 
NeedlessLsnListAllocate(size_t list_capacity)
{
	NeedlessLsnList* needless_lsn_list = (NeedlessLsnList*) palloc0(sizeof(NeedlessLsnList));

	needless_lsn_list->capacity = list_capacity;
	needless_lsn_list->fullness = 0;

	needless_lsn_list->list = (char*) palloc0(sizeof(char) * list_capacity);
	needless_lsn_list->ptr = 0;

	return needless_lsn_list;
}

/*
 * Add new element to end of list
 */
void 
NeedlessLsnListPush(NeedlessLsnList* needless_lsn_list, XLogRecPtr new_elem)
{
	if (NeedlessLsnListGetRestListCapacity(needless_lsn_list) < sizeof(new_elem))
		NeedlessLsnListIncrease(needless_lsn_list);

	memcpy(needless_lsn_list->list + needless_lsn_list->fullness, new_elem, sizeof(XLogRecPtr));

	needless_lsn_list->fullness += sizeof(XLogRecPtr);
}

/*
 * In case there is not enough memory in list to push new element, 
 * list will be increased by 1.5 times
 */
void 
NeedlessLsnListIncrease(NeedlessLsnList* needless_lsn_list)
{
	needless_lsn_list->capacity *= 1.5;
	needless_lsn_list->list = (char*) repalloc(needless_lsn_list->list, needless_lsn_list->capacity);
}

bool 
NeedlessLsnListFind(NeedlessLsnList* needless_lsn_list, XLogRecPtr elem)
{
	XLogRecPtr* target = (XLogRecPtr*) (needless_lsn_list->list + needless_lsn_list->ptr * sizeof(XLogRecPtr));
	if (*target == elem)
	{
		needless_lsn_list->ptr += 1;
		return true;
	}
	else
		return false;
}

void 
NeedlessLsnListFree(NeedlessLsnList* needless_lsn_list)
{
	pfree(needless_lsn_list->list);
	pfree(needless_lsn_list);
}
