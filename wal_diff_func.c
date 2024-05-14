#include "wal_diff_func.h"

void 
copy_file_part(uint64 size, uint64 src_offset, 
			   char* tmp_buffer, char* xlog_rec_buffer)
{
	int      	 srcfd;
	int64 		 read_left = size;
	bool		 read_only_header = false;

	/*
	* Open the file
	*/
	srcfd = OpenTransientFile(writer_state.src_path, O_RDONLY | PG_BINARY);
	if (srcfd < 0)
	ereport(ERROR,
		(errcode_for_file_access(),
			errmsg("could not open file \"%s\": %m", writer_state.src_path)));

	if (lseek(srcfd, src_offset, SEEK_SET) < 0)
	ereport(ERROR,
		(errcode_for_file_access(),
			errmsg("could not navigate through the file \"%s\": %m", writer_state.src_path)));

	/*
	* Do the data copying.
	*/
	while(true)
	{
		int read_len = read_one_xlog_rec(srcfd, writer_state.src_path, xlog_rec_buffer, tmp_buffer, read_left, &read_only_header);
		if (read_len == -1 || read_only_header)
			break;
		
		write_one_xlog_rec(writer_state.dest_fd, writer_state.dest_path, xlog_rec_buffer);
		read_left -= read_len;
		if (read_left <= 0)
			break;
	}

	if (CloseTransientFile(srcfd) != 0)
	ereport(ERROR,
		(errcode_for_file_access(),
			errmsg("could not close file \"%d\": %m", writer_state.dest_fd)));
}

int
read_one_xlog_rec(int src_fd, const char* src_file_name, 
				  char* xlog_rec_buffer, char* tmp_buff, 
				  uint64 read_left, bool* read_only_header)
{
	uint64 		current_file_pos = lseek(src_fd, 0, SEEK_CUR);
	XLogRecord* record;
	uint64 		read_in_total = 0;
	uint64 		xlog_rec_buff_offset = 0;
	bool 		is_first_page = true;

	int 		nbytes;

	if (current_file_pos < 0)
		ereport(ERROR,
			(errcode_for_file_access(),
			errmsg("could not navigate in file \"%s\": %m", src_file_name)));

	while (true)
	{
		if ((current_file_pos + read_in_total) % BLCKSZ == 0)
		{
			XLogPageHeader hdr;
			nbytes = read_file2buff(src_fd, tmp_buff, SizeOfXLogShortPHD, 0, src_file_name);
			if (nbytes == 0)
				break;
			
			read_in_total += nbytes;

			hdr = (XLogPageHeader) tmp_buff;
			if (XLogPageHeaderSize(hdr) > SizeOfXLogShortPHD)
			{
				XLogLongPageHeader long_hdr;
				nbytes = read_file2buff(src_fd, tmp_buff, SizeOfXLogLongPHD - SizeOfXLogShortPHD, read_in_total, src_file_name);
				if (nbytes == 0)
					break;

				long_hdr  = (XLogLongPageHeader) tmp_buff;
				writer_state.sys_id 	  = long_hdr->xlp_sysid;
				writer_state.page_addr = hdr->xlp_pageaddr;
				writer_state.tli 	  = hdr->xlp_tli;
				
				read_in_total += nbytes;
			}

			if (read_left == read_in_total)
			{
				*read_only_header = true;
				break;
			}

			if (hdr->xlp_rem_len > 0)
			{
				uint64 data_len = 0;

				if (is_first_page)
					ereport(ERROR,
						errmsg("previous record was was not fully read in addr : %X/%X", LSN_FORMAT_ARGS(hdr->xlp_pageaddr)));
				is_first_page = false;

				if (! XlogRecFitsOnPage(current_file_pos + read_in_total, hdr->xlp_rem_len))
					data_len = BLCKSZ * (1 + (current_file_pos + read_in_total) / BLCKSZ) - current_file_pos - read_in_total;
				else
					data_len = hdr->xlp_rem_len;
				
				nbytes = read_file2buff(src_fd, xlog_rec_buffer, data_len, xlog_rec_buff_offset, src_file_name);
				if (nbytes == 0)
					break;
				
				read_in_total += nbytes;
				xlog_rec_buff_offset += nbytes;

				if (data_len == hdr->xlp_rem_len)
				{
					lseek(src_fd, MAXALIGN(hdr->xlp_rem_len) - data_len, SEEK_CUR);
					read_in_total += MAXALIGN(hdr->xlp_rem_len) - data_len;
					return read_in_total;
				}
				
				else
					continue;
			}
		}

		if (! XlogRecHdrFitsOnPage(current_file_pos + read_in_total))
		{
			uint64 data_len = BLCKSZ * (1 + (current_file_pos + read_in_total) / BLCKSZ) - current_file_pos - read_in_total;
			nbytes = read_file2buff(src_fd, xlog_rec_buffer, data_len, xlog_rec_buff_offset, src_file_name);
			if (nbytes == 0)
				break;
			
			read_in_total += nbytes;
			xlog_rec_buff_offset += nbytes;

			is_first_page = false;
			continue;
		}
		else 
		{
			uint64 data_len;
			nbytes = read_file2buff(src_fd, tmp_buff, SizeOfXLogRecord, 0, src_file_name);
			if (nbytes == 0)
				break;
			
			read_in_total += nbytes;

			record = (XLogRecord*) tmp_buff;
			memcpy((char*) xlog_rec_buffer + xlog_rec_buff_offset, tmp_buff, SizeOfXLogRecord);
			xlog_rec_buff_offset += SizeOfXLogRecord;

			if (! XlogRecFitsOnPage(current_file_pos + read_in_total, record->xl_tot_len - SizeOfXLogRecord))
				data_len = BLCKSZ * (1 + (current_file_pos + read_in_total) / BLCKSZ) - current_file_pos - read_in_total;
			else
				data_len = record->xl_tot_len - SizeOfXLogRecord;
			
			nbytes = read_file2buff(src_fd, xlog_rec_buffer, data_len, xlog_rec_buff_offset, src_file_name);
			if (nbytes == 0)
				break;
			
			read_in_total += nbytes;
			xlog_rec_buff_offset += nbytes;

			if (data_len == (record->xl_tot_len - SizeOfXLogRecord))
			{
				lseek(src_fd, MAXALIGN(record->xl_tot_len) - SizeOfXLogRecord - data_len, SEEK_CUR);
				read_in_total += MAXALIGN(record->xl_tot_len) - SizeOfXLogRecord - data_len;
				return read_in_total;
			}
			
			is_first_page = false;
			continue;
		}
	}
	return -1;
}

void
write_one_xlog_rec(int dst_fd, const char* dst_file_name, char* xlog_rec_buffer)
{
	int 		nbytes;
	uint64 		rem_data_len = 0;
	XLogRecord* record;
	pg_crc32c	crc;
	uint64 		already_read = 0;

	/* Use it when we need to put padding into file */
	char 		null_buff[1024];
	memset(null_buff, 0, 1024);

	while (true) 
	{
		if (writer_state.dest_curr_offset % BLCKSZ == 0)
		{
			if (writer_state.dest_curr_offset == 0)
			{
				XLogLongPageHeaderData long_hdr;

				long_hdr.xlp_sysid 		  = writer_state.sys_id;
				long_hdr.xlp_seg_size	  = writer_state.wal_segment_size;
				long_hdr.xlp_xlog_blcksz  = XLOG_BLCKSZ;

				long_hdr.std.xlp_info 	  = 0;
				long_hdr.std.xlp_info 	  |= XLP_LONG_HEADER;
				long_hdr.std.xlp_tli 	  = writer_state.tli;
				long_hdr.std.xlp_rem_len  = 0;
				long_hdr.std.xlp_magic 	  = XLOG_PAGE_MAGIC;
				long_hdr.std.xlp_pageaddr = writer_state.page_addr;

				ereport(LOG, errmsg("LONG HDR. ADDR : %ld", long_hdr.std.xlp_pageaddr));

				nbytes = write_buff2file(dst_fd, (char*) &long_hdr, SizeOfXLogLongPHD, 0);
				writer_state.dest_curr_offset += nbytes;
			}
			else
			{
				XLogPageHeaderData hdr;

				hdr.xlp_info 	 = 0;
				hdr.xlp_info 	|= XLP_BKP_REMOVABLE;
				if (rem_data_len > 0)
					hdr.xlp_info |= XLP_FIRST_IS_CONTRECORD;
				hdr.xlp_rem_len = rem_data_len;
				
				hdr.xlp_tli 	 = writer_state.tli;
				hdr.xlp_magic 	 = XLOG_PAGE_MAGIC;
				hdr.xlp_pageaddr = writer_state.page_addr + writer_state.dest_curr_offset;

				nbytes = write_buff2file(dst_fd, (char*) &hdr, SizeOfXLogShortPHD, 0);
				writer_state.dest_curr_offset += nbytes;
			}
		}
		if (rem_data_len > 0)
		{
			if (! XlogRecFitsOnPage(writer_state.dest_curr_offset, rem_data_len))
			{
				uint64 data_len = BLCKSZ * (1 + (writer_state.dest_curr_offset / BLCKSZ)) - writer_state.dest_curr_offset;
				nbytes = write_buff2file(dst_fd, xlog_rec_buffer, data_len, already_read);
				rem_data_len = (rem_data_len - data_len);
				
				writer_state.dest_curr_offset += nbytes;
				already_read += nbytes;

				continue;
			}
			else
			{
				nbytes = write_buff2file(dst_fd, xlog_rec_buffer, rem_data_len, already_read);
				writer_state.dest_curr_offset += nbytes;

				if (MAXALIGN(record->xl_tot_len) - record->xl_tot_len)
				{
					nbytes = write_buff2file(dst_fd, null_buff, MAXALIGN(record->xl_tot_len) - record->xl_tot_len, 0);
					writer_state.dest_curr_offset += nbytes;
				}
				return;
			}
		}

		record = (XLogRecord*) xlog_rec_buffer;
		record->xl_prev = writer_state.last_read_rec + writer_state.page_addr;
		writer_state.last_read_rec = writer_state.dest_curr_offset;

		INIT_CRC32C(crc);
		COMP_CRC32C(crc, ((char *) record) + SizeOfXLogRecord, record->xl_tot_len - SizeOfXLogRecord);
		COMP_CRC32C(crc, (char *) record, offsetof(XLogRecord, xl_crc));
		FIN_CRC32C(crc);
		record->xl_crc = crc;

		/* TODO delete it later */
		// ereport(LOG, errmsg("Prev lsn : %X/%X\tTotal len : %d\tTotal len aligned : %ld\t Current LSN : %X/%X", 
		// 						LSN_FORMAT_ARGS(record->xl_prev), 
		// 						record->xl_tot_len, 
		// 						MAXALIGN(record->xl_tot_len),
		// 						LSN_FORMAT_ARGS(writer_state.dest_curr_offset)));

		if (! XlogRecFitsOnPage(writer_state.dest_curr_offset, record->xl_tot_len))
		{
			uint64 data_len = BLCKSZ * (1 + (writer_state.dest_curr_offset / BLCKSZ)) - writer_state.dest_curr_offset;
			rem_data_len = record->xl_tot_len - data_len;
			nbytes = write_buff2file(dst_fd, xlog_rec_buffer, data_len, 0);

			writer_state.dest_curr_offset += nbytes;
			already_read += nbytes;
			continue;
		}
		else
		{
			nbytes = write_buff2file(dst_fd, xlog_rec_buffer, record->xl_tot_len, 0);
			writer_state.dest_curr_offset += nbytes;

			if (MAXALIGN(record->xl_tot_len) - record->xl_tot_len > 0)
			{
				nbytes = write_buff2file(dst_fd, null_buff, MAXALIGN(record->xl_tot_len) - record->xl_tot_len, 0);
				writer_state.dest_curr_offset += nbytes;
			}
			return;
		}
	}
}

/*
 * Reads 'size' bytes from file to (char*)(buffer + buff_offset)
 */
int
read_file2buff(int fd, char* buffer, uint64 size, uint64 buff_offset, const char* file_name)
{
	int nbytes;

	pgstat_report_wait_start(WAIT_EVENT_COPY_FILE_READ);
	nbytes = read(fd, (char*) (buffer + buff_offset), size);
	if (nbytes < 0)
		ereport(ERROR,
			(errcode_for_file_access(),
			errmsg("could not read file \"%s\": %m", file_name)));
	if (nbytes == 0)
		ereport(WARNING,
			(errcode_for_file_access(),
			errmsg("file descriptor closed for read \"%d\": %m", fd)));
	pgstat_report_wait_end();
	
	return nbytes;
}

/*
 * Writes 'size' bytes from (char*)(buffer + buff_offset) to file
 */
int
write_buff2file(int fd, char* buffer, uint64 size, uint64 buff_offset)
{
	int nbytes;

	pgstat_report_wait_start(WAIT_EVENT_COPY_FILE_WRITE);

	nbytes = write(fd, (char*) (buffer + buff_offset), size);
	if (nbytes != size)
		ereport(ERROR,
			(errcode_for_file_access(),
			errmsg("could not write to file \"%d\": %m", fd)));
	if (nbytes == 0)
		ereport(ERROR,
			(errcode_for_file_access(),
			errmsg("file descriptor closed for write \"%d\": %m", fd)));

	pgstat_report_wait_end();

	return nbytes;
}

int 
WalReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
			XLogRecPtr targetPtr, char *readBuff)
{
	XLogDumpPrivate *private = state->private_data;
	int			count = XLOG_BLCKSZ;
	WALReadError errinfo;

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

	if (!WALRead(state, readBuff, targetPagePtr, count, private->timeline,
				 &errinfo))
	{
		WALOpenSegment *seg = &errinfo.wre_seg;
		char		fname[MAXPGPATH];

		XLogFileName(fname, seg->ws_tli, seg->ws_segno,
					 state->segcxt.ws_segsize);

		if (errinfo.wre_errno != 0)
		{
			errno = errinfo.wre_errno;
			ereport(ERROR, 
					errmsg("could not read from file %s, offset %d: %m",
					fname, errinfo.wre_off));
		}
		else
			ereport(ERROR,
					errmsg("could not read from file %s, offset %d: read %d of %d",
					fname, errinfo.wre_off, errinfo.wre_read,
					errinfo.wre_req));
	}

	return count;
}

void 
WalOpenSegment(XLogReaderState *state, XLogSegNo nextSegNo, TimeLineID *tli_p)
{
	TimeLineID tli = *tli_p;
    char fname[MAXPGPATH];
	char fpath[MAXPGPATH];

    XLogFileName(fname, tli, nextSegNo, state->segcxt.ws_segsize);

	if (snprintf(fpath, MAXPGPATH, "%s/%s", state->segcxt.ws_dir, fname) == -1)
		ereport(ERROR,
				errmsg("error during reading WAL absolute path : %s/%s", state->segcxt.ws_dir, fname));

	state->seg.ws_file = OpenTransientFile(fpath, O_RDONLY | PG_BINARY);
	if (state->seg.ws_file < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", fpath)));
}

void 
WalCloseSegment(XLogReaderState *state)
{
	close(state->seg.ws_file);
	state->seg.ws_file = -1;
}

/*
 * Extracts wal directory name from full path and stores it in writer_state
 */
void
getWalDirecotry(const char *path, const char *file)
{
	// char wal_dir[MAXPGPATH];

	// if (strlen(path) > MAXPGPATH)
	// 	ereport(ERROR,
	// 			errmsg("WAL file absolute name is too long : %s", path));

	// sprintf(wal_dir, "%s", path);

	// MemSet(wal_dir + (strlen(path) - strlen(file) - 1), 0, strlen(file));
	// ereport(LOG, 
	// 		errmsg("wal directory is : %s", wal_dir));

	// writer_state.src_dir = palloc0(strlen(wal_dir) + 1);
	// sprintf(writer_state.src_dir, "%s", wal_dir);

	writer_state.src_dir = palloc0(strlen(XLOGDIR) + 1);
	sprintf(writer_state.src_dir, "%s", XLOGDIR);
}

/*
 * Pritn record to stdout
 */
void
XLogDisplayRecord(XLogReaderState *record)
{
	const char *id;
	const RmgrData desc = GetRmgr(XLogRecGetRmid(record));
	uint32		rec_len;
	uint32		fpi_len;
	uint8		info = XLogRecGetInfo(record);
	XLogRecPtr	xl_prev = XLogRecGetPrev(record);
	StringInfoData s;

	XLogRecGetLen(record, &rec_len, &fpi_len);

	ereport(LOG, errmsg("rmgr: %-11s \nlen (rec/tot): %6u/%6u, \ntx: %10u, \nlsn: %X/%08X, \nprev %X/%08X, \n",
		   desc.rm_name,
		   rec_len, XLogRecGetTotalLen(record),
		   XLogRecGetXid(record),
		   LSN_FORMAT_ARGS(record->ReadRecPtr),
		   LSN_FORMAT_ARGS(xl_prev)));

	id = desc.rm_identify(info);
	if (id == NULL)
		ereport(LOG, errmsg("desc: UNKNOWN (%x) ", info & ~XLR_INFO_MASK));
	else
		ereport(LOG, errmsg("desc: %s ", id));

	initStringInfo(&s);
	desc.rm_desc(&s, record);
	ereport(LOG, errmsg("%s", s.data));

	resetStringInfo(&s);
	XLogRecGetBlockRefInfo(record, true, true, &s, NULL);
	ereport(LOG, errmsg("%s", s.data));
}

// don't need crc, 'cause it includes only the data, but switch record contains only the header
void 		
finish_wal_diff(char *xlog_rec_buffer)
{
	XLogRecord switch_record = {0};

	switch_record.xl_tot_len = SizeOfXLogRecord;
	switch_record.xl_info = XLOG_SWITCH;
	switch_record.xl_rmid = RM_XLOG_ID;

	memcpy(xlog_rec_buffer, (char*) &switch_record, SizeOfXLogRecord);

	if (writer_state.wal_segment_size - writer_state.dest_curr_offset >= SizeOfXLogRecord) 
	{
		write_one_xlog_rec(writer_state.dest_fd, writer_state.dest_path, xlog_rec_buffer);
		ereport(LOG, errmsg("writer_state.dest_curr_offset: %lu", writer_state.dest_curr_offset));
		ereport(LOG, errmsg("Added SWITCH record"));
	}
	
	ereport(LOG, errmsg("Finished the wal file"));

}