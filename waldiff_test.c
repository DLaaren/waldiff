#include "postgres.h"

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "access/xlogdefs.h"
#include "fmgr.h"
#include "utils/pg_lsn.h"
#include "utils/builtins.h"
#include "varatt.h"
#include <string.h>
#include <unistd.h>
#include <stdio.h>

#include "waldiff_writer.h"
#include "wal_raw_reader.h"
#include "executor/spi.h"

#define TEXT_OID 25
#define INTEGER_OID 23
#define BIGINT_OID 20

#define WALDIFF_DIR "waldiff"

static text* create_error_msg(char* error);

static int update_raw_reader_state_table();
static int update_writer_state_table();

static int get_actual_raw_reader_state();
static int get_actual_writer_state();

static int 		  wal_seg_size;
static XLogSegNo  segNo;
static TimeLineID tli;

static WALDIFFWriterState*  writer = NULL;
static WALRawReaderState*   raw_reader = NULL;

PG_FUNCTION_INFO_V1(read_raw_xlog_rec);
PG_FUNCTION_INFO_V1(write_raw_xlog_rec);

Datum
read_raw_xlog_rec(PG_FUNCTION_ARGS)
{
	text*		arg_0 			 = PG_GETARG_TEXT_PP(0);
	int			res;
	char 		wal_file_name[255];

	WALRawRecordReadResult read_result;
	wal_seg_size = 16777216; // synthetic example

    raw_reader = WALRawReaderAllocate(wal_seg_size, 
									XLOGDIR, 
									WALRAWREADER_ROUTINE(.read_record   = WALReadRawRecord,
															.skip_record  = WALSkipRawRecord,
															.segment_open  = WALDIFFOpenSegment,
															.segment_close = WALDIFFCloseSegment), 
									XLogRecordMaxSize);

	memcpy(wal_file_name, VARDATA(arg_0), VARSIZE_ANY_EXHDR(arg_0));
	wal_file_name[VARSIZE_ANY_EXHDR(arg_0)] = '\0';

	res = get_actual_raw_reader_state();
	if (res < 0)
	{
		WALRawReaderFree(raw_reader);
		PG_RETURN_TEXT_P(create_error_msg("Error while processing spi command"));
	}

	WALRawBeginRead(raw_reader, segNo, tli, O_RDONLY | PG_BINARY);
	if (raw_reader->wal_seg.fd < 0)
	{
		WALRawReaderFree(raw_reader);
		PG_RETURN_TEXT_P(create_error_msg("Error while opening wal segment file"));
	}

	lseek(raw_reader->wal_seg.fd, raw_reader->already_read, SEEK_SET);
	read_result = WALReadRawRecord(raw_reader, NULL);

	if (read_result == WALREAD_EOF)
	{
		WALRawReaderFree(raw_reader);
		PG_RETURN_TEXT_P(create_error_msg("End of WAL segment file reached"));
	}
	else if (read_result == WALREAD_FAIL)
	{
		WALRawReaderFree(raw_reader);
		PG_RETURN_TEXT_P(create_error_msg("Error during reading raw record"));
	}
	
	res = update_raw_reader_state_table();
	if (res < 0)
	{
		WALRawReaderFree(raw_reader);
		PG_RETURN_TEXT_P(create_error_msg("Error while processing spi command"));
	}
	
	WALRawReaderFree(raw_reader);
	PG_RETURN_VOID();
}

Datum
write_raw_xlog_rec(PG_FUNCTION_ARGS)
{
	bytea* arg_1 			= PG_GETARG_BYTEA_P(1);
	text*  arg_0 			= PG_GETARG_TEXT_PP(0);

	char*  xlog_rec_buffer  = palloc0(XLogRecordMaxSize);
	int    res;
	char   wal_diff_file_name[255];

	WALDIFFRecordWriteResult write_result;
	wal_seg_size = 16777216; // synthetic example
	writer = WALDIFFWriterAllocate(wal_seg_size, 
									   WALDIFF_DIR,
									   WALDIFFWRITER_ROUTINE(.write_record  = WALDIFFWriteRecord,
															 .segment_open  = WALDIFFOpenSegment,
															 .segment_close = WALDIFFCloseSegment),
									   XLogRecordMaxSize + SizeOfXLogLongPHD);

	memcpy(wal_diff_file_name, VARDATA(arg_0), VARSIZE_ANY_EXHDR(arg_0));
	wal_diff_file_name[VARSIZE_ANY_EXHDR(arg_0)] = '\0';

	res = get_actual_writer_state();
	if (res < 0)
	{
		WALDIFFWriterFree(writer);
		PG_RETURN_VOID();
	}

	WALDIFFBeginWrite(writer, segNo, tli, O_RDWR | O_CREAT | O_APPEND | PG_BINARY);
	if (writer->waldiff_seg.fd < 0)
	{
		WALDIFFWriterFree(writer);
		ereport(ERROR, errmsg("Error while opening waldiff segment file"));
		PG_RETURN_VOID();
	}

	memset(xlog_rec_buffer, 0, XLogRecordMaxSize);
	memcpy(xlog_rec_buffer, VARDATA(arg_1), VARSIZE_ANY_EXHDR(arg_1));

	lseek(writer->waldiff_seg.fd, writer->already_written, SEEK_SET);
	write_result = WALDIFFWriteRecord(writer, xlog_rec_buffer);

	if (write_result == WALDIFFWRITE_EOF)
	{
		WALDIFFWriterFree(writer);
		ereport(ERROR, errmsg("End of WAL segment file reached"));
		PG_RETURN_VOID();
	}
	else if (write_result == WALDIFFWRITE_FAIL)
	{
		WALDIFFWriterFree(writer);
		ereport(ERROR, errmsg("Error during reading raw record"));
		PG_RETURN_VOID();
	}

	res = update_writer_state_table();
	if (res < 0)
	{
		WALDIFFWriterFree(writer);
		ereport(ERROR, errmsg("Error while processing spi command"));
		PG_RETURN_VOID();
	}

	WALDIFFWriterFree(writer);
	PG_RETURN_VOID();
}

static text* 
create_error_msg(char* error)
{
	text* res;
	char  err_msg_str[255];
	int   err_msg_len = strlen(error);

	ereport(ERROR, errmsg(error));

	memcpy(err_msg_str, error, err_msg_len);
	err_msg_str[strlen(error)] = '\0';

	res = (text*) palloc0(err_msg_len + VARHDRSZ);
	memcpy(VARDATA(res), err_msg_str, err_msg_len);
	SET_VARSIZE(res, err_msg_len + VARHDRSZ);

	return res;
}

static int 
update_raw_reader_state_table()
{
	Oid	param_types[] = {
		INTEGER_OID, // segno
		BIGINT_OID, // tli
		BIGINT_OID, // system_identifier
		TEXT_OID, // tmp_buffer
		BIGINT_OID, // tmp_buffer_fullness
		BIGINT_OID, // buffer_fullness
		TEXT_OID, // buffer
		BIGINT_OID, // already_read
		BIGINT_OID, // first_page_addr
	};
	Datum params_datum[] = {
		UInt64GetDatum(raw_reader->wal_seg.segno),
		UInt32GetDatum(raw_reader->wal_seg.tli),
		UInt64GetDatum(raw_reader->system_identifier),
		CStringGetTextDatum(raw_reader->tmp_buffer),
		UInt64GetDatum(raw_reader->tmp_buffer_fullness),
		UInt64GetDatum(raw_reader->buffer_fullness),
		CStringGetTextDatum(raw_reader->buffer),
		UInt64GetDatum(raw_reader->already_read),
		UInt64GetDatum(raw_reader->first_page_addr)
	};

	int res = SPI_connect();
	if (res == SPI_ERROR_CONNECT)
	{
		ereport(ERROR, errmsg("Cannot connext to spi"));
		return -1;
	}

	res = SPI_execute_with_args(
			"UPDATE raw_reader_state SET segno = $1, tli = $2, system_identifier = $3, tmp_buffer = $4, tmp_buffer_fullness = $5,"
			"buffer_fullness = $6, buffer = $7, already_read = $8, first_page_addr = $9  WHERE id = 1;", 
			9, param_types, params_datum, NULL, false, 0);
	if (res != SPI_OK_UPDATE)
	{
		ereport(ERROR, errmsg("Cannot update table raw_reader_state"));
		return -1;
	}

	SPI_finish();

	return 0;
}

static int 
update_writer_state_table()
{
	Oid	param_types[] = {
		BIGINT_OID, // segno
		BIGINT_OID, // tli
		BIGINT_OID, // system_identifier
		BIGINT_OID, // already_written
		BIGINT_OID, // first_page_addr
	};
	Datum params_datum[] = {
		UInt64GetDatum(writer->waldiff_seg.segno),
		UInt64GetDatum(writer->waldiff_seg.tli),
		UInt64GetDatum(writer->system_identifier),
		UInt64GetDatum(writer->already_written),
		UInt64GetDatum(writer->first_page_addr)
	};

	int res = SPI_connect();
	if (res == SPI_ERROR_CONNECT)
	{
		ereport(ERROR, errmsg("Cannot connext to spi"));
		return -1;
	}

	res = SPI_execute_with_args(
			"UPDATE writer_state SET segno = $1, tli = $2, system_identifier = $3, already_written = $4, first_page_addr = $5  WHERE id = 1;",
			5, param_types, params_datum, NULL, false, 0);
	if (res != SPI_OK_UPDATE)
	{
		ereport(ERROR, errmsg("Cannot update table writer_state"));
		return -1;
	}

	SPI_finish();

	return 0;
}

static int 
get_actual_raw_reader_state()
{
	Datum  read_tuples_datum;
	bool   is_null;
	char* tmp;

	int res = SPI_connect();
	if (res == SPI_ERROR_CONNECT)
	{
		ereport(ERROR, errmsg("Cannot connext to spi"));
		return -1;
	}

	res = SPI_execute(
			"SELECT segno, tli, tmp_buffer, tmp_buffer_fullness, buffer_fullness, buffer,"
			"system_identifier, first_page_addr, already_read FROM raw_reader_state WHERE id = 1;",
			false, 0);
	if (res != SPI_OK_SELECT)
	{
		ereport(ERROR, errmsg("Cannot select from table raw_reader_state"));
		return -1;
	}

	read_tuples_datum   = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &is_null);
	segNo = DatumGetUInt64(read_tuples_datum);

	read_tuples_datum 	   = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &is_null);
	tli = DatumGetUInt32(read_tuples_datum);

	read_tuples_datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3, &is_null);
	if (!is_null)
	{
		tmp = TextDatumGetCString(read_tuples_datum);
		memcpy(raw_reader->tmp_buffer, tmp, TMP_BUFFER_CAPACITY);
	}

	read_tuples_datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 4, &is_null);
	raw_reader->tmp_buffer_fullness = DatumGetUInt64(read_tuples_datum);

	read_tuples_datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 5, &is_null);
	raw_reader->buffer_fullness = DatumGetUInt64(read_tuples_datum);

	raw_reader->buffer_capacity = XLogRecordMaxSize;

	read_tuples_datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 6, &is_null);
	if (! is_null)
	{
		tmp = TextDatumGetCString(read_tuples_datum);
		memcpy(raw_reader->buffer, tmp, raw_reader->buffer_fullness);
	}

	read_tuples_datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 7, &is_null);
	raw_reader->system_identifier = DatumGetUInt64(read_tuples_datum);

	read_tuples_datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 8, &is_null);
	raw_reader->first_page_addr = DatumGetUInt64(read_tuples_datum);

	read_tuples_datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 9, &is_null);
	raw_reader->already_read = DatumGetUInt64(read_tuples_datum);

	SPI_finish();

	return 0;
}

static int 
get_actual_writer_state()
{
	Datum  read_tuples_datum;
	bool   is_null;

	int res = SPI_connect();
	if (res == SPI_ERROR_CONNECT)
	{
		ereport(ERROR, errmsg("Cannot connext to spi"));
		return -1;
	}

	res = SPI_execute(
			"SELECT segno, tli, system_identifier, first_page_addr, already_written FROM writer_state WHERE id = 1;",
			false, 0);
	if (res != SPI_OK_SELECT)
	{
		ereport(ERROR, errmsg("Cannot select from table raw_reader_state"));
		return -1;
	}

	read_tuples_datum   = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &is_null);
	segNo = DatumGetUInt64(read_tuples_datum);

	read_tuples_datum 	   = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &is_null);
	tli = DatumGetUInt32(read_tuples_datum);

	read_tuples_datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3, &is_null);
	writer->system_identifier = DatumGetUInt64(read_tuples_datum);

	read_tuples_datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 4, &is_null);
	writer->first_page_addr = DatumGetUInt64(read_tuples_datum);

	read_tuples_datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 5, &is_null);
	writer->already_written = DatumGetUInt64(read_tuples_datum);

	SPI_finish();

	return 0;
}