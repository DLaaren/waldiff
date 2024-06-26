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

static text* create_error_msg(char* error);
static int update_writer_state_table();
static int get_actual_writer_state();


PG_FUNCTION_INFO_V1(read_raw_xlog_rec);
PG_FUNCTION_INFO_V1(write_raw_xlog_rec);

Datum
read_raw_xlog_rec(PG_FUNCTION_ARGS)
{
	// text*		arg_0 			 = PG_GETARG_TEXT_PP(0);
	// int			wal_file_fd;
	// char 		wal_file_name[255];

    // WALRawReaderState* raw_reader = WALRawReaderAllocate(wal_segment_size, 
	// 									                XLOGDIR, 
	// 									                WALRAWREADER_ROUTINE(.read_record   = WALReadRawRecord,
    //                                                                          .skip_record  = WALSkipRawRecord,
    //                                                                          .segment_open  = WALDIFFOpenSegment,
    //                                                                          .segment_close = WALDIFFCloseSegment), 
	// 									                XLogRecordMaxSize);

	// memcpy(wal_file_name, VARDATA(arg_0), VARSIZE_ANY_EXHDR(arg_0));
	// wal_file_name[VARSIZE_ANY_EXHDR(arg_0)] = '\0';

	// res = get_actual_writer_state();
	// if (res < 0)
	// {
	// 	pfree(tmp_buffer);
	// 	pfree(xlog_rec_buffer);

	// 	PG_RETURN_TEXT_P(create_error_msg("Error while processing spi command"));
	// }

	// wal_file_fd = OpenTransientFile(wal_file_name, O_RDONLY | PG_BINARY);
	// if (wal_file_fd < 0)
	// {
	// 	pfree(tmp_buffer);
	// 	pfree(xlog_rec_buffer);

	// 	PG_RETURN_TEXT_P(create_error_msg("Error while opening wal segment file"));
	// }

	// memset(xlog_rec_buffer, 0, XLogRecordMaxSize);
	// lseek(wal_file_fd, src_file_offset, SEEK_SET);
	// read_count = read_one_xlog_rec(wal_file_fd, wal_file_name, xlog_rec_buffer, tmp_buffer, UINT64_MAX, &read_only_header);
	// src_file_offset = lseek(wal_file_fd, 0, SEEK_CUR);
	// ereport(LOG, errmsg("READ LEN : %d", read_count));
	
	// res = update_writer_state_table(writer_state);
	// if (res < 0)
	// {
	// 	pfree(tmp_buffer);
	// 	pfree(xlog_rec_buffer);
	// 	PG_RETURN_TEXT_P(create_error_msg("Error while processing spi command"));
	// }

	// if (read_count == -1 || read_only_header)
	// {
	// 	pfree(tmp_buffer);
	// 	pfree(xlog_rec_buffer);
	// 	CloseTransientFile(wal_file_fd);

	// 	PG_RETURN_TEXT_P(create_error_msg("Error while reading wal segment file"));
	// }

	// CloseTransientFile(wal_file_fd);
	// pfree(tmp_buffer);
	// pfree(xlog_rec_buffer);

	PG_RETURN_VOID();
}

Datum
write_raw_xlog_rec(PG_FUNCTION_ARGS)
{
	// bytea* arg_1 			= PG_GETARG_BYTEA_P(1);
	// text*  arg_0 			= PG_GETARG_TEXT_PP(0);

	// char*  xlog_rec_buffer  = palloc0(XLogRecordMaxSize);
	// int	   wal_file_fd;
	// char   wal_diff_file_name[255];

	// int res = get_actual_writer_state();
	// if (res < 0)
	// {
	// 	ereport(ERROR, errmsg("Error while processing spi command"));
	// 	pfree(xlog_rec_buffer);
	// 	PG_RETURN_VOID();
	// }

	// memcpy(wal_diff_file_name, VARDATA(arg_0), VARSIZE_ANY_EXHDR(arg_0));
	// wal_diff_file_name[VARSIZE_ANY_EXHDR(arg_0)] = '\0';

	// wal_file_fd = OpenTransientFile(wal_diff_file_name,  O_RDWR | O_CREAT | O_APPEND | PG_BINARY);
	// if (wal_file_fd < 0)
	// {
	// 	ereport(ERROR, errmsg("Cannot open wal file : %s", wal_diff_file_name));
	// 	CloseTransientFile(wal_file_fd);
	// 	pfree(xlog_rec_buffer);
	// 	PG_RETURN_VOID();
	// }

	// memset(xlog_rec_buffer, 0, XLogRecordMaxSize);
	// memcpy(xlog_rec_buffer, VARDATA(arg_1), VARSIZE_ANY_EXHDR(arg_1));

	// lseek(wal_file_fd, writer_state.dest_curr_offset, SEEK_SET);
	// write_one_xlog_rec(wal_file_fd, wal_diff_file_name, xlog_rec_buffer);

	// res = update_writer_state_table(writer_state);
	// if (res < 0)
	// {
	// 	ereport(ERROR, errmsg("Error while processing spi command"));
	// 	CloseTransientFile(wal_file_fd);
	// 	pfree(xlog_rec_buffer);
	// 	PG_RETURN_VOID();
	// }

	// CloseTransientFile(wal_file_fd);
	// pfree(xlog_rec_buffer);
	PG_RETURN_VOID();
}

static text* 
create_error_msg(char* error)
{
	// text* res;
	// char  err_msg_str[255];
	// int   err_msg_len = strlen(error);

	// ereport(ERROR, errmsg(error));

	// memcpy(err_msg_str, error, err_msg_len);
	// err_msg_str[strlen(error)] = '\0';

	// res = (text*) palloc0(err_msg_len + VARHDRSZ);
	// memcpy(VARDATA(res), err_msg_str, err_msg_len);
	// SET_VARSIZE(res, err_msg_len + VARHDRSZ);

	// return res;

	return NULL;
}

static int 
update_writer_state_table()
{
	// Oid	param_types[] = {
	// 	BIGINT_OID, // sys_id
	// 	BIGINT_OID, // page_addr
	// 	INTEGER_OID, // tli
	// 	TEXT_OID, // dest_dir
	// 	BIGINT_OID, // dest_curr_offset
	// 	INTEGER_OID, // wal_segment_size
	// 	BIGINT_OID, // last_read_rec
	// 	BIGINT_OID // src_fd_pos
	// };
	// Datum params_datum[] = {
	// 	UInt64GetDatum(writer_state.sys_id),
	// 	UInt64GetDatum(writer_state.page_addr),
	// 	UInt32GetDatum(writer_state.tli),
	// 	CStringGetTextDatum(writer_state.dest_dir),
	// 	UInt64GetDatum(writer_state.dest_curr_offset),
	// 	Int32GetDatum(writer_state.wal_segment_size),
	// 	UInt64GetDatum(writer_state.last_read_rec),
	// 	UInt64GetDatum(src_file_offset)
	// };

	// int res = SPI_connect();
	// if (res == SPI_ERROR_CONNECT)
	// {
	// 	ereport(ERROR, errmsg("Cannot connext to spi"));
	// 	return -1;
	// }

	// res = SPI_execute_with_args(
	// 		"UPDATE writer_state SET sys_id = $1, page_addr = $2, tli = $3, dest_dir = $4, dest_curr_offset = $5,"
	// 		"wal_segment_size = $6, last_read_rec = $7, src_fd_pos = $8 WHERE id = 1;", 
	// 		8, param_types, params_datum, NULL, false, 0);
	// if (res != SPI_OK_UPDATE)
	// {
	// 	ereport(ERROR, errmsg("Cannot update table writer_state"));
	// 	return -1;
	// }

	// SPI_finish();

	return 0;
}

static int 
get_actual_writer_state()
{
	// Datum  read_tuples_datum;
	// bool   is_null;

	// int res = SPI_connect();
	// if (res == SPI_ERROR_CONNECT)
	// {
	// 	ereport(ERROR, errmsg("Cannot connext to spi"));
	// 	return -1;
	// }

	// res = SPI_execute(
	// 		"SELECT sys_id, page_addr, tli, dest_dir, dest_curr_offset,"
	// 		"wal_segment_size, last_read_rec, src_fd_pos FROM writer_state WHERE id = 1;",
	// 		false, 0);
	// if (res != SPI_OK_SELECT)
	// {
	// 	ereport(ERROR, errmsg("Cannot select from table writer_state"));
	// 	return -1;
	// }

	// read_tuples_datum   = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &is_null);
	// writer_state.sys_id = DatumGetUInt64(read_tuples_datum);

	// read_tuples_datum 	   = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &is_null);
	// writer_state.page_addr = DatumGetUInt64(read_tuples_datum);

	// read_tuples_datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3, &is_null);
	// writer_state.tli  = DatumGetUInt32(read_tuples_datum);

	// read_tuples_datum 	  = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 4, &is_null);
	// writer_state.dest_dir = TextDatumGetCString(read_tuples_datum);

	// read_tuples_datum 			  = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 5, &is_null);
	// writer_state.dest_curr_offset = DatumGetUInt64(read_tuples_datum);

	// read_tuples_datum 			  = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 6, &is_null);
	// writer_state.wal_segment_size = DatumGetInt32(read_tuples_datum);

	// read_tuples_datum   		= SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 7, &is_null);
	// writer_state.last_read_rec  = DatumGetUInt64(read_tuples_datum);

	// read_tuples_datum 	= SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 8, &is_null);
	// src_file_offset 	= DatumGetUInt64(read_tuples_datum);

	// SPI_finish();

	return 0;
}
