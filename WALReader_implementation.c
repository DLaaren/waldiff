#include "postgres.h"
#include "pg_config_manual.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "port.h"
#include "c.h"
#include "fmgr.h"
#include "utils/builtins.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h> // for "open" function
#include <unistd.h>

PG_MODULE_MAGIC;
typedef struct XLogDumpPrivate {
    TimeLineID timeline;
    XLogRecPtr startptr;
    XLogRecPtr endptr;
    bool  endptr_reached;
} XLogDumpPrivate;

static void _read_header(const char* wal_file_name, const char* wal_dir_path);
int open_file_in_directory(const char *directory, const char *fname);

static int WalSegSz;

/*
 * Open the file in the valid target directory.
 * return a read only fd
 */
int open_file_in_directory(const char *directory, const char *fname) {
    int fd = -1;
    char fpath[MAXPGPATH];

    snprintf(fpath, MAXPGPATH, "%s/%s", directory, fname);
    fd = open(fpath, O_RDONLY | PG_BINARY, 0);
    if (fd < 0) {
        perror("Error :");
    }

    if (fd < 0 && errno != ENOENT)
    printf("could not open file \"%s\": %m", fname);
    
    return fd;
}

static int PageReadCallback (XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen, XLogRecPtr targetPtr, char *readBuff) {
    XLogDumpPrivate *private = state->private_data;
    int count = XLOG_BLCKSZ;
    WALReadError errinfo;

    if (private->endptr != InvalidXLogRecPtr) {
        if (targetPagePtr + XLOG_BLCKSZ <= private->endptr)
            count = XLOG_BLCKSZ;
        else if (targetPagePtr + reqLen <= private->endptr)
            count = private->endptr - targetPagePtr;
        else {
            private->endptr_reached = true;
            return -1;
        }
    }

    if (!WALRead(state, readBuff, targetPagePtr, count, private->timeline, &errinfo)) {
        WALOpenSegment *seg = &errinfo.wre_seg;
        char fname[MAXPGPATH];

        XLogFileName(fname, seg->ws_tli, seg->ws_segno, state->segcxt.ws_segsize);

        if (errinfo.wre_errno != 0) {
            errno = errinfo.wre_errno;
            printf("Could not read from file\n");
        }
        else
            printf("could not read from file");
    }

    return count;
}

static void OpenSegmentCallback(XLogReaderState *state, XLogSegNo nextSegNo, TimeLineID *tli_p) {
    TimeLineID tli = *tli_p;
    char fname[MAXPGPATH];

    XLogFileName(fname, tli, nextSegNo, state->segcxt.ws_segsize);

    /*
    * In follow mode there is a short period of time after the server has
    * written the end of the previous file before the new file is available.
    * So we loop for 5 seconds looking for the file to appear before giving
    * up.
    */
    state->seg.ws_file = open_file_in_directory(state->segcxt.ws_dir, fname);

    if (state->seg.ws_file >= 0) return;
    else
        printf("Could not find file\n");
}

static void CloseSegmentCallback(XLogReaderState *state) {
    close(state->seg.ws_file);
    state->seg.ws_file = -1;
}

PG_FUNCTION_INFO_V1(read_header);

Datum read_header(PG_FUNCTION_ARGS) {
    text* arg_1 = PG_GETARG_TEXT_PP(0);
    text* arg_2 = PG_GETARG_TEXT_PP(1);

    const char* wal_dir = text_to_cstring(arg_1);
    const char* wal_file = text_to_cstring(arg_2);

    _read_header(wal_file, wal_dir);

    PG_RETURN_VOID();
}

static void _read_header(const char* wal_file_name, const char* wal_dir_path) {
    PGAlignedXLogBlock buff; // local variable, holding a page buffer
    int read_count = 0;
    XLogDumpPrivate private;
    XLogSegNo	segno;
    XLogRecPtr first_record;
    char* errmsg;
    XLogReaderState* xlogreader;
    XLogRecord* record;

    int headers_length = 0;

    int fd = open_file_in_directory(wal_dir_path, wal_file_name);
    if (fd < 0) {
        elog(INFO, "Cannot not open file\n");
    }
    
    read_count = read(fd, buff.data, XLOG_BLCKSZ);
    if (read_count == XLOG_BLCKSZ) {
        XLogLongPageHeader longhdr = (XLogLongPageHeader) buff.data;
        WalSegSz = longhdr->xlp_seg_size;
        if (!IsValidWalSegSize(WalSegSz)) {
            elog(INFO, "Invalid wal segment size : %d\n", WalSegSz);
        }
    }
    else {
        elog(INFO, "Cannot read file\n");
    }

    memset(&private, 0, sizeof(XLogDumpPrivate));
    private.timeline = 1;
	private.startptr = InvalidXLogRecPtr;
	private.endptr = InvalidXLogRecPtr;
	private.endptr_reached = false;

    XLogFromFileName(wal_file_name, &(private.timeline), &segno, WalSegSz);
    XLogSegNoOffsetToRecPtr(segno, 0, WalSegSz, private.startptr);

    xlogreader = XLogReaderAllocate(WalSegSz, wal_dir_path, 
                                XL_ROUTINE(.page_read = PageReadCallback, 
                                            .segment_open = OpenSegmentCallback, 
                                            .segment_close = CloseSegmentCallback), &private);
    if (!xlogreader) {
        elog(FATAL, "out of memory while allocating a WAL reading processor");
        return;
    }
    
    first_record = XLogFindNextRecord(xlogreader, private.startptr);

	if (first_record == InvalidXLogRecPtr) {
        elog(FATAL, "could not find a valid record after %X/%X", LSN_FORMAT_ARGS(private.startptr));
        return;
    }
    
    record = XLogReadRecord(xlogreader, &errmsg);
    // После этой функции, xlogreader->record указывает на структуру DecodedXLogRecord. При этом xlogreader->read_buf содержит непосредственно информацию, вычитанную из файла

    if (record == InvalidXLogRecPtr) {
        elog(INFO, "XLogReadRecord failed to read first record\n");
        return;
    }    

    elog(INFO, "Location of read record : %ld\n", xlogreader->record->lsn);
    elog(INFO, "Max block id : %d\n", xlogreader->record->max_block_id);

    headers_length = xlogreader->record->size - 24 - xlogreader->record->main_data_len;
    if (headers_length > 2) { // that means that we have at least one XLogRecordBlockHeader
        XLogRecordBlockHeader* block_header = (XLogRecordBlockHeader*) (xlogreader->readBuf + SizeOfXLogRecord);
        elog(INFO, "block reference id : %hhu\t payload bytes %hu\n", block_header->id, block_header->data_length);
    }
}
