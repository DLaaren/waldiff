#include <postgres.h>
#include <pg_config_manual.h>
#include <xlog_internal.h>
#include <xlogreader.h>
// #include <port.h>
#include <c.h>

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h> // for "open" function
#include <unistd.h>

int pg_snprintf(char *str, size_t count, const char *fmt,...) pg_attribute_printf(3, 4);

typedef struct XLogDumpPrivate
{
 TimeLineID timeline;
 XLogRecPtr startptr;
 XLogRecPtr endptr;
 bool  endptr_reached;
} XLogDumpPrivate;

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
    int tries;

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

int main(int argc, char** argv) {

    /*
    Find out WAL segment size, meanwhile make sure, that file exists
    */
    PGAlignedXLogBlock buff; // local variable, holding a page buffer
    int read_count = 0;
    int WalSegSz = 0;

    if (argc < 3) {
        printf("Too few params\n");
        return 0;
    }

    const char* wal_dir = argv[1];
    const char* wal_file = argv[2];
    int fd = open_file_in_directory(wal_dir, wal_file);
    
    read_count = read(fd, buff.data, XLOG_BLCKSZ);
    if (read_count == XLOG_BLCKSZ) {
        XLogLongPageHeader longhdr = (XLogLongPageHeader) buff.data;
        WalSegSz = longhdr->xlp_seg_size;
        if (!IsValidWalSegSize(WalSegSz)) {
            printf("Invalid wal segment size : %d\n", WalSegSz);
        }
    }
    else {
        printf("Cannot read file\n");
        perror("Error :");
    }

    XLogDumpPrivate private;
    XLogReaderState* xlogreader = XLogReaderAllocate(WalSegSz, wal_dir, 
                                XL_ROUTINE(.page_read = PageReadCallback, 
                                            .segment_open = OpenSegmentCallback, 
                                            .segment_close = CloseSegmentCallback), &private);
        
    return 0;
}