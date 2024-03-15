#include "postgres.h"
#include "pg_config_manual.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "port.h"
#include "c.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/heapam_xlog.h"
#include "storage/bufpage.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h> // for "open" function
#include <unistd.h>
#include <stdio.h>

#ifndef DEFAULTTABLESPACE_OID
#define DEFAULTTABLESPACE_OID 1663
#endif

#ifndef GLOBALTABLESPACE_OID
#define GLOBALTABLESPACE_OID 1664
#endif

PG_MODULE_MAGIC;
typedef struct XLogDumpPrivate {
    TimeLineID timeline;
    XLogRecPtr startptr;
    XLogRecPtr endptr;
    bool  endptr_reached;
} XLogDumpPrivate;

static void fetch_readable_info_from_wal(const char* wal_file_name, const char* wal_dir_path);
static char* find_path2relation_directory(RelFileLocator rlocator, ForkNumber forknum);
static char* form_relation_filename(RelFileLocator rlocator, ForkNumber forknum);
int open_file_in_directory(const char *directory, const char *fname);

static int WalSegSz;

const char *const forkNames[] = {
	"main",						/* MAIN_FORKNUM */
	"fsm",						/* FSM_FORKNUM */
	"vm",						/* VISIBILITYMAP_FORKNUM */
	"init"						/* INIT_FORKNUM */
};

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

// TODO не забыть, что при создании копии, все имена каталогов оканчиваются на .tar (вроде как)
// TODO На будущее : если таблица достаточно большая (занимает как минимум два файла), то мы должны это учесть
static char* find_path2relation_directory(RelFileLocator rlocator, ForkNumber forknum) {
    char* path;
    if (rlocator.spcOid == GLOBALTABLESPACE_OID) {
        Assert(rlocator.dbOid == 0);
        if (forknum != MAIN_FORKNUM)
            path = psprintf("global");
        else
            path = psprintf("global");
    }
    else if (rlocator.spcOid == DEFAULTTABLESPACE_OID) {
        if (forknum != MAIN_FORKNUM)
            path = psprintf("base/%u", rlocator.dbOid);
        else
            path = psprintf("base/%u", rlocator.dbOid);
    }
    else {
        if (forknum != MAIN_FORKNUM)
			path = psprintf("pg_tblspc/%u/%s/%u", rlocator.spcOid, TABLESPACE_VERSION_DIRECTORY, rlocator.dbOid);
        else
            path = psprintf("pg_tblspc/%u/%s/%u", rlocator.spcOid, TABLESPACE_VERSION_DIRECTORY, rlocator.dbOid);
    }
    return path;
}

static char* form_relation_filename(RelFileLocator rlocator, ForkNumber forknum) {
    char* filename;

    if (rlocator.spcOid == GLOBALTABLESPACE_OID) {
        Assert(rlocator.dbOid == 0);
        if (forknum != MAIN_FORKNUM)
            filename = psprintf("%u_%s", rlocator.relNumber, forkNames[forknum]);
        else
            filename = psprintf("%u", rlocator.relNumber);
    }
    else if (rlocator.spcOid == DEFAULTTABLESPACE_OID) {
        if (forknum != MAIN_FORKNUM)
            filename = psprintf("%u_%s", rlocator.relNumber, forkNames[forknum]);
        else
            filename = psprintf("%u", rlocator.relNumber);
    }
    else {
        if (forknum != MAIN_FORKNUM)
			filename = psprintf("%u_%s", rlocator.relNumber, forkNames[forknum]);
        else
            filename = psprintf("%u/%u", rlocator.dbOid, rlocator.relNumber);
    }

    return filename;
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

static void fetch_from_insert(XLogReaderState* xlogreader) {
    RelFileLocator target_locator;
    BlockNumber blknum;
    ForkNumber forknum;
    ItemPointerData target_tid;
    xl_heap_insert *xlrec;
    char* data;
    Size data_len;
    HeapTupleHeader tuple_hdr;
    xl_heap_header xlhdr;

    union {
		HeapTupleHeaderData hdr;
		char		data[MaxHeapTupleSize];
	} tbuf;

    elog(INFO, "Got INSERT record\n");

    XLogRecGetBlockTag(xlogreader, 0, &target_locator, &forknum, &blknum); // указатель record в структуре XLogReaderSate указывает на последнюю декодированную запись
    xlrec = (xl_heap_insert *) XLogRecGetData(xlogreader);
    ItemPointerSetBlockNumber(&target_tid, blknum);
    ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);

    data = XLogRecGetBlockData(xlogreader, 0, &data_len); // насколько я понял, heap и heap2 держат информацию в одном единственном блоке

    /*
    насколько я понял, в целях экономии в insert или update записях содержатся не все данные, необходимые для HeapTupleHeader,
    поэтому вычитываем имеющиеся данные в xl_heap_header
    */
    memcpy((char *) &xlhdr, data, SizeOfHeapHeader);

    tuple_hdr = &tbuf.hdr; // инициализируем, чтобы не ругался Make
    MemSet((char *) tuple_hdr, 0, SizeofHeapTupleHeader);
    memcpy((char *) tuple_hdr + SizeofHeapTupleHeader, data + SizeOfHeapHeader, data_len - SizeOfHeapHeader);

    tuple_hdr->t_infomask2 = xlhdr.t_infomask2;
    tuple_hdr->t_infomask = xlhdr.t_infomask;
    tuple_hdr->t_hoff = xlhdr.t_hoff;
    HeapTupleHeaderSetXmin(tuple_hdr, XLogRecGetXid(xlogreader));
    HeapTupleHeaderSetCmin(tuple_hdr, FirstCommandId);
    tuple_hdr->t_ctid = target_tid;

    /*
    После этих действий, tuple_hdr может быть прикастована к Item и вставлена в страницу с offset = xlrec->offnum
    (указание для использования конкретного line pointer)

    Вставка в страницу происходит посредством обычного memcpy
    */

    // Попытаемя теперь узнать, к какой таблице относится данная запись
    elog(INFO, "Tablespace : %d\nDatabase : %d\nRelation : %d\n", target_locator.spcOid, target_locator.dbOid, target_locator.relNumber);
    /*
    Имея на руках target_locator, мы можем узнать табличное пространство, базу данных и отношение, к которому относится запись WAL :
    SELECT datname FROM pg_database WHERE oid = 'ваш_oid_базы_данных';
    SELECT spcname FROM pg_tablespace WHERE oid = 'ваш_oid_tablespace';
    SELECT relname FROM pg_class WHERE oid = 'ваш_oid_таблицы';
    */
}

static const char* pgdata_path = "/usr/local/pgsql/data"; // TODO это заглушка. не забыть удалить потом

static void fetch_from_update(XLogReaderState* xlogreader) {
    xl_heap_update* xlrec = (xl_heap_update *) XLogRecGetData(xlogreader);
    RelFileLocator rlocator;
	BlockNumber oldblk;
	BlockNumber newblk;
    ItemPointerData newtid;
    HeapTupleData oldtup;
    ForkNumber forknum;
    HeapTupleHeader htup;

    oldtup.t_data = NULL; // инициализируем, чтобы не ругался компилятор
	oldtup.t_len = 0;

    uint16 prefixlen = 0;
	uint16 suffixlen = 0;
    char* recdata;
    char* newtup;
    char *recdata_end;
    xl_heap_header xlhdr;
    Size data_len;
    Size tuplen;

    union
	{
		HeapTupleHeaderData hdr;
		char data[MaxHeapTupleSize];
	} tbuf;


    XLogRecGetBlockTag(xlogreader, 0, &rlocator, &forknum, &newblk);
	if (!XLogRecGetBlockTagExtended(xlogreader, 1, NULL, NULL, &oldblk, NULL))
		oldblk = newblk;

    ItemPointerSet(&newtid, newblk, xlrec->new_offnum);

    /*
    Тут мы формируем старую версию строки
    */
    if (rlocator.dbOid == 5 && rlocator.relNumber == 16397) { // TODO временная заглушка, естественно
        Page page;
        char* relation_directory_path;
        char* relation_file_name;
        char* full_relation_directory_path;
        int fd;
        PGAlignedXLogBlock buff;
        PageHeader page_hdr;
        int read_count;

        OffsetNumber offnum;
        ItemId lp;

        elog(INFO, "Tablespace : %d\tDatabase : %d\tRelation : %d\n", rlocator.spcOid, rlocator.dbOid, rlocator.relNumber);
        elog(INFO, "Forknumber : %d\nOldBlockNumber : %d\nInBLockOffset : %d\n", forknum, oldblk, newtid.ip_posid);

        relation_directory_path = find_path2relation_directory(rlocator, forknum);
        full_relation_directory_path = psprintf("%s/%s", pgdata_path, relation_directory_path);
        relation_file_name = form_relation_filename(rlocator, forknum);

        elog(INFO, "Full path to the relation : %s/%s\n", full_relation_directory_path, relation_file_name);

        fd = open_file_in_directory(full_relation_directory_path, relation_file_name);
        if (fd < 0) {
            elog(INFO, "Cannot not open file\n");
        }
        read_count = read(fd, buff.data, BLCKSZ);
        if (read_count == BLCKSZ) {
            elog(INFO, "Read from file succesfully\n");
        }
        close(fd);

        page = (Page) buff.data;
        page_hdr = (PageHeader) buff.data;
        elog(INFO, "Page lower offset : %d\tUpper offset : %d\tSpecial space : %d\n", page_hdr->pd_lower, page_hdr->pd_upper, page_hdr->pd_special);
        offnum = xlrec->old_offnum;
        if (PageGetMaxOffsetNumber(page) < offnum)
            elog(INFO, "Target tuple cannot be found on page\n");
        lp = PageGetItemId(page, offnum);

        htup = (HeapTupleHeader) PageGetItem(page, lp);

		oldtup.t_data = htup;
		oldtup.t_len = ItemIdGetLength(lp);

		htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
		htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
        // TODO после этого мы должны учесть еще и HOT update - см. в heap_xlog_update
        HeapTupleHeaderSetXmax(htup, xlrec->old_xmax);
		HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
		/* Set forward chain link in t_ctid */
		htup->t_ctid = newtid;

        pfree(relation_directory_path);
        pfree(full_relation_directory_path);
        pfree(relation_file_name);
    }

    /*
    Теперь сформируем новую версию строки
    */
    recdata = XLogRecGetBlockData(xlogreader, 0, &data_len);
    if (xlrec->flags & XLH_UPDATE_PREFIX_FROM_OLD) {
        Assert(newblk == oldblk);
        memcpy(&prefixlen, recdata, sizeof(uint16));
        recdata += sizeof(uint16);
    }
    if (xlrec->flags & XLH_UPDATE_SUFFIX_FROM_OLD) {
        Assert(newblk == oldblk);
        memcpy(&suffixlen, recdata, sizeof(uint16));
        recdata += sizeof(uint16);
    }
    memcpy((char *) &xlhdr, recdata, SizeOfHeapHeader);
    recdata += SizeOfHeapHeader;

    tuplen = recdata_end - recdata;
    htup = &tbuf.hdr;
    MemSet((char *) htup, 0, SizeofHeapTupleHeader);

    newtup = (char *) htup + SizeofHeapTupleHeader; // по факту вычитываем все данные в htup

    if (prefixlen > 0) {
        int	len;
        /* copy bitmap [+ padding] [+ oid] from WAL record */
        len = xlhdr.t_hoff - SizeofHeapTupleHeader;
        memcpy(newtup, recdata, len);
        recdata += len;
        newtup += len;

        /* copy prefix from old tuple */
        memcpy(newtup, (char *) oldtup.t_data + oldtup.t_data->t_hoff, prefixlen);
        newtup += prefixlen;

        /* copy new tuple data from WAL record */
        len = tuplen - (xlhdr.t_hoff - SizeofHeapTupleHeader);
        memcpy(newtup, recdata, len);
        recdata += len;
        newtup += len;
    }
    else {
        memcpy(newtup, recdata, tuplen);
        recdata += tuplen;
        newtup += tuplen;
    }

    if (suffixlen > 0)
        memcpy(newtup, (char *) oldtup.t_data + oldtup.t_len - suffixlen, suffixlen);

    htup->t_infomask2 = xlhdr.t_infomask2;
    htup->t_infomask = xlhdr.t_infomask;
    htup->t_hoff = xlhdr.t_hoff;

    HeapTupleHeaderSetXmin(htup, XLogRecGetXid(xlogreader));
    HeapTupleHeaderSetCmin(htup, FirstCommandId);
    HeapTupleHeaderSetXmax(htup, xlrec->new_xmax);
    htup->t_ctid = newtid;
}

PG_FUNCTION_INFO_V1(explain_wal_record);

Datum explain_wal_record(PG_FUNCTION_ARGS) {
    text* arg_1 = PG_GETARG_TEXT_PP(0);
    text* arg_2 = PG_GETARG_TEXT_PP(1);

    const char* wal_dir = text_to_cstring(arg_1);
    const char* wal_file = text_to_cstring(arg_2);

    fetch_readable_info_from_wal(wal_file, wal_dir);

    PG_RETURN_VOID();
}

static void fetch_readable_info_from_wal(const char* wal_file_name, const char* wal_dir_path) {
    /*
    Много чего взято из файла src/bin/pg_waldump/pg_waldump.c
    Если что, идем туда
    */
    PGAlignedXLogBlock buff; // local variable, holding a page buffer
    int read_count = 0;
    XLogDumpPrivate private;
    XLogSegNo	segno;
    XLogRecPtr first_record;
    XLogReaderState* xlogreader;

    XLogPageHeader page_hdr;
    XLogRecord* record;
    char* errmsg;
    uint8 info_bits;
    int fd;

    fd = open_file_in_directory(wal_dir_path, wal_file_name);
    if (fd < 0) {
        elog(INFO, "Cannot not open file\n");
    }
    
    read_count = read(fd, buff.data, XLOG_BLCKSZ);
    close(fd);

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

    page_hdr = (XLogPageHeader) xlogreader->readBuf;
    if (XLogPageHeaderSize(page_hdr) == SizeOfXLogLongPHD)
        elog(INFO, "Got long page header\n");
    else
        elog(INFO, "Got short page header\n");

    elog(INFO, "Remaining data from a previous page : %d\n", page_hdr->xlp_rem_len);
    
    while (true) {
        record = XLogReadRecord(xlogreader, &errmsg);
        // После этой функции, xlogreader->record указывает на структуру DecodedXLogRecord. При этом xlogreader->read_buf содержит непосредственно информацию, вычитанную из файла
        if (!record)
            break;
        if (record == InvalidXLogRecPtr) {
            elog(INFO, "XLogReadRecord failed to read record\n");
            return;
        }
        if (strcmp(GetRmgr(xlogreader->record->header.xl_rmid).rm_name, "Heap") == 0 || strcmp(GetRmgr(xlogreader->record->header.xl_rmid).rm_name, "Heap2") == 0) {
            // elog(INFO, "Resource manager : %s\n", GetRmgr(xlogreader->record->header.xl_rmid).rm_name);

            info_bits = XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK;
            if ((info_bits & XLOG_HEAP_OPMASK) == XLOG_HEAP_INSERT) {
                // fetch_from_insert(xlogreader);
                // break;
            }
            else if ((info_bits & XLOG_HEAP_OPMASK) == XLOG_HEAP_UPDATE || (info_bits & XLOG_HEAP_OPMASK) == XLOG_HEAP_HOT_UPDATE) { //TODO временно объединяем update и hot update
                fetch_from_update(xlogreader);
                // break;
            }
        }
    }
    
	XLogReaderFree(xlogreader);
}
