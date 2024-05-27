/*-------------------------------------------------------------------------
 *
 * waldiff_reader.h
 *	  Definitions for the WALDIFF reading facility
 * 
 *-------------------------------------------------------------------------
 */
#ifndef _WALDIFF_READER_H_
#define _WALDIFF_READER_H_

#include "waldiff.h"


typedef struct WALDIFFReaderState WALDIFFReaderState;

/* Return values from WALDIFFRecordReadCB. */
typedef enum WALDIFFRecordReadResult
{
	WALDIFFREAD_SUCCESS = 0,		/* record is successfully read */
	WALDIFFREAD_EOF = 1,
	WALDIFFREAD_FAIL = -1,			/* failed during reading a record */
} WALDIFFRecordReadResult;

/* Function type definitions for various WALDIFFReader interactions */
typedef WALDIFFRecordReadResult (*WALDIFFRecordReadCB) (WALDIFFReaderState *waldiff_reader);
typedef void (*WALDIFFReaderSegmentOpenCB) (WALDIFFSegmentContext *segcxt,
											WALDIFFSegment *seg);
typedef void (*WALDIFFReaderSegmentCloseCB) (WALDIFFSegment *seg);

typedef struct WALDIFFReaderRoutine
{
    /*
	 * This callback shall read recordLen valid bytes from readBuf
     * from the WAL page starting at targetPagePtr.  The callback
	 * shall return the number of bytes read (never more than XLOG_BLCKSZ), 
     * or -1 on failure.
	 *
	 * targetRecPtr is the position of the WALDIFF record we're reading from.
	 *
	 * The callback shall set ->seg.ws_tli to the TLI of the file the page was
	 * read from.
	 */
    WALDIFFRecordReadCB read_record;

    /*
	 * Callback to open the specified WALDIFF segment for reading.  
     * ->seg.wds_fd shall be set to the file descriptor of the opened segment.  
     * In case of failure, an error shall be raised by the callback and it 
     * shall not return.
	 *
	 * "nextSegNo" is the number of the segment to be opened.
	 */
    WALDIFFReaderSegmentOpenCB segment_open;

    /*
	 * WALDIFF segment close callback.  ->seg.ws_fd shall be set to a negative
	 * number.
	 */
    WALDIFFReaderSegmentCloseCB segment_close;

} WALDIFFReaderRoutine;

#define WALDIFFREADER_ROUTINE(...) &(WALDIFFReaderRoutine){__VA_ARGS__}

struct WALDIFFReaderState
{
	// let it be here for now
	XLogReaderState *xlog_reader;
	
    /*
	 * Operational callbacks
	 */
    WALDIFFReaderRoutine routine;

    /*
     * Segment context
     */
    WALDIFFSegmentContext segcxt;
	WALDIFFSegment    	  seg;
	uint32		          segoff;

	/*
	 * System identifier of the waldiff files we're about to read.  
     * Set to zero (the default value) if unknown or unimportant.
	 */
	uint64 system_identifier;

    /*
	 * Start and end point of last record read.  
     * EndRecPtr is also used as the position to read next.  
     * Calling WALDIFFBeginRead() sets EndRecPtr to the
	 * starting position and StartRecPtr to invalid.
	 *
	 * Start and end point of last record returned by WALDIFFReadRecord().
     * These are also available as record->lsn and record->next_lsn.
	 */
	XLogRecPtr	StartRecPtr;	/* start of last record read */
	XLogRecPtr	EndRecPtr;		/* end+1 of last record read */

    /*
	 * Recently read WAL record
	 */
	XLogRecord	*record;

	/* Buffer to hold error message */
	char	   *errormsg_buf;
	bool		errormsg_deferred;
};

/*
 * Macros that provide access to parts of the record 
 */
#define WALGetRec(reader) ((reader)->record)
#define WALRecGetTotalLen(record) ((record)->xl_tot_len)
#define WALRecGetPrev(record) ((record)->xl_prev)
#define WALRecGetInfo(record) ((record)->xl_info)
#define WALRecGetRmid(record) ((record)->xl_rmid)
#define WALRecGetXid(record) ((record)->xl_xid)
#define WALRecGetXLogType(record) ((record)->xl_info & XLR_RMGR_INFO_MASK & XLOG_HEAP_OPMASK)

/* Get a new WALDIFFReader */
extern WALDIFFReaderState *WALDIFFReaderAllocate(int wal_segment_size,
										      	 const char *wal_dir,
										      	 WALDIFFReaderRoutine *routine);

/* Free a WALDIFFWReader */
extern void WALDIFFReaderFree(WALDIFFReaderState *state);

/* Position the WALDIFFWReader to the beginning */
extern void WALDIFFBeginRead(WALDIFFReaderState *state, 
							 XLogRecPtr RecPtr, 
							 XLogSegNo segNo, 
							 TimeLineID tli);


#endif /* _WALDIFF_READER_H_ */