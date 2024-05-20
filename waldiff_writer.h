/*-------------------------------------------------------------------------
 *
 * waldiff_writer.h
 *	  Definitions for the WALDIFF writing facility
 * 
 *-------------------------------------------------------------------------
 */
#ifndef _WALDIFF_WRITER_H_
#define _WALDIFF_WRITER_H_

#include "waldiff.h"

typedef struct WALDIFFWriterState WALDIFFWriterState;

/* Function type definitions for various WALDIFFWriter interactions */
typedef int (*WALDIFFRecordWriteCB) (WALDIFFWriterState *waldiff_writer,
							         XLogRecPtr targetPagePtr,
							         int recordLen,
									 XLogRecPtr targetRecPtr,
							         char *writeBuf);
typedef void (*WALDIFFWriterSegmentOpenCB) (WALDIFFWriterState *waldiff_writer,
								      		XLogSegNo nextSegNo,
								      		TimeLineID *tli_p);
typedef void (*WALDIFFWriterSegmentCloseCB) (WALDIFFWriterState *waldiff_writer);

typedef struct WALDIFFWriterRoutine
{
    /*
	 * This callback shall write recordLen valid bytes from writeBuf
     * to the WALDIFF page starting at targetPagePtr.  The callback
	 * shall return the number of bytes written (never more than XLOG_BLCKSZ), 
     * or -1 on failure.
	 *
	 * targetRecPtr is the position of the WALDIFF record we're writing to.
	 *
	 * The callback shall set ->seg.wds_tli to the TLI of the file the page was
	 * written to.
	 */
    WALDIFFRecordWriteCB record_write;

    /*
	 * Callback to open the specified WALDIFF segment for writing.  
     * ->seg.wds_fd shall be set to the file descriptor of the opened segment.  
     * In case of failure, an error shall be raised by the callback and it 
     * shall not return.
	 *
	 * "nextSegNo" is the number of the segment to be opened.
	 */
    WALDIFFWriterSegmentOpenCB segment_open;

    /*
	 * WALDIFF segment close callback.  ->seg.ws_fd shall be set to a negative
	 * number.
	 */
    WALDIFFWriterSegmentCloseCB    segment_close;

} WALDIFFWriterRoutine;

#define WALDIFFWRITER_ROUTINE(...) &(WALDIFFWriterRoutine){__VA_ARGS__}

struct WALDIFFWriterState
{
    /*
	 * Operational callbacks
	 */
    WALDIFFWriterRoutine routine;

    /*
     * Segment context
     */
    WALDIFFSegmentContext segcxt;
	WALDIFFOpenSegment seg;
	uint32		segoff;

	/*
	 * System identifier of the waldiff files we're about to write.  
     * Set to zero (the default value) if unknown or unimportant.
	 */
	uint64		system_identifier;

    /*
	 * Start and end point of last record written.  
     * EndRecPtr is also used as the position to write to next.  
     * Calling WALDIFFBeginWrite() sets EndRecPtr to the
	 * starting position and StartRecPtr to invalid.
	 *
	 * Start and end point of last record returned by WALDIFFWriteRecord().
     * These are also available as record->lsn and record->next_lsn.
	 */
	XLogRecPtr	StartRecPtr;	/* start of last record written */
	XLogRecPtr	EndRecPtr;		/* end+1 of last record written */

    /*
	 * Buffer with current WALDIFF record to write
	 */
	char	   *writeRecordBuf;
	uint32		writeRecordBufSize;

	/* Buffer to hold error message */
	char	   *errormsg_buf;
	bool		errormsg_deferred;
};

/* Get a new WALDIFFWriter */
extern WALDIFFWriterState *WALDIFFWriterAllocate(int wal_segment_size,
										      const char *waldir,
										      WALDIFFWriterRoutine *routine);

/* Free a WALDIFFWriter */
extern void WALDIFFWriterFree(WALDIFFWriterRoutine *state);

/* Position the XLogReader to the beginning */
extern void WALDIFFBeginWrite(WALDIFFWriterRoutine *state, 
                              XLogRecPtr RecPtr);

/* Return values from WALDIFFRecordWriteCB. */
typedef enum WALDIFFRecordWriteResult
{
	WALDIFFWRITE_SUCCESS = 0,		/* record is successfully written */
	WALDIFFWRITE_FAIL = -1,			/* failed during writing a record */
} WALDIFFRecordWriteResult;

/* Write WALDIFF record. Returns NULL on end-of-WALDIFF or failure */
extern void WALDIFFWriteRecord(WALDIFFWriterState *state,
						       char **errormsg);


#endif /* _WALDIFF_WRITER_H_ */