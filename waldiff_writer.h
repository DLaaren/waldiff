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

/* Return values from WALDIFFRecordWriteCB. */
typedef enum WALDIFFRecordWriteResult
{
	WALDIFFWRITE_SUCCESS = 0,		/* record is successfully written */
	WALDIFFWRITE_FAIL = -1,			/* failed during writing a record */
} WALDIFFRecordWriteResult;

/* Function type definitions for various WALDIFFWriter interactions */
typedef WALDIFFRecordWriteResult (*WALDIFFRecordWriteCB) (WALDIFFWriterState *waldiff_writer);
typedef void (*WALDIFFWriterSegmentOpenCB) (WALDIFFSegmentContext *segcxt,
											WALDIFFSegment *seg);
typedef void (*WALDIFFWriterSegmentCloseCB) (WALDIFFSegment *seg);

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
    WALDIFFRecordWriteCB write_records;

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
    WALDIFFWriterSegmentCloseCB segment_close;

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
	WALDIFFSegment 	  	  seg;
	uint32			      segoff;

	/*
	 * System identifier of the waldiff files we're about to write.  
     * Set to zero (the default value) if unknown or unimportant.
	 */
	uint64 system_identifier;

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
	 * Buffer with current WALDIFF records to write
	 * Max size of the buffer = BLCKSZ
	 */
	char	   *writeBuf;
	uint32		writeBufSize;

	/* Buffer to hold error message */
	char	   *errormsg_buf;
	bool		errormsg_deferred;
};

/* Get a new WALDIFFWriter */
extern WALDIFFWriterState *WALDIFFWriterAllocate(int wal_segment_size,
										      const char *waldir,
										      WALDIFFWriterRoutine *routine);

/* Free a WALDIFFWriter */
extern void WALDIFFWriterFree(WALDIFFWriterState *state);

/* Position the WALDIFFWriter to the beginning */
extern void WALDIFFBeginWrite(WALDIFFWriterState *state, 
                              XLogRecPtr RecPtr,
							  XLogSegNo segNo, 
							  TimeLineID tli);


#endif /* _WALDIFF_WRITER_H_ */