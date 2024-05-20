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

/* Function type definitions for various WALDIFFWriter interactions */
typedef int (*WALDIFFRecordReadCB) (WALDIFFReaderState *waldiff_reader,
							        XLogRecPtr targetPagePtr,
							        int recordLen,
									XLogRecPtr targetRecPtr,
							        char *readBuf);
typedef void (*WALDIFFReaderSegmentOpenCB) (WALDIFFReaderState *waldiff_reader,
								      	XLogSegNo nextSegNo,
								      	TimeLineID *tli_p);
typedef void (*WALDIFFReaderSegmentCloseCB) (WALDIFFReaderState *waldiff_reader);

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
    WALDIFFRecordReadCB record_read;

    /*
	 * Callback to open the specified WALDIFF segment for writing.  
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
    WALDIFFReaderSegmentCloseCB    segment_close;

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
	WALDIFFOpenSegment seg;
	uint32		segoff;

	/*
	 * System identifier of the waldiff files we're about to write.  
     * Set to zero (the default value) if unknown or unimportant.
	 */
	uint64		system_identifier;

    /*
	 * Start and end point of last record read.  
     * EndRecPtr is also used as the position to read next.  
     * Calling WALDIFFBeginRead() sets EndRecPtr to the
	 * starting position and StartRecPtr to invalid.
	 *
	 * Start and end point of last record returned by WALDIFFReadRecord().
     * These are also available as record->lsn and record->next_lsn.
	 */
	XLogRecPtr	StartRecPtr;	/* start of last record written */
	XLogRecPtr	EndRecPtr;		/* end+1 of last record written */

    /*
	 * Buffer for reading WAL record
	 */
	char	   *readRecordBuf;
	uint32		readRecordBufSize;

	/* Buffer to hold error message */
	char	   *errormsg_buf;
	bool		errormsg_deferred;
};

/* Get a new WALDIFFWriter */
extern WALDIFFReaderState *WALDIFFReaderAllocate(int wal_segment_size,
										      	 const char *waldir,
										      	 WALDIFFReaderRoutine *routine);

/* Free a WALDIFFWriter */
extern void WALDIFFReaderFree(WALDIFFReaderRoutine *state);

/* Position the XLogReader to the beginning */
extern void WALDIFFBeginRead(WALDIFFReaderRoutine *state, 
                             XLogRecPtr RecPtr);

/* Return values from WALDIFFRecordWriteCB. */
typedef enum WALDIFFRecordReadResult
{
	WALDIFFREAD_SUCCESS = 0,		/* record is successfully read */
	WALDIFFREAD_FAIL = -1,			/* failed during reading a record */
} WALDIFFRecordReadResult;

/* Write WALDIFF record. Returns NULL on end-of-WALDIFF or failure */
extern void WALDIFFReadRecord(WALDIFFReaderRoutine *state,
						      char **errormsg);


#endif /* _WALDIFF_READER_H_ */