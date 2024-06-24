/*-------------------------------------------------------------------------
 *
 * wal_raw_reader.h
 *	  Definitions for the WAL reading facility
 * 
 *-------------------------------------------------------------------------
 */
#ifndef _WAL_RAW_READER_H_
#define _WAL_RAW_READER_H_

#include "waldiff.h"

typedef struct WALRawReaderState WALRawReaderState;

/* Return values from WALRecordReadCB. */
typedef enum WALRawRecordReadResult
{
	WALREAD_SUCCESS = 0,		/* record is successfully read */
	WALREAD_FAIL = -1,			/* failed during reading a record */
    WALREAD_EOF = 1,            /* end of wal file reached */
} WALRawRecordReadResult;

/* Function type definitions for various WALReader interactions */
typedef WALRawRecordReadResult (*WALRawRecordReadCB) (WALRawReaderState *waldiff_reader, XLogRecord *record);
typedef void (*WALRawReaderSegmentOpenCB) (WALSegment *seg);
typedef void (*WALRawReaderSegmentCloseCB) (WALSegment *seg);

typedef struct WALRawReaderRoutine
{
    /* 
	 * This callback shall read raw record with given header from WAL segment
	 * and write it to internal buffer
	 */
    WALRawRecordReadCB read_record;

    /*
	 * Callback to open the specified WAL segment for reading
	 */
    WALRawReaderSegmentOpenCB segment_open;

    /*
	 * WAL segment close callback
	 */
    WALRawReaderSegmentCloseCB segment_close;

} WALRawReaderRoutine;

#define WALRAWREADER_ROUTINE(...) &(WALRawReaderRoutine){__VA_ARGS__}

struct WALRawReaderState
{
    /*
	 * Operational callbacks
	 */
    WALRawReaderRoutine routine;

    /*
     * Segment context
     */
	WALSegment wal_seg;

	/*
	 * System identifier of the WAL files we're about to read.  
	 */
	uint64 system_identifier;

    /*
	 * Buffer with last read record from WAL file
	 */
	char	   *buffer;
	Size		buffer_fullness;
	Size 		buffer_capacity;

	/*
	 * Buffer for internal logic
	 */
	char* tmp_buffer;
	
	/*
	 * This field contains total number of bytes, read from buffer.
	 */
	Size already_read;

	/*
	 * Addres of first page in wal segment. This value also stored in
	 * long page header
	 */
	XLogRecPtr first_page_addr;

	/* Buffer to hold error message */
	char	   *errormsg_buf;
	bool		errormsg_deferred;
};

#define WALRawReaderGetRestOfBufCapacity(reader) (((reader)->buffer_capacity) - ((reader)->buffer_fullness))
#define WALRawReaderGetErrMsg(reader) ((reader)->errormsg_buf)
#define WALRawReaderGetLastRecordRead(reader) ((reader)->wal_seg.last_processed_record)

/* Get a new WALReader */
extern WALRawReaderState *WALRawReaderAllocate(int wal_segment_size,
										  char* wal_dir,
										  WALRawReaderRoutine *routine,
										  Size buffer_capacity);

/* Free a WALReader */
extern void WALRawReaderFree(WALRawReaderState *state);

/* Position the WALReader to the beginning */
extern void WALRawBeginRead(WALRawReaderState *state, 
							  XLogSegNo segNo, 
							  TimeLineID tli);

#endif /* _WALREADER_H_ */
