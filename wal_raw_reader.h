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

#define TMP_BUFFER_CAPACITY BLCKSZ*2

typedef struct WALRawReaderState WALRawReaderState;

/* Return values from WALRecordReadCB. */
typedef enum WALRawRecordReadResult
{
	WALREAD_SUCCESS = 0,		/* record is successfully read */
	WALREAD_FAIL = -1,			/* failed during reading a record */
    WALREAD_EOF = 1,            /* end of wal file reached */
} WALRawRecordReadResult;

/* Return values from WALRecordSkipCB. */
typedef enum WALRawRecordSkipResult
{
	WALSKIP_SUCCESS = 0,		/* record is successfully read */
	WALSKIP_FAIL = -1,			/* failed during reading a record */
    WALSKIP_EOF = 1,            /* end of wal file reached */
} WALRawRecordSkipResult;

/* Function type definitions for various WALReader interactions */
typedef WALRawRecordReadResult (*WALRawRecordReadCB) (WALRawReaderState *waldiff_reader, XLogRecord *record); // TODO do we need XLogRecord parameter?
typedef WALRawRecordSkipResult (*WALRawRecordSkipCB) (WALRawReaderState *waldiff_reader, XLogRecord *record);
typedef void (*WALRawReaderSegmentOpenCB) (WALSegment *seg, int flags);
typedef void (*WALRawReaderSegmentCloseCB) (WALSegment *seg);

typedef struct WALRawReaderRoutine
{
    /* 
	 * This callback shall read next raw record from WAL segment
	 * and write it to internal buffer
	 */
    WALRawRecordReadCB read_record;

	/* 
	 * This callback shall skip record with given header from WAL segment
	 */
    WALRawRecordSkipCB skip_record;

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
	char *tmp_buffer;
	Size  tmp_buffer_fullness;
	
	/*
	 * This field contains total number of bytes, read from WAL file.
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

#define WALRawReaderGetRestBufferCapacity(reader) ((reader)->buffer_capacity - (reader)->buffer_fullness)
#define WALRawReaderGetRestTmpBufferCapacity(reader) (TMP_BUFFER_CAPACITY - (reader)->tmp_buffer_fullness)
#define WALRawReaderGetErrMsg(reader) ((reader)->errormsg_buf)
#define WALRawReaderGetLastRecordRead(reader) ((reader)->buffer)

/*
 * Whether XLogRecord fits on the page with given offset from start of 
 * WAL file
 */
#define XlogRecHdrFitsOnPage(file_offset) \
( \
	(file_offset) + SizeOfXLogRecord < \
	BLCKSZ * (1 + (file_offset) / BLCKSZ) \
)

/*
 * Whether record with given length fits on the page with given offset
 * from start of WAL file
 */
#define XlogRecFitsOnPage(file_offset, rec_len) \
( \
	(file_offset) + (rec_len) < \
	BLCKSZ * (1 + (file_offset) / BLCKSZ) \
)

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
							TimeLineID tli,
							int flags);

extern int  append_to_buff(WALRawReaderState* raw_reader, uint64 size);
extern void reset_buff(WALRawReaderState* raw_reader);

extern int  append_to_tmp_buff(WALRawReaderState* raw_reader, uint64 size);
extern void reset_tmp_buff(WALRawReaderState* raw_reader);

/*
 * Declaration of WALDIFFWriterState routine implementations
 */
extern WALRawRecordSkipResult WALSkipRawRecord(WALRawReaderState *raw_reader, XLogRecord *target);
extern WALRawRecordReadResult WALReadRawRecord(WALRawReaderState *raw_reader, XLogRecord *target);

#endif /* _WALREADER_H_ */
