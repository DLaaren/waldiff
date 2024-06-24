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
typedef WALDIFFRecordWriteResult (*WALDIFFRecordWriteCB) (WALDIFFWriterState *waldiff_writer,
														  XLogRecord *record,
														  XLogReaderState* reader);
typedef void (*WALDIFFWriterSegmentOpenCB) (WALSegment *seg);
typedef void (*WALDIFFWriterSegmentCloseCB) (WALSegment *seg);

typedef struct WALDIFFWriterRoutine
{
    /* 
	 * This callback shall read given record from WAL segment
	 * and write it to WALDIFF segment
	 */
    WALDIFFRecordWriteCB write_records;

    /*
	 * Callback to open the specified WAL/WALDIFF segment for writing
	 */
    WALDIFFWriterSegmentOpenCB segment_open;

    /*
	 * WAL/WALDIFF segment close callback
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
     * Segments context
     */
	WALSegment 	  	  	  wal_seg;
	WALSegment			  waldiff_seg;

	/*
	 * System identifier of the waldiff files we're about to write.  
     * Set to zero (the default value) if unknown or unimportant.
	 */
	uint64 system_identifier;

	/*
	 * Address of last record, written to buffer
	 */
	XLogRecPtr last_record_written;

    /*
	 * Buffer with current WALDIFF records to write
	 * Max size of the buffer = WALDIFF_WRITER_BUFF_CAPACITY
	 */
	char	   *buffer;
	Size		buffer_fullness;
	Size 		buffer_capacity;
	
	/*
	 * This field contains total number of bytes, written to waldiff segment.
	 */
	Size already_written;

	/*
	 * Addres of first page in wal segment. This value also stored in
	 * long page header
	 */
	XLogRecPtr first_page_addr;

	/* Buffer to hold error message */
	char	   *errormsg_buf;
	bool		errormsg_deferred;
};

#define WALDIFFWriterGetRestOfBufCapacity(writer) (((writer)->buffer_capacity) - ((writer)->buffer_fullness))
#define WALDIFFWriterGetErrMsg(writer) ((writer)->errormsg_buf)

/* Get a new WALDIFFWriter */
extern WALDIFFWriterState *WALDIFFWriterAllocate(int wal_segment_size,
										      	 char *waldiff_dir,
					  							 char* wal_dir,
										      	 WALDIFFWriterRoutine *routine,
												 Size buffer_capacity);

/* Free a WALDIFFWriter */
extern void WALDIFFWriterFree(WALDIFFWriterState *state);

/* Position the WALDIFFWriter to the beginning */
extern void WALDIFFBeginWrite(WALDIFFWriterState *state, 
							  XLogSegNo segNo, 
							  TimeLineID tli);

extern WALDIFFRecordWriteResult WALDIFFFlushBuffer(WALDIFFWriterState *state);


#endif /* _WALDIFF_WRITER_H_ */
