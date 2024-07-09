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
	WALDIFFWRITE_EOF = 1,
} WALDIFFRecordWriteResult;

/* Function type definitions for various WALDIFFWriter interactions */
typedef WALDIFFRecordWriteResult (*WALDIFFRecordWriteCB) (WALDIFFWriterState *waldiff_writer, char *record);
typedef void (*WALDIFFWriterSegmentOpenCB) (WALSegment *seg, int flags);
typedef void (*WALDIFFWriterSegmentCloseCB) (WALSegment *seg);

typedef struct WALDIFFWriterRoutine
{
    /* 
	 * This callback shall write given record to WALDIFF segment
	 */
    WALDIFFRecordWriteCB write_record;

    /*
	 * Callback to open the specified WALDIFF segment for writing
	 */
    WALDIFFWriterSegmentOpenCB segment_open;

    /*
	 * WALDIFF segment close callback
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
	WALSegment			  waldiff_seg;

	/*
	 * System identifier of the waldiff files we're about to write.  
     * Set to zero (the default value) if unknown or unimportant.
	 */
	uint64 system_identifier; // TODO do we need this?
	
	/*
	 * This field contains total number of bytes, written to WALDIFF segment
	 */
	Size already_written;

	/*
	 * Addres of first page in wal segment. This value also stored in
	 * long page header
	 */
	XLogRecPtr first_page_addr; // TODO do we need this?

	/* Buffer to hold error message */
	char	   *errormsg_buf;
	bool		errormsg_deferred;
};

#define WALDIFFWriterGetRestOfBufCapacity(writer) (((writer)->buffer_capacity) - ((writer)->buffer_fullness))
#define WALDIFFWriterGetErrMsg(writer) ((writer)->errormsg_buf)
#define WALDIFFWriterGetLastRecordWritten(writer) ((writer)->waldiff_seg.last_processed_record)

/* Get a new WALDIFFWriter */
extern WALDIFFWriterState *WALDIFFWriterAllocate(int wal_segment_size,
										      	 char *waldiff_dir,
										      	 WALDIFFWriterRoutine *routine,
												 Size buffer_capacity);

/* Free a WALDIFFWriter */
extern void WALDIFFWriterFree(WALDIFFWriterState *state);

/* Position the WALDIFFWriter to the beginning */
extern void WALDIFFBeginWrite(WALDIFFWriterState *state, 
							  XLogSegNo segNo, 
							  TimeLineID tli,
							  int flags);

extern int write_data_to_file(WALDIFFWriterState *writer, char *data, uint64 data_size);
extern void WALDIFFFinishWithZeros(WALDIFFWriterState *writer);

/*
 * Declaration of WALDIFFWriterState routine implementations
 */
extern WALDIFFRecordWriteResult WALDIFFWriteRecord(WALDIFFWriterState *writer, char *record);
extern void WALDIFFOpenSegment(WALSegment *seg, int flags);
extern void WALDIFFCloseSegment(WALSegment *seg);

#endif /* _WALDIFF_WRITER_H_ */
