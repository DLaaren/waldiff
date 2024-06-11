#include "waldiff_reader.h"

WALDIFFReaderState *
WALDIFFReaderAllocate(int wal_segment_size,
					  char *wal_dir,
					  WALDIFFReaderRoutine *routine)
{
	WALDIFFReaderState *state;
	state = (WALDIFFReaderState *)
		palloc_extended(sizeof(WALDIFFReaderState),
						MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
	if (!state)
		return NULL;

	state->routine = *routine;

	state->readBuf =(char *) palloc_extended(XLogRecordMaxSize,
											MCXT_ALLOC_NO_OOM);
	if (!state->readBuf)
	{
		pfree(state);
		return NULL;
	}					

	state->seg.fd = -1;
	state->seg.segno = 0;
	state->seg.tli = 0;

	state->segcxt.segsize = wal_segment_size;
	if (wal_dir)
		state->segcxt.dir = wal_dir;

	/* ReadRecPtr, EndRecPtr and readLen initialized to zeroes above */
	state->errormsg_buf = palloc_extended(MAX_ERRORMSG_LEN + 1,
										  MCXT_ALLOC_NO_OOM);
	if (!state->errormsg_buf)
	{
		pfree(state->readBuf);
		pfree(state);
		return NULL;
	}
	state->errormsg_buf[0] = '\0';

	if (state->readBuf)
		pfree(state->readBuf);
	state->readBuf = (char *) palloc(BLCKSZ);
	state->readBuf[0] = '\0';
	state->readBufSize = 0;

	return state;	
}                                          

void
WALDIFFReaderFree(WALDIFFReaderState *state)
{
	if (state->seg.fd != -1)
		state->routine.segment_close(&(state->seg));

	pfree(state->errormsg_buf);
	pfree(state->readBuf);
	pfree(state);
}

void 
WALDIFFBeginRead(WALDIFFReaderState *state, 
				 XLogRecPtr RecPtr, 
				 XLogSegNo segNo, 
				 TimeLineID tli)
{
	Assert(!XLogRecPtrIsInvalid(RecPtr));
	
	state->EndRecPtr = RecPtr;
	state->StartRecPtr = InvalidXLogRecPtr;

	state->seg.segno = segNo;
	state->seg.tli = tli;

	state->routine.segment_open(&(state->segcxt), &(state->seg));
}                         