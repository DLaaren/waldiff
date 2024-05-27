#include "waldiff_reader.h"

WALDIFFReaderState *
WALDIFFReaderAllocate(int wal_segment_size,
					  const char *wal_dir,
					  WALDIFFReaderRoutine *routine)
{
	WALDIFFReaderState *state;
	state = (WALDIFFReaderState *)
		palloc_extended(sizeof(WALDIFFReaderState),
						MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
	if (!state)
		return NULL;

	state->routine = *routine;

	state->record =(char *) palloc_extended(XLogRecordMaxSize,
											MCXT_ALLOC_NO_OOM);
	if (!state->record)
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
		pfree(state->record);
		pfree(state);
		return NULL;
	}
	state->errormsg_buf[0] = '\0';

	if (state->record)
		pfree(state->record);
	state->record = (char *) palloc0(XLogRecordMaxSize);

	return state;	
}                                          

void
WALDIFFReaderFree(WALDIFFReaderState *state)
{
	if (state->seg.fd != -1)
		state->routine.segment_close(&(state->seg));

	pfree(state->errormsg_buf);
	pfree(state->record);
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

	state->routine.segment_open(&(state->seg), &(state->segcxt));
}                         