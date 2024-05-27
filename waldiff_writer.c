#include "waldiff_writer.h"


WALDIFFWriterState *
WALDIFFWriterAllocate(int wal_segment_size,
					  const char *waldiff_dir,
					  WALDIFFWriterRoutine *routine)
{
	WALDIFFWriterState *state;
	state = (WALDIFFWriterState *)
		palloc_extended(sizeof(WALDIFFWriterState),
						MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
	if (!state)
		return NULL;

	state->routine = *routine;

	state->writeBuf =(char *) palloc_extended(XLogRecordMaxSize,
											  MCXT_ALLOC_NO_OOM);

	if (!state->writeBuf)
	{
		pfree(state);
		return NULL;
	}					

	state->seg.fd = -1;
	state->seg.segno = 0;
	state->seg.tli = 0;

	state->segcxt.segsize = wal_segment_size;
	if (waldiff_dir)
		state->segcxt.dir = waldiff_dir;

	/* ReadRecPtr, EndRecPtr and readLen initialized to zeroes above */
	state->errormsg_buf = palloc_extended(MAX_ERRORMSG_LEN + 1,
										  MCXT_ALLOC_NO_OOM);
	if (!state->errormsg_buf)
	{
		pfree(state->writeBuf);
		pfree(state);
		return NULL;
	}
	state->errormsg_buf[0] = '\0';

	if (state->writeBuf)
		pfree(state->writeBuf);
	state->writeBuf = (char *) palloc(BLCKSZ);
	state->writeBuf[0] = '\0';
	state->writeBufSize = 0;

	return state;						
}

void 
WALDIFFWriterFree(WALDIFFWriterState *state)
{
	if (state->seg.fd != -1)
		state->routine.segment_close(&(state->seg));

	pfree(state->errormsg_buf);
	pfree(state->writeBuf);
	pfree(state);
}

void 
WALDIFFBeginWrite(WALDIFFWriterState *state,
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