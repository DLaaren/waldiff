#include "waldiff_writer.h"

WALDIFFWriterState *WALDIFFWriterAllocate(int wal_segment_size,
									      const char *waldir,
									      WALDIFFWriterRoutine *routine)
{

}

void WALDIFFWriterFree(WALDIFFWriterRoutine *state)
{

}

void WALDIFFBeginWrite(WALDIFFWriterRoutine *state, XLogRecPtr RecPtr)
{

}

void WALDIFFWriteRecord(WALDIFFWriterState *state, char **errormsg)
{
    
}