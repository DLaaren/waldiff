#include "waldiff_reader.h"

WALDIFFReaderState *WALDIFFReaderAllocate(int wal_segment_size,
										  const char *waldir,
										  WALDIFFReaderRoutine *routine)
{

}                                          

void WALDIFFReaderFree(WALDIFFReaderRoutine *state)
{

}

/* Position the XLogReader to the beginning */
void WALDIFFBeginRead(WALDIFFReaderRoutine *state, 
                      XLogRecPtr RecPtr)
{

}

void WALDIFFReadRecord(WALDIFFReaderRoutine *state,
					   char **errormsg)
{

}                              