# contrib/waldiff/Makefile

MODULE_big = waldiff
OBJS = \
	waldiff_rmgr.o \
	waldiff.o \
	waldiff_writer.o \
	wal_raw_reader.o \
	waldiff_test.o 
	
PGFILEDESC = "waldiff - archive module with compressing"

NO_INSTALLCHECK = 1
TAP_TESTS = 1

EXTENSION = waldiff
DATA = waldiff--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/waldiff
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
