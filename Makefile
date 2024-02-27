# contrib/wal_diff/Makefile

MODULE_big = wal_diff
OBJS = \
	WALReader_implementation.o

EXTENSION = wal_diff
DATA = wal_diff--1.0.sql

PGFILEDESC = "wal_diff"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/wal_diff
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif