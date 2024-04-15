# contrib/wal_diff/Makefile

MODULE_big = wal_diff
OBJS = \
	wal_diff.o \
	wal_diff_func.o

PGFILEDESC = "wal_diff - archive module with compressing"

NO_INSTALLCHECK = 1
TAP_TESTS = 1

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
