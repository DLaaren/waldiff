# contrib/wal_diff/Makefile

MODULES = wal_diff
PGFILEDESC = "wal_diff - archive module with compressing"

NO_INSTALLCHECK = 1


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

