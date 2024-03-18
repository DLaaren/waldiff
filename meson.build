wal_diff_sources = files(
  'wal_diff.c',
)

if host_system == 'windows'
  wal_diff_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'wal_diff',
    '--FILEDESC', 'wal_diff - archive module with compressing',])
endif

wal_diff = shared_module('wal_diff',
  wal_diff_sources,
  kwargs: contrib_mod_args, 
)
contrib_targets += wal_diff
