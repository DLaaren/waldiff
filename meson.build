waldiff_sources = files(
  'waldiff.h', 'waldiff_writer.h', 'waldiff.c', 'waldiff_writer.c' 
)

if host_system == 'windows'
  wal_diff_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'waldiff',
    '--FILEDESC', 'waldiff - archive module with compressing',])
endif

wal_diff = shared_module('waldiff',
  waldiff_sources,
  kwargs: contrib_mod_args, 
)
contrib_targets += waldiff

tests += {
  'name': 'waldiff',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_switch_wal.pl',
      't/002_check_waldiff.pl',
    ],
  },
}
