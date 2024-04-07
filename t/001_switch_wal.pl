use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::RecursiveCopy;
use PostgreSQL::Test::Utils;
use Test::More;

# test plans:
# 1 check wal file saving 
# 2 check insert detection
# 3 check update detection
# 4 check hot update detection
# 5 check delete detection

# start up a node
# my = local variable 
my $node = PostgreSQL::Test::Cluster->new('main');

# Create a data directory with initdb
$node->init;

$node->append_conf(
    'postgresql.conf', 
    q{
        wal_level = 'replica'
        archive_mode = 'on'
        archive_library = 'wal_diff'
        wal_diff.wal_diff_directory = 'wal_diff'
    }
);

# Start the PostgreSQL server
$node->start;

# достаем имя wal файла
my ($walfile_name, $blocksize) = split '\|' => $node->safe_psql('postgres',
	"SELECT pg_walfile_name(pg_switch_wal()), current_setting('block_size')");

ok("Switched wal");

sleep(1);

my $wal_file = $node->data_dir . '/pg_wal/' . $walfile_name;

# -f = plain file
# -e = exists
ok(-f -e $wal_file, "Got a wal file");

my $wal_diff_file = $node->data_dir . '/wal_diff/' . $walfile_name;

sleep(3600);

ok(-f -e $wal_diff_file, "Got a wal diff file");

# Stop the server
$node->stop('immediate');

done_testing();