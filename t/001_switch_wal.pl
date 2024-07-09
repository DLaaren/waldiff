use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::RecursiveCopy;
use PostgreSQL::Test::Utils;
use Test::More;

#
# Checking if the archive modules creates waldiff direcorty
# and WALDIFF with the same name in than directory
#

# start up a node
# my = local variable 
my $node = PostgreSQL::Test::Cluster->new('main');

# Create a data directory with initdb
$node->init;

$node->append_conf(
    'postgresql.conf', 
    q(
        shared_preload_libraries = 'waldiff'
        wal_level = replica
        archive_mode = on
        archive_library = 'waldiff'
        waldiff.waldiff_dir = 'waldiff'
    )
);

# Start the PostgreSQL server
$node->start;

# достаем имя wal файла
my ($walfile_name, $blocksize) = split '\|' => $node->safe_psql('postgres',
	"SELECT pg_walfile_name(pg_switch_wal()), current_setting('block_size')");

ok("Switched wal");

sleep(2);

my $wal_file = $node->data_dir . '/pg_wal/' . $walfile_name;

# -f = plain file
# -e = exists
ok(-f -e $wal_file, "Got a wal file");

my $waldiff_file = $node->data_dir . '/waldiff/' . $walfile_name;

sleep(2);

ok(-f -e $waldiff_file, "Got a wal diff file");


sleep(2);

# Stop the server
$node->stop('immediate');

done_testing();