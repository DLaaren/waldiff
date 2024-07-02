use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::RecursiveCopy;
use PostgreSQL::Test::Utils;
use Test::More;
use File::Copy;

#
# Checking if a databse can be recovered from WALDIFF
# If it does, then WALDIFF can be written and read corecctly
#

# start up a node
# my = local variable 
my $node = PostgreSQL::Test::Cluster->new('main');

# Create a data directory with initdb
$node->init();

$node->append_conf(
    'postgresql.conf', 
    q{
        wal_level = 'replica'
        archive_mode = 'on'
        archive_library = 'waldiff'
        waldiff.waldiff_dir = 'waldiff'
        max_wal_senders = 2
        log_min_messages = 'DEBUG1'
    }
);

# Start the PostgreSQL server
$node->start();

$node->safe_psql('postgres', 'create table test(num int)');
$node->safe_psql('postgres', 'insert into test values(12345)');
$node->safe_psql('postgres', 'insert into test values(123456)');

my $table_oid= $node->safe_psql('postgres', 'SELECT oid FROM pg_class WHERE relname = \'test\' AND relkind = \'r\';');

diag("table oid :");
diag($table_oid);

# sleep(3600);

my ($walfile_name, $blocksize) = split '\|' => $node->safe_psql('postgres',
	"SELECT pg_walfile_name(pg_switch_wal()), current_setting('block_size')");

sleep(5);

# copy($node->data_dir . '/pg_wal/' . $walfile_name, $node->data_dir . '/waldiff_directory/' . $walfile_name);
# delete wal file with our insert
unlink($node->data_dir . '/pg_wal/' . $walfile_name);

# Stop the server
$node->stop('immediate');

# copy($node->data_dir . '/waldiff_directory/' . $walfile_name, $node->data_dir . '/pg_wal/' . $walfile_name);
copy($node->data_dir . '/waldiff/' . $walfile_name, $node->data_dir . '/pg_wal/' . $walfile_name);

# Start the PostgreSQL server
$node->start();

sleep(5);

my $stdout1= $node->safe_psql('postgres', 'select * from test');

diag("stdout");
diag($stdout1);

ok($stdout1 == 12345, 'copy was successful');

# Stop the server
$node->stop('immediate');

done_testing();