use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::RecursiveCopy;
use PostgreSQL::Test::Utils;
use Test::More;
use File::Copy;

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
$node->init();

$node->append_conf(
    'postgresql.conf', 
    q{
        wal_level = 'replica'
        archive_mode = 'on'
        archive_library = 'wal_diff'
        wal_diff.wal_diff_directory = 'wal_diff'
        shared_preload_libraries = 'wal_diff'
        max_wal_senders = 2
    }
);

# Start the PostgreSQL server
$node->start();

$node->safe_psql('postgres', 'create table test(num int)');
$node->safe_psql('postgres', 'insert into test values(12345)');

my ($walfile_name, $blocksize) = split '\|' => $node->safe_psql('postgres',
	"SELECT pg_walfile_name(pg_switch_wal()), current_setting('block_size')");

sleep(10);


# copy($node->data_dir . '/pg_wal/' . $walfile_name, $node->data_dir . '/wal_diff_directory/' . $walfile_name);
# delete wal file with our insert
unlink($node->data_dir . '/pg_wal/' . $walfile_name);

# Stop the server
$node->stop('immediate');

# copy($node->data_dir . '/wal_diff_directory/' . $walfile_name, $node->data_dir . '/pg_wal/' . $walfile_name);
copy($node->data_dir . '/wal_diff/' . $walfile_name, $node->data_dir . '/pg_wal/' . $walfile_name);

# Start the PostgreSQL server
$node->start();

my $stdout1= $node->safe_psql('postgres', 'select * from test');

ok($stdout1 == 12345, 'copy was successful');

sleep(3600);

# Stop the server
$node->stop('immediate');

done_testing();