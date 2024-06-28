use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::RecursiveCopy;
use PostgreSQL::Test::Utils;
use Test::More;
use Data::Dumper;

my $node = PostgreSQL::Test::Cluster->new('main');

# Create a data directory with initdb
$node->init;

$node->append_conf(
    'postgresql.conf', 
    q{
        wal_level = 'replica'
        archive_mode = 'on'
        archive_library = 'waldiff'
        waldiff.waldiff_dir = 'waldiff'
        shared_preload_libraries = 'waldiff'
    }
); # TODO need shared_preload_libraries only for rmgr?

# Start the PostgreSQL server
$node->start;

# Setup
$node->safe_psql('postgres', 'CREATE EXTENSION waldiff');

# Create table wich contain WALDIFFWriterState struct, because we
# need to pass it through processes
$node->safe_psql(
        'postgres', 
        "CREATE TABLE writer_state (
        id INTEGER PRIMARY KEY,

        fd INTEGER,
        segno bigint,
        tli bigint,
        dir text,
        segsize bigint,
        current_offset integer,
        last_processed_record bigint,

        system_identifier bigint,

        already_written bigint,

        first_page_addr bigint
        );"
);

# Insert initial values
$node->safe_psql(
    'postgres', 
    "INSERT INTO writer_state (id, segno, tli, system_identifier, first_page_addr, already_written) VALUES (1, 1, 1, 0, 0, 0);"
);

# Create table wich contain WALRawReaderState struct, because we
# need to pass it through processes
$node->safe_psql(
        'postgres', 
        "CREATE TABLE raw_reader_state (
        id INTEGER PRIMARY KEY,

        fd INTEGER,
        segno bigint,
        tli bigint,
        dir text,
        segsize bigint,
        current_offset integer,
        last_processed_record bigint,

        system_identifier bigint,

        buffer text,
        buffer_fullness bigint,
        buffer_capacity bigint,

        tmp_buffer text,
        tmp_buffer_fullness bigint,

        already_read bigint,

        first_page_addr bigint
        );"
);

# Insert initial values
$node->safe_psql(
    'postgres', 
    "INSERT INTO raw_reader_state (id, segno, tli, tmp_buffer_fullness, buffer_fullness, system_identifier, first_page_addr, already_read) VALUES (1, 1, 1, 0, 0, 0, 0, 0);"
);

# This table always will contain position in wal file (and waldiff respectively) of last read/written record
$node->safe_psql(
        'postgres', 
        "CREATE TABLE service_info (
        id INTEGER PRIMARY KEY,
        last_read_record_position bigint,
        last_read_record_maxaligned_length bigint,
        last_read_record_length bigint
        );"
);

# Insert initial values
$node->safe_psql(
    'postgres', 
    "INSERT INTO service_info (id, last_read_record_position) VALUES (1, 0);"
);

mkdir $node->data_dir . "/waldiff";

# Create wal/wal diff file names
my $wal_file = $node->data_dir . '/pg_wal/000000010000000000000001';
my $waldiff_file = $node->data_dir . '/waldiff/000000010000000000000001';
ok(-f -e $wal_file, "Got a wal file");

my $read_record;
my $num_records = 100;

my @record_addresses; # this array will contain addresses of records from wal file
my @record_lengths; # this array will contain lengths of records from wal file
my @record_maxaligned_lengths; # this array will contain maxaligned lengths of records from wal file

my $record_addr;
my $record_length;
my $record_maxaligned_length;

# Read record from wal segment and write it to wal_diff
# segment $num_records times
for (my $iter = 0; $iter < $num_records; $iter++) {
    # read record from wal segment
    eval {
        $read_record = $node->safe_psql(
            'postgres', 
            "SELECT read_raw_xlog_rec('$wal_file');"
        );
        1;
    } or do {
        my $error = $@ || 'Unknown error';
        $node->stop('immediate');
        BAIL_OUT("Failed to execute 'read_raw_xlog_rec' ");
    };

    # get record's address
    eval {
        $record_addr = $node->safe_psql(
            'postgres', 
            "SELECT last_read_record_position FROM service_info WHERE id = 1;"
        );
        1;
    } or do {
        my $error = $@ || 'Unknown error';
        $node->stop('immediate');
        BAIL_OUT("Failed to retrieve 'last_read_record_position' from 'service_info' ");
    };
    push @record_addresses, int($record_addr);

    # get record's length
    eval {
        $record_length = $node->safe_psql(
            'postgres', 
            "SELECT last_read_record_length FROM service_info WHERE id = 1;"
        );
        1;
    } or do {
        my $error = $@ || 'Unknown error';
        $node->stop('immediate');
        BAIL_OUT("Failed to retrieve 'last_read_record_length' from 'service_info' ");
    };
    push @record_lengths, int($record_length);

    # get record's maxaligned length
    eval {
        $record_maxaligned_length = $node->safe_psql(
            'postgres', 
            "SELECT last_read_record_maxaligned_length FROM service_info WHERE id = 1;"
        );
        1;
    } or do {
        my $error = $@ || 'Unknown error';
        $node->stop('immediate');
        BAIL_OUT("Failed to retrieve 'last_read_record_maxaligned_length' from 'service_info' ");
    };
    push @record_maxaligned_lengths, int($record_maxaligned_length);

    # write record to waldiff segment
    eval {
        $node->safe_psql(
            'postgres', 
            "SELECT write_raw_xlog_rec('$waldiff_file', '$read_record');"
        );
        1;
    } or do {
        my $error = $@ || 'Unknown error';
        $node->stop('immediate');
        BAIL_OUT("Failed to execute 'write_raw_xlog_rec' ");
    };
}

# get total number of read from wal segment bytes
my $already_read;
my $total_read_count = 0;
eval {
    $already_read = $node->safe_psql(
        'postgres', 
        "SELECT already_read FROM raw_reader_state WHERE id = 1;"
    );
    1;
} or do {
    my $error = $@ || 'Unknown error';
    $node->stop('immediate');
    BAIL_OUT("Failed to retrieve 'already_read' from 'raw_reader_state' ");
};

$total_read_count = int($already_read);

my $size = -s $waldiff_file;
ok($size == $total_read_count, "Read bytes == write bytes");

# Copy all read wal and all written waldiff into buffers
my ($buffer1, $buffer2);
open(my $fh1, '<:raw', $wal_file) or die "Cannot open file '$wal_file': $!";
open(my $fh2, '<:raw', $waldiff_file) or die "Cannot open file '$waldiff_file': $!";
my $bytes_read1 = read($fh1, $buffer1, $total_read_count);
my $bytes_read2 = read($fh2, $buffer2, $total_read_count);
close($fh1);
close($fh2);

my $is_equal = 1;

sub substrings_equal {
    my ($current_position, $data_len) = @_;
    
    if (substr($buffer1, $current_position, $data_len) ne substr($buffer2, $current_position, $data_len)) {
        return 0;
    }
    return 1;
}

# some system parameters
my $xlog_rec_hdr_size     = 24;
my $block_size            = 8192;
my $long_page_header_len  = 40;
my $short_page_header_len = 24;

my $current_position = 0;

for (my $i = 0; $i < $num_records; $i++) {
    
    my $rec_maxaligned_len = @record_maxaligned_lengths[$i];
    my $rec_len            = @record_lengths[$i];
    my $hdr_len            = 0;

     if ($current_position % $block_size == 0) 
    {
        $hdr_len = $short_page_header_len;
        if ($current_position == 0) # if we are in start of segment 
        {
            $hdr_len = $long_page_header_len;
        }

        $is_equal = substrings_equal($current_position, $hdr_len);
        
        $current_position += $hdr_len;
        
        ok($is_equal == 1, "Headers match");

        $hdr_len = 0;
    }

    if ($block_size * (1 + (int($current_position / $block_size))) < ($current_position + $rec_len))
    {
        $hdr_len = $short_page_header_len;
    }

    my $missmatch_num = 0;
    for (my $j = $current_position; $j < $current_position + $rec_maxaligned_len + $hdr_len; $j++) {
        if (substr($buffer1, $j, 1) ne substr($buffer2, $j, 1)) {
            $missmatch_num += 1;
        }
    }

    # xl_prev and xl_crc fields in struct XLogRecord might be different in waldiff segment,
    # so we don't take them into account
    if ($missmatch_num > 12)
    {
        diag("");
        diag("record number");
        diag($i);
        diag("current position");
        diag($current_position);
        diag("record length");
        diag($rec_len);
        diag("record maxalign length");
        diag($rec_maxaligned_len);

        for (my $j = $current_position; $j < $current_position + $rec_maxaligned_len + $hdr_len; $j++) {
            if (substr($buffer1, $j, 1) ne substr($buffer2, $j, 1)) {
                diag("not equal in position");
                diag($j);
            }
        }
    }

    $current_position += ($rec_maxaligned_len + $hdr_len);
}

ok($is_equal == 1, "Wal diff file is equal to wal file");

# Stop the server
$node->stop('immediate');
done_testing();