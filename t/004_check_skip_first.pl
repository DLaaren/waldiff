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
);

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
    "INSERT INTO writer_state (id, segno, tli, system_identifier, first_page_addr, already_written) 
     VALUES (1, 1, 1, 0, 0, 0);"
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
    "INSERT INTO raw_reader_state (id, segno, tli, tmp_buffer_fullness, buffer_fullness, system_identifier, first_page_addr, already_read) 
     VALUES (1, 1, 1, 0, 0, 0, 0, 0);"
);

# This table always will contain position in WAL file (and WALDIFF respectively) of last read/written record
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

# Create WAL/WALDIFF file names
my $wal_file = $node->data_dir . '/pg_wal/000000010000000000000001';
my $waldiff_file = $node->data_dir . '/waldiff/000000010000000000000001';
ok(-f -e $wal_file, "Got a wal file");

# These arrays will contain information about each read record
my @record_addresses;
my @record_lengths;
my @record_maxaligned_lengths;

# Skip record from WAL segment $num_skip_records times

my $num_skip_records = 1;

for (my $iter = 0; $iter < $num_skip_records; $iter++) 
{
    my $read_record;

    # Read record from WAL segment
    eval 
    {
        $read_record = $node->safe_psql(
            'postgres', 
            "SELECT skip_raw_xlog_rec('$wal_file');"
        );
        1;
    } 
    or do 
    {
        my $error = $@ || 'Unknown error';
        $node->stop('immediate');
        BAIL_OUT("Failed to execute 'skip_raw_xlog_rec' ");
    };

    # Get record's address, length, maxalign length and store them into arrays
    eval 
    {
        my $service_info = $node->safe_psql(
            'postgres', 
            "SELECT last_read_record_position, last_read_record_length, last_read_record_maxaligned_length 
               FROM service_info WHERE id = 1;"
        );
        my @values = split(/\|/, $service_info);
        my ($record_addr, $record_length, $record_maxaligned_length) = @values;

        push @record_addresses, int($record_addr);
        push @record_lengths, int($record_length);
        push @record_maxaligned_lengths, int($record_maxaligned_length);
        1;
    } 
    or do 
    {
        my $error = $@ || 'Unknown error';
        $node->stop('immediate');
        BAIL_OUT("Failed to retrieve fields from 'service_info' ");
    };
}

# Read record from WAL segment and write it to WALDIFF
# segment $num_read_records times

my $num_read_records = 2;

for (my $iter = 0; $iter < $num_read_records; $iter++) 
{
    my $read_record;

    # Read record from WAL segment
    eval 
    {
        $read_record = $node->safe_psql(
            'postgres', 
            "SELECT read_raw_xlog_rec('$wal_file');"
        );
        1;
    } 
    or do 
    {
        my $error = $@ || 'Unknown error';
        $node->stop('immediate');
        BAIL_OUT("Failed to execute 'read_raw_xlog_rec' ");
    };

    # Get record's address, length, maxalign length and store them into arrays
    eval 
    {
        my $service_info = $node->safe_psql(
            'postgres', 
            "SELECT last_read_record_position, last_read_record_length, last_read_record_maxaligned_length 
               FROM service_info WHERE id = 1;"
        );
        my @values = split(/\|/, $service_info);
        my ($record_addr, $record_length, $record_maxaligned_length) = @values;

        push @record_addresses, int($record_addr);
        push @record_lengths, int($record_length);
        push @record_maxaligned_lengths, int($record_maxaligned_length);
        1;
    } 
    or do 
    {
        my $error = $@ || 'Unknown error';
        $node->stop('immediate');
        BAIL_OUT("Failed to retrieve fields from 'service_info' ");
    };

    # Write record to WALDIFF segment
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

# get total number of read from WAL segment bytes
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

# Copy all read from WAL and all written to WALDIFF bytes into buffers
my ($buffer1, $buffer2);
open(my $fh1, '<:raw', $wal_file) or die "Cannot open file '$wal_file': $!";
open(my $fh2, '<:raw', $waldiff_file) or die "Cannot open file '$waldiff_file': $!";
my $bytes_read1 = read($fh1, $buffer1, $total_read_count);
my $bytes_read2 = read($fh2, $buffer2, $size);
close($fh1);
close($fh2);


sub substrings_equal 
{
    my ($current_position, $data_len, $buffer1_position, $buffer2_position) = @_;
    
    if (substr($buffer1, $buffer1_position, $data_len) ne substr($buffer2, $buffer2_position, $data_len)) 
    {
        return 0;
    }
    return 1;
}

# some system parameters
my $xlog_rec_hdr_size     = 24;
my $block_size            = 8192;
my $long_page_header_len  = 40;
my $short_page_header_len = 24;

# At first, skip all bytes in WAL segment, that not presented in WALDIFF segment
my $current_position = 0;

for (my $i = 0; $i < $num_skip_records; $i++) 
{   
    my $rec_maxaligned_len = @record_maxaligned_lengths[$i];
    my $rec_len            = @record_lengths[$i];
    my $hdr_len            = 0;

    # Check whether we are in start of page
    if ($current_position % $block_size == 0) 
    {
        $hdr_len = $short_page_header_len;

        # Check whether we are in start of segment
        if ($current_position == 0)
        {
            $hdr_len = $long_page_header_len;
        }
        
        $current_position += $hdr_len;
        $hdr_len = 0;
    }

    # If record doesn't fit on page, we must also read page header
    # TODO : if record is bigger than two pages, this logic is incorrect, but it will work for a smaller ones
    if ($block_size * (1 + (int($current_position / $block_size))) < ($current_position + $rec_len))
    {
        $hdr_len = $short_page_header_len;
    }

    $current_position += ($rec_maxaligned_len + $hdr_len);
}

# Finally, compare WAL and WALDIFF segment

my $is_equal = 1;
my $current_waldiff_position = 0;

for (my $i = $num_skip_records; $i < $num_skip_records + $num_read_records; $i++) 
{
    
    my $rec_maxaligned_len = @record_maxaligned_lengths[$i];
    my $rec_len            = @record_lengths[$i];
    my $hdr_len            = 0;

    # Check whether we are in start of page
    if ($current_position % $block_size == 0) 
    {
        $hdr_len = $short_page_header_len;

        # Check whether we are in start of segment
        if ($current_position == 0)
        {
            $hdr_len = $long_page_header_len;
        }

        # Compare headers
        $is_equal = substrings_equal($current_position, $hdr_len, $current_position, $current_waldiff_position);
        ok($is_equal == 1, "Headers match");
        
        $current_position += $hdr_len;
        $current_waldiff_position += $hdr_len;
        $hdr_len = 0;
    }

    # If record doesn't fit on page, we must also read page header
    # TODO : if record is bigger than two pages, this logic is incorrect, but it will work for a smaller ones
    if ($block_size * (1 + (int($current_position / $block_size))) < ($current_position + $rec_len))
    {
        $hdr_len = $short_page_header_len;
    }
 
    # Compare segments byte-by-byte
    my $mismatch_num = 0;
    my $j = $current_position;
    my $k = $current_waldiff_position;
    my $end_position = $current_position + $rec_maxaligned_len + $hdr_len;

    for (; $j < $end_position; $j++, $k++) 
    {
        if (substr($buffer1, $j, 1) ne substr($buffer2, $k, 1)) 
        {
            $mismatch_num += 1;
        }
    }

    # xl_prev and xl_crc fields in struct XLogRecord might be different in WALDIFF segment,
    # so we don't take them into account
    if ($mismatch_num > 12)
    {
        diag("");
        diag("record number");
        diag($i);
        diag("current wal position");
        diag($current_position);
        diag("current waldiff position");
        diag($current_waldiff_position);
        diag("record length");
        diag($rec_len);
        diag("record maxalign length");
        diag($rec_maxaligned_len);

        $j = $current_position;
        $k = $current_waldiff_position;
        $end_position = $current_position + $rec_maxaligned_len + $hdr_len;

        for (; $j < $end_position; $j++, $k++) 
        {
            if (substr($buffer1, $j, 1) ne substr($buffer2, $k, 1)) 
            {
                diag("not equal in position");
                diag($j);
            }
        }

        $is_equal = 0;
    }

    ok($is_equal == 1, "Records match");

    $current_position += ($rec_maxaligned_len + $hdr_len);
    $current_waldiff_position += ($rec_maxaligned_len + $hdr_len);
}

ok($is_equal == 1, "WALDIFF file is equal to WAL file");

# Stop the server
$node->stop('immediate');
done_testing();