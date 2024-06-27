/* contrib/waldiff/waldiff--1.0.sql */

CREATE FUNCTION read_raw_xlog_rec(TEXT) RETURNS BYTEA
AS 'MODULE_PATHNAME', 'read_raw_xlog_rec'
LANGUAGE C STRICT;

CREATE FUNCTION write_raw_xlog_rec(TEXT, BYTEA) RETURNS VOID
AS 'MODULE_PATHNAME', 'write_raw_xlog_rec'
LANGUAGE C STRICT;