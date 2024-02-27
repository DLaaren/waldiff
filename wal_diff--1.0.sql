CREATE FUNCTION read_wal_header(text, text) RETURNS void
AS 'MODULE_PATHNAME', 'read_header'
LANGUAGE C;