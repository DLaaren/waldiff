CREATE FUNCTION explain_wal_record(text, text) RETURNS void
AS 'MODULE_PATHNAME', 'explain_wal_record'
LANGUAGE C;