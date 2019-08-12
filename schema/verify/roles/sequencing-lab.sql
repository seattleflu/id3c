-- Verify seattleflu/schema:roles/sequencing-lab on pg

begin;

set local role id3c;

select 1/pg_catalog.has_database_privilege('sequencing-lab', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('sequencing-lab', 'receiving', 'usage')::int;
select 1/pg_catalog.has_column_privilege('sequencing-lab', 'receiving.sequence_read_set', 'document', 'insert')::int;

select 1/(not pg_catalog.has_column_privilege('sequencing-lab', 'receiving.sequence_read_set', 'sequence_read_set_id', 'select,insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('sequencing-lab', 'receiving.sequence_read_set', 'document', 'select,update'))::int;
select 1/(not pg_catalog.has_column_privilege('sequencing-lab', 'receiving.sequence_read_set', 'received', 'select,insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('sequencing-lab', 'receiving.sequence_read_set', 'processing_log', 'select,insert,update'))::int;

rollback;
