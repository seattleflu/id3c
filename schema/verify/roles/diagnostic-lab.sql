-- Verify seattleflu/schema:roles/diagnostic-lab on pg

begin;

select 1/pg_catalog.has_database_privilege('diagnostic-lab', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('diagnostic-lab', 'receiving', 'usage')::int;
select 1/pg_catalog.has_column_privilege('diagnostic-lab', 'receiving.presence_absence', 'document', 'insert')::int;

select 1/(not pg_catalog.has_column_privilege('diagnostic-lab', 'receiving.presence_absence', 'presence_absence_id', 'select,insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('diagnostic-lab', 'receiving.presence_absence', 'document', 'select,update'))::int;
select 1/(not pg_catalog.has_column_privilege('diagnostic-lab', 'receiving.presence_absence', 'received', 'select,insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('diagnostic-lab', 'receiving.presence_absence', 'processing_log', 'select,insert,update'))::int;

rollback;
