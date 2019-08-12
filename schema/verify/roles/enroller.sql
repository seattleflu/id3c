-- Verify seattleflu/schema:roles/enroller on pg

begin;

set local role id3c;

select 1/pg_catalog.has_database_privilege('enroller', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('enroller', 'receiving', 'usage')::int;
select 1/pg_catalog.has_column_privilege('enroller', 'receiving.enrollment', 'document', 'insert')::int;

select 1/(not pg_catalog.has_column_privilege('enroller', 'receiving.enrollment', 'enrollment_id', 'select,insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('enroller', 'receiving.enrollment', 'document',      'select,update'))::int;
select 1/(not pg_catalog.has_column_privilege('enroller', 'receiving.enrollment', 'received',      'select,insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('enroller', 'receiving.enrollment', 'processed',     'select,insert,update'))::int;

rollback;
