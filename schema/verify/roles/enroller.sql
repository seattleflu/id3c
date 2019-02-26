-- Verify seattleflu/schema:roles/enroller on pg

begin;

select 1/pg_catalog.has_database_privilege('enroller', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('enroller', 'staging', 'usage')::int;
select 1/pg_catalog.has_column_privilege('enroller', 'staging.enrollment', 'document', 'insert')::int;

select 1/(not pg_catalog.has_column_privilege('enroller', 'staging.enrollment', 'enrollment_id', 'select,insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('enroller', 'staging.enrollment', 'document',      'select,update'))::int;
select 1/(not pg_catalog.has_column_privilege('enroller', 'staging.enrollment', 'received',      'select,insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('enroller', 'staging.enrollment', 'processed',     'select,insert,update'))::int;

rollback;
