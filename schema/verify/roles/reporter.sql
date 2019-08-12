-- Verify seattleflu/schema:roles/reporter on pg

begin;

set local role id3c;

select 1/pg_catalog.has_database_privilege('reporter', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('reporter', 'receiving', 'usage')::int;

rollback;
