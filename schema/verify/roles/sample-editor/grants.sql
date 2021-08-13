-- Verify seattleflu/schema:roles/sample-editor/grants on pg

begin;

select 1/pg_catalog.has_database_privilege('sample-editor', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('sample-editor', 'warehouse', 'usage')::int;
select 1/pg_catalog.has_table_privilege('sample-editor', 'warehouse.identifier', 'select')::int;
select 1/pg_catalog.has_table_privilege('sample-editor', 'warehouse.identifier_set', 'select')::int;
select 1/pg_catalog.has_table_privilege('sample-editor', 'warehouse.sample', 'select,insert,update')::int;

select 1/(not pg_catalog.has_schema_privilege('sample-editor', 'receiving', 'usage'))::int;
select 1/(not pg_catalog.has_table_privilege('sample-editor', 'warehouse.identifier', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('sample-editor', 'warehouse.identifier_set', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('sample-editor', 'warehouse.sample', 'delete'))::int;

rollback;
