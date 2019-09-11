-- Verify seattleflu/schema:roles/enrollment-processor/grants on pg

begin;

select 1/pg_catalog.has_database_privilege('enrollment-processor', :'DBNAME', 'connect')::int;

select 1/pg_catalog.has_schema_privilege('enrollment-processor', 'receiving', 'usage')::int;
select 1/pg_catalog.has_schema_privilege('enrollment-processor', 'warehouse', 'usage')::int;

select 1/pg_catalog.has_table_privilege('enrollment-processor', 'receiving.enrollment', 'select')::int;
select 1/(not pg_catalog.has_table_privilege('enrollment-processor', 'receiving.enrollment', 'insert,update,delete'))::int;

select 1/pg_catalog.has_column_privilege('enrollment-processor', 'receiving.enrollment', 'processing_log', 'update')::int;
select 1/(not pg_catalog.has_column_privilege('enrollment-processor', 'receiving.enrollment', 'processing_log', 'insert'))::int;

select 1/pg_catalog.has_table_privilege('enrollment-processor', 'warehouse.identifier', 'select')::int;
select 1/(not pg_catalog.has_table_privilege('enrollment-processor', 'warehouse.identifier', 'insert,update,delete'))::int;

select 1/pg_catalog.has_table_privilege('enrollment-processor', 'warehouse.identifier_set', 'select')::int;
select 1/(not pg_catalog.has_table_privilege('enrollment-processor', 'warehouse.identifier_set', 'insert,update,delete'))::int;

create temporary table mutable_tables (name) as values
    ('warehouse.site'),
    ('warehouse.individual'),
    ('warehouse.encounter'),
    ('warehouse.encounter_location'),
    ('warehouse.sample'),
    ('warehouse.location')
;

select 1/pg_catalog.has_table_privilege('enrollment-processor', name, 'select,insert,update')::int from mutable_tables;
select 1/(not pg_catalog.has_table_privilege('enrollment-processor', name, 'delete'))::int from mutable_tables;

rollback;
