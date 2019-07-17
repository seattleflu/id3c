-- Verify seattleflu/schema:roles/kit-processor/grants on pg

begin;

select 1/pg_catalog.has_database_privilege('kit-processor', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('kit-processor', 'receiving', 'usage')::int;
select 1/pg_catalog.has_schema_privilege('kit-processor', 'warehouse', 'usage')::int;
select 1/pg_catalog.has_table_privilege('kit-processor', 'receiving.enrollment', 'select')::int;
select 1/pg_catalog.has_table_privilege('kit-processor', 'receiving.manifest', 'select')::int;
select 1/pg_catalog.has_column_privilege('kit-processor', 'receiving.enrollment', 'processing_log', 'update')::int;
select 1/pg_catalog.has_column_privilege('kit-processor', 'receiving.manifest', 'processing_log', 'update')::int;
select 1/pg_catalog.has_table_privilege('kit-processor', 'warehouse.identifier', 'select')::int;
select 1/pg_catalog.has_table_privilege('kit-processor', 'warehouse.identifier_set', 'select')::int;
select 1/pg_catalog.has_table_privilege('kit-processor', 'warehouse.encounter', 'select')::int;
select 1/pg_catalog.has_table_privilege('kit-processor', 'warehouse.site', 'select')::int;
select 1/pg_catalog.has_table_privilege('kit-processor', 'warehouse.sample', 'select')::int;
select 1/pg_catalog.has_column_privilege('kit-processor', 'warehouse.sample','encounter_id', 'update')::int;
select 1/pg_catalog.has_table_privilege('kit-processor', 'warehouse.kit', 'select, insert, update')::int;

select 1/(not pg_catalog.has_table_privilege('kit-processor', 'receiving.enrollment', 'delete'))::int;
select 1/(not pg_catalog.has_table_privilege('kit-processor', 'receiving.manifest', 'delete'))::int;
select 1/(not pg_catalog.has_column_privilege('kit-processor', 'receiving.enrollment', 'enrollment_id', 'insert, update'))::int;
select 1/(not pg_catalog.has_column_privilege('kit-processor', 'receiving.enrollment', 'document', 'insert, update'))::int;
select 1/(not pg_catalog.has_column_privilege('kit-processor', 'receiving.enrollment', 'received', 'insert, update'))::int;
select 1/(not pg_catalog.has_column_privilege('kit-processor', 'receiving.manifest', 'manifest_id', 'insert, update'))::int;
select 1/(not pg_catalog.has_column_privilege('kit-processor', 'receiving.manifest', 'document', 'insert, update'))::int;
select 1/(not pg_catalog.has_column_privilege('kit-processor', 'receiving.manifest', 'received', 'insert, update'))::int;
select 1/(not pg_catalog.has_table_privilege('kit-processor', 'warehouse.identifier', 'insert, update, delete'))::int;
select 1/(not pg_catalog.has_table_privilege('kit-processor', 'warehouse.identifier_set', 'insert, update, delete'))::int;
select 1/(not pg_catalog.has_table_privilege('kit-processor', 'warehouse.encounter', 'insert, update, delete'))::int;
select 1/(not pg_catalog.has_table_privilege('kit-processor', 'warehouse.site', 'insert, update, delete'))::int;
select 1/(not pg_catalog.has_table_privilege('kit-processor', 'warehouse.sample', 'insert, delete'))::int;
select 1/(not pg_catalog.has_column_privilege('kit-processor', 'warehouse.sample', 'sample_id', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('kit-processor', 'warehouse.sample', 'identifier', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('kit-processor', 'warehouse.sample', 'details', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('kit-processor', 'warehouse.sample', 'collection_identifier', 'update'))::int;
select 1/(not pg_catalog.has_table_privilege('kit-processor', 'warehouse.kit', 'delete'))::int;

rollback;
