-- Verify seattleflu/schema:roles/clinical-processor/grants on pg

begin;

select 1/pg_catalog.has_database_privilege('clinical-processor', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('clinical-processor', 'receiving', 'usage')::int;
select 1/pg_catalog.has_schema_privilege('clinical-processor', 'warehouse', 'usage')::int;
select 1/pg_catalog.has_table_privilege('clinical-processor', 'receiving.clinical', 'select')::int;
select 1/pg_catalog.has_column_privilege('clinical-processor', 'receiving.clinical', 'processing_log', 'update')::int;
select 1/pg_catalog.has_table_privilege('clinical-processor', 'warehouse.sample', 'select')::int;
select 1/pg_catalog.has_column_privilege('clinical-processor', 'warehouse.sample', 'encounter_id', 'select,update')::int;
select 1/pg_catalog.has_table_privilege('clinical-processor', 'warehouse.identifier', 'select')::int;
select 1/pg_catalog.has_table_privilege('clinical-processor', 'warehouse.identifier_set', 'select')::int;
select 1/pg_catalog.has_table_privilege('clinical-processor', 'warehouse.encounter', 'select,insert,update')::int;
select 1/pg_catalog.has_table_privilege('clinical-processor', 'warehouse.individual', 'select,insert,update')::int;
select 1/pg_catalog.has_table_privilege('clinical-processor', 'warehouse.site', 'select,insert')::int;

select 1/(not pg_catalog.has_table_privilege('clinical-processor', 'receiving.clinical', 'delete'))::int;
select 1/(not pg_catalog.has_column_privilege('clinical-processor', 'receiving.clinical', 'clinical_id', 'insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('clinical-processor', 'receiving.clinical', 'document', 'insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('clinical-processor', 'receiving.clinical', 'received', 'insert,update'))::int;
select 1/(not pg_catalog.has_table_privilege('clinical-processor', 'warehouse.sample', 'insert,delete'))::int;
select 1/(not pg_catalog.has_column_privilege('clinical-processor', 'warehouse.sample', 'identifier', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('clinical-processor', 'warehouse.sample', 'details', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('clinical-processor', 'warehouse.sample', 'collection_identifier', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('clinical-processor', 'warehouse.sample', 'sample_id', 'update'))::int;
select 1/(not pg_catalog.has_table_privilege('clinical-processor', 'warehouse.identifier', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('clinical-processor', 'warehouse.identifier_set', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('clinical-processor', 'warehouse.encounter', 'delete'))::int;
select 1/(not pg_catalog.has_table_privilege('clinical-processor', 'warehouse.individual', 'delete'))::int;
select 1/(not pg_catalog.has_table_privilege('clinical-processor', 'warehouse.site', 'update,delete'))::int;


rollback;
