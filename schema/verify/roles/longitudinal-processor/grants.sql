-- Verify seattleflu/schema:roles/longitudinal-processor/grants on pg

begin;

select 1/pg_catalog.has_database_privilege('longitudinal-processor', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('longitudinal-processor', 'receiving', 'usage')::int;
select 1/pg_catalog.has_schema_privilege('longitudinal-processor', 'warehouse', 'usage')::int;
select 1/pg_catalog.has_table_privilege('longitudinal-processor', 'receiving.longitudinal', 'select')::int;
select 1/pg_catalog.has_column_privilege('longitudinal-processor', 'receiving.longitudinal', 'processing_log', 'update')::int;
select 1/pg_catalog.has_table_privilege('longitudinal-processor', 'warehouse.sample', 'select')::int;
select 1/pg_catalog.has_column_privilege('longitudinal-processor', 'warehouse.sample', 'encounter_id', 'select,update')::int;
select 1/pg_catalog.has_table_privilege('longitudinal-processor', 'warehouse.identifier', 'select')::int;
select 1/pg_catalog.has_table_privilege('longitudinal-processor', 'warehouse.identifier_set', 'select')::int;
select 1/pg_catalog.has_table_privilege('longitudinal-processor', 'warehouse.encounter', 'select,insert,update')::int;
select 1/pg_catalog.has_table_privilege('longitudinal-processor', 'warehouse.individual', 'select,insert,update')::int;
select 1/pg_catalog.has_table_privilege('longitudinal-processor', 'warehouse.site', 'select,insert')::int;

select 1/(not pg_catalog.has_table_privilege('longitudinal-processor', 'receiving.longitudinal', 'delete'))::int;
select 1/(not pg_catalog.has_column_privilege('longitudinal-processor', 'receiving.longitudinal', 'longitudinal_id', 'insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('longitudinal-processor', 'receiving.longitudinal', 'document', 'insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('longitudinal-processor', 'receiving.longitudinal', 'received', 'insert,update'))::int;
select 1/(not pg_catalog.has_table_privilege('longitudinal-processor', 'warehouse.sample', 'insert,delete'))::int;
select 1/(not pg_catalog.has_column_privilege('longitudinal-processor', 'warehouse.sample', 'identifier', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('longitudinal-processor', 'warehouse.sample', 'details', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('longitudinal-processor', 'warehouse.sample', 'collection_identifier', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('longitudinal-processor', 'warehouse.sample', 'sample_id', 'update'))::int;
select 1/(not pg_catalog.has_table_privilege('longitudinal-processor', 'warehouse.identifier', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('longitudinal-processor', 'warehouse.identifier_set', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('longitudinal-processor', 'warehouse.encounter', 'delete'))::int;
select 1/(not pg_catalog.has_table_privilege('longitudinal-processor', 'warehouse.individual', 'delete'))::int;
select 1/(not pg_catalog.has_table_privilege('longitudinal-processor', 'warehouse.site', 'update,delete'))::int;


rollback;
