-- Verify seattleflu/schema:roles/presence-absence-processor on pg

begin;

select 1/pg_catalog.has_database_privilege('presence-absence-processor', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('presence-absence-processor', 'receiving', 'usage')::int;
select 1/pg_catalog.has_schema_privilege('presence-absence-processor', 'warehouse', 'usage')::int;
select 1/pg_catalog.has_table_privilege('presence-absence-processor', 'receiving.presence_absence', 'select')::int;
select 1/pg_catalog.has_column_privilege('presence-absence-processor', 'receiving.presence_absence', 'processing_log', 'update')::int;
select 1/pg_catalog.has_table_privilege('presence-absence-processor', 'warehouse.sample', 'select,insert,update')::int;
select 1/pg_catalog.has_table_privilege('presence-absence-processor', 'warehouse.target', 'select,insert,update')::int;
select 1/pg_catalog.has_table_privilege('presence-absence-processor', 'warehouse.presence_absence', 'select,insert,update')::int;
select 1/pg_catalog.has_table_privilege('presence-absence-processor', 'warehouse.identifier', 'select')::int;
select 1/pg_catalog.has_table_privilege('presence-absence-processor', 'warehouse.identifier_set', 'select')::int;

select 1/(not pg_catalog.has_column_privilege('presence-absence-processor', 'receiving.presence_absence', 'presence_absence_id', 'insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('presence-absence-processor', 'receiving.presence_absence', 'document', 'insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('presence-absence-processor', 'receiving.presence_absence', 'received', 'insert,update'))::int;
select 1/(not pg_catalog.has_table_privilege('presence-absence-processor', 'warehouse.sample', 'delete'))::int;
select 1/(not pg_catalog.has_table_privilege('presence-absence-processor', 'warehouse.target', 'delete'))::int;
select 1/(not pg_catalog.has_table_privilege('presence-absence-processor', 'warehouse.presence_absence', 'delete'))::int;
select 1/(not pg_catalog.has_table_privilege('presence-absence-processor', 'warehouse.identifier', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('presence-absence-processor', 'warehouse.identifier_set', 'insert,update,delete'))::int;

rollback;
