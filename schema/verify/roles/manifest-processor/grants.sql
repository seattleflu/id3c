-- Verify seattleflu/schema:roles/manifest-processor/grants on pg

begin;

select 1/pg_catalog.has_database_privilege('manifest-processor', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('manifest-processor', 'receiving', 'usage')::int;
select 1/pg_catalog.has_schema_privilege('manifest-processor', 'warehouse', 'usage')::int;
select 1/pg_catalog.has_table_privilege('manifest-processor', 'receiving.manifest', 'select')::int;
select 1/pg_catalog.has_column_privilege('manifest-processor', 'receiving.manifest', 'processing_log', 'update')::int;
select 1/pg_catalog.has_table_privilege('manifest-processor', 'warehouse.sample', 'select,insert,update')::int;
select 1/pg_catalog.has_table_privilege('manifest-processor', 'warehouse.identifier', 'select')::int;
select 1/pg_catalog.has_table_privilege('manifest-processor', 'warehouse.identifier_set', 'select')::int;

select 1/(not pg_catalog.has_column_privilege('manifest-processor', 'receiving.manifest', 'manifest_id', 'insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('manifest-processor', 'receiving.manifest', 'document', 'insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('manifest-processor', 'receiving.manifest', 'received', 'insert,update'))::int;
select 1/(not pg_catalog.has_table_privilege('manifest-processor', 'warehouse.sample', 'delete'))::int;
select 1/(not pg_catalog.has_table_privilege('manifest-processor', 'warehouse.identifier', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('manifest-processor', 'warehouse.identifier_set', 'insert,update,delete'))::int;

rollback;
