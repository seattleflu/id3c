-- Verify seattleflu/schema:roles/manifest-uploader/grants on pg

begin;

set local role id3c;

select 1/pg_catalog.has_database_privilege('manifest-uploader', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('manifest-uploader', 'receiving', 'usage')::int;
select 1/pg_catalog.has_column_privilege('manifest-uploader', 'receiving.manifest', 'document', 'insert')::int;

select 1/(not pg_catalog.has_column_privilege('manifest-uploader', 'receiving.manifest', 'manifest_id',    'insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('manifest-uploader', 'receiving.manifest', 'document',       'select,update'))::int;
select 1/(not pg_catalog.has_column_privilege('manifest-uploader', 'receiving.manifest', 'received',       'select,insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('manifest-uploader', 'receiving.manifest', 'processing_log', 'select,insert,update'))::int;

rollback;
