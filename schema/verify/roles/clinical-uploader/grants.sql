-- Verify seattleflu/schema:roles/clinical-uploader/grants on pg

begin;

set local role id3c;

select 1/pg_catalog.has_database_privilege('clinical-uploader', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('clinical-uploader', 'receiving', 'usage')::int;
select 1/pg_catalog.has_column_privilege('clinical-uploader', 'receiving.clinical', 'document', 'insert')::int;

select 1/(not pg_catalog.has_column_privilege('clinical-uploader', 'receiving.clinical', 'clinical_id',    'insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('clinical-uploader', 'receiving.clinical', 'document',       'select,update'))::int;
select 1/(not pg_catalog.has_column_privilege('clinical-uploader', 'receiving.clinical', 'received',       'select,insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('clinical-uploader', 'receiving.clinical', 'processing_log', 'select,insert,update'))::int;

rollback;
