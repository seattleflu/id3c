-- Verify seattleflu/schema:roles/longitudinal-uploader/grants on pg

begin;

set local role id3c;

select 1/pg_catalog.has_database_privilege('longitudinal-uploader', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('longitudinal-uploader', 'receiving', 'usage')::int;
select 1/pg_catalog.has_column_privilege('longitudinal-uploader', 'receiving.longitudinal', 'document', 'insert')::int;

select 1/(not pg_catalog.has_column_privilege('longitudinal-uploader', 'receiving.longitudinal', 'longitudinal_id','insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('longitudinal-uploader', 'receiving.longitudinal', 'document',       'select,update'))::int;
select 1/(not pg_catalog.has_column_privilege('longitudinal-uploader', 'receiving.longitudinal', 'received',       'select,insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('longitudinal-uploader', 'receiving.longitudinal', 'processing_log', 'select,insert,update'))::int;

rollback;
