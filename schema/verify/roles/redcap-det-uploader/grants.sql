-- Verify seattleflu/schema:roles/redcap-det-uploader/grants on pg

begin;

select 1/pg_catalog.has_database_privilege('redcap-det-uploader', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('redcap-det-uploader', 'receiving', 'usage')::int;
select 1/pg_catalog.has_column_privilege('redcap-det-uploader', 'receiving.redcap_det', 'document', 'insert')::int;

select 1/(not pg_catalog.has_column_privilege('redcap-det-uploader', 'receiving.redcap_det', 'redcap_det_id',    'insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('redcap-det-uploader', 'receiving.redcap_det', 'document',       'select,update'))::int;
select 1/(not pg_catalog.has_column_privilege('redcap-det-uploader', 'receiving.redcap_det', 'received',       'select,insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('redcap-det-uploader', 'receiving.redcap_det', 'processing_log', 'select,insert,update'))::int;


rollback;
