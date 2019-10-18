-- Verify seattleflu/schema:roles/fhir-uploader/grants on pg

begin;

select 1/pg_catalog.has_database_privilege('fhir-uploader', :'DBNAME',          'connect')::int;
select 1/pg_catalog.has_schema_privilege('fhir-uploader',   'receiving',        'usage')::int;
select 1/pg_catalog.has_column_privilege('fhir-uploader',   'receiving.fhir',   'document',        'insert')::int;

select 1/(not pg_catalog.has_column_privilege('fhir-uploader', 'receiving.fhir', 'fhir_id',        'insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('fhir-uploader', 'receiving.fhir', 'document',       'select,update'))::int;
select 1/(not pg_catalog.has_column_privilege('fhir-uploader', 'receiving.fhir', 'received',       'select,insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('fhir-uploader', 'receiving.fhir', 'processing_log', 'select,insert,update'))::int;

rollback;
