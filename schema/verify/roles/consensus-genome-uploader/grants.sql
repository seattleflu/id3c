-- Verify seattleflu/schema:roles/consensus-genome-uploader/grants on pg

begin;

select 1/pg_catalog.has_database_privilege('consensus-genome-uploader', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('consensus-genome-uploader', 'receiving', 'usage')::int;
select 1/pg_catalog.has_column_privilege('consensus-genome-uploader', 'receiving.consensus_genome', 'document', 'insert')::int;

select 1/(not pg_catalog.has_column_privilege('consensus-genome-uploader', 'receiving.consensus_genome', 'consensus_genome_id','insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('consensus-genome-uploader', 'receiving.consensus_genome', 'document',       'select,update'))::int;
select 1/(not pg_catalog.has_column_privilege('consensus-genome-uploader', 'receiving.consensus_genome', 'received',       'select,insert,update'))::int;
select 1/(not pg_catalog.has_column_privilege('consensus-genome-uploader', 'receiving.consensus_genome', 'processing_log', 'select,insert,update'))::int;

rollback;
