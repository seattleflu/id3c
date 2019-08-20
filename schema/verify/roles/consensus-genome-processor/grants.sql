-- Verify seattleflu/schema:roles/consensus-genome-processor/grants on pg

begin;

select 1/pg_catalog.has_database_privilege('consensus-genome-processor', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('consensus-genome-processor', 'receiving', 'usage')::int;
select 1/pg_catalog.has_schema_privilege('consensus-genome-processor', 'warehouse', 'usage')::int;
select 1/pg_catalog.has_table_privilege('consensus-genome-processor', 'receiving.consensus_genome', 'select')::int;
select 1/pg_catalog.has_table_privilege('consensus-genome-processor', 'warehouse.sample', 'select')::int;
select 1/pg_catalog.has_table_privilege('consensus-genome-processor', 'warehouse.organism', 'select')::int;
select 1/pg_catalog.has_table_privilege('consensus-genome-processor', 'warehouse.consensus_genome', 'select,insert')::int;
select 1/pg_catalog.has_table_privilege('consensus-genome-processor', 'warehouse.sequence_read_set', 'select,insert')::int;
select 1/pg_catalog.has_table_privilege('consensus-genome-processor', 'warehouse.genomic_sequence', 'select,insert')::int;
select 1/pg_catalog.has_column_privilege('consensus-genome-processor', 'receiving.consensus_genome', 'processing_log', 'update')::int;
select 1/pg_catalog.has_column_privilege('consensus-genome-processor', 'warehouse.sequence_read_set', 'details', 'update')::int;
select 1/pg_catalog.has_column_privilege('consensus-genome-processor', 'warehouse.consensus_genome', 'details', 'update')::int;
select 1/pg_catalog.has_column_privilege('consensus-genome-processor', 'warehouse.genomic_sequence', 'details', 'update')::int;
select 1/pg_catalog.has_column_privilege('consensus-genome-processor', 'warehouse.genomic_sequence', 'seq', 'update')::int;

select 1/(not pg_catalog.has_schema_privilege('consensus-genome-processor', 'shipping', 'usage'))::int;
select 1/(not pg_catalog.has_table_privilege('consensus-genome-processor', 'receiving.consensus_genome', 'insert,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('consensus-genome-processor', 'warehouse.sample', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('consensus-genome-processor', 'warehouse.organism', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('consensus-genome-processor', 'warehouse.consensus_genome', 'delete'))::int;
select 1/(not pg_catalog.has_table_privilege('consensus-genome-processor', 'warehouse.sequence_read_set', 'delete'))::int;
select 1/(not pg_catalog.has_table_privilege('consensus-genome-processor', 'warehouse.genomic_sequence', 'delete'))::int;
select 1/(not pg_catalog.has_column_privilege('consensus-genome-processor', 'receiving.consensus_genome', 'consensus_genome_id', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('consensus-genome-processor', 'receiving.consensus_genome', 'received', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('consensus-genome-processor', 'receiving.consensus_genome', 'document', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('consensus-genome-processor', 'warehouse.sequence_read_set', 'sequence_read_set_id', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('consensus-genome-processor', 'warehouse.sequence_read_set', 'sample_id', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('consensus-genome-processor', 'warehouse.sequence_read_set', 'urls', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('consensus-genome-processor', 'warehouse.consensus_genome', 'consensus_genome_id', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('consensus-genome-processor', 'warehouse.consensus_genome', 'organism_id', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('consensus-genome-processor', 'warehouse.consensus_genome', 'sequence_read_set_id', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('consensus-genome-processor', 'warehouse.genomic_sequence', 'genomic_sequence_id', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('consensus-genome-processor', 'warehouse.genomic_sequence', 'identifier', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('consensus-genome-processor', 'warehouse.genomic_sequence', 'segment', 'update'))::int;

rollback;
