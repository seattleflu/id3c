-- Revert seattleflu/schema:receiving/consensus-genome/indexes/processing-log from pg

begin;

drop index receiving.consensus_genome_processing_log_idx;

commit;
