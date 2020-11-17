-- Deploy seattleflu/schema:receiving/consensus-genome/indexes/processing-log to pg
-- requires: receiving/consensus-genome

begin;

create index consensus_genome_processing_log_idx
  on receiving.consensus_genome
  using gin (processing_log jsonb_path_ops);

commit;
