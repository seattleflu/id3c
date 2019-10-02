-- Deploy seattleflu/schema:roles/consensus-genome-processor/grants to pg
-- requires: receiving/consensus-genome
-- requires: roles/consensus-genome-processor/create
-- requires: warehouse/consensus-genome
-- requires: warehouse/genomic-sequence
-- requires: warehouse/sequence-read-set/triggers/urls-not-null-check
-- requires: warehouse/sequence-read-set/triggers/urls-unique-check
-- requires: warehouse/sequence-read-set/triggers/urls-unique-to-one-set

begin;

grant connect on database :"DBNAME" to "consensus-genome-processor";

grant usage
   on schema receiving, warehouse
   to "consensus-genome-processor";

grant select
   on receiving.consensus_genome, warehouse.sample,
      warehouse.sequence_read_set, warehouse.organism, warehouse.consensus_genome,
      warehouse.genomic_sequence
   to "consensus-genome-processor";

grant insert
  on warehouse.sequence_read_set, warehouse.consensus_genome,
     warehouse.genomic_sequence
  to "consensus-genome-processor";

grant update (processing_log)
   on receiving.consensus_genome
   to "consensus-genome-processor";

grant update (details)
  on warehouse.sequence_read_set, warehouse.consensus_genome
  to "consensus-genome-processor";

grant update (seq, details)
  on warehouse.genomic_sequence
  to "consensus-genome-processor";

commit;
