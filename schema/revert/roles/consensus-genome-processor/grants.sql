-- Revert seattleflu/schema:roles/consensus-genome-processor/grants from pg

begin;

revoke update (seq, details)
    on warehouse.genomic_sequence
  from "consensus-genome-processor";

revoke update (details)
    on warehouse.sequence_read_set, warehouse.genome
  from "consensus-genome-processor";

revoke update (processing_log)
    on receiving.consensus_genome
  from "consensus-genome-processor";

revoke insert
    on warehouse.sequence_read_set, warehouse.genome,
       warehouse.genomic_sequence
  from "consensus-genome-processor";

revoke select
    on receiving.consensus_genome, warehouse.sample,
       warehouse.sequence_read_set, warehouse.organism, warehouse.genome,
       warehouse.genomic_sequence
  from "consensus-genome-processor";

revoke usage
    on schema receiving, warehouse
  from "consensus-genome-processor";

revoke connect on database :"DBNAME" from "consensus-genome-processor";

commit;
