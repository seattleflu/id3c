-- Deploy seattleflu/schema:receiving/sequence-read-set/indexes/processing-log to pg
-- requires: receiving/sequence-read-set

begin;

create index sequence_read_set_processing_log_idx
  on receiving.sequence_read_set
  using gin (processing_log jsonb_path_ops);

commit;
