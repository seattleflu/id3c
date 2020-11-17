-- Revert seattleflu/schema:receiving/sequence-read-set/indexes/processing-log from pg

begin;

drop index receiving.sequence_read_set_processing_log_idx;

commit;
