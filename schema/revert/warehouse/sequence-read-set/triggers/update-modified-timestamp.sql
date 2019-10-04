-- Revert seattleflu/schema:warehouse/sequence-read-set/triggers/update-modified-timestamp from pg

begin;

drop trigger update_modified_timestamp on warehouse.sequence_read_set;

commit;
