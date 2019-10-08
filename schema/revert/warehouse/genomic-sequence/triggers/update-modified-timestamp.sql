-- Revert seattleflu/schema:warehouse/genomic-sequence/triggers/update-modified-timestamp from pg

begin;

drop trigger update_modified_timestamp on warehouse.genomic_sequence;

commit;
