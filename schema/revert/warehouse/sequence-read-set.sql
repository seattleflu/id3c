-- Revert seattleflu/schema:warehouse/sequence-read-set from pg

begin;

drop table warehouse.sequence_read_set;

commit;
