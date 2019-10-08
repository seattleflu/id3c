-- Revert seattleflu/schema:warehouse/sequence-read-set/timestamp from pg

begin;

alter table warehouse.sequence_read_set
    drop column created,
    drop column modified;

commit;
