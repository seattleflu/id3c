-- Revert seattleflu/schema:warehouse/genomic-sequence/timestamp from pg

begin;

alter table warehouse.genomic_sequence
    drop column created,
    drop column modified;

commit;
