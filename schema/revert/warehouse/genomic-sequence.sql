-- Revert seattleflu/schema:warehouse/genomic-sequence from pg

begin;

drop table warehouse.genomic_sequence;

commit;
