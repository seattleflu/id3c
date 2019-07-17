-- Revert seattleflu/schema:warehouse/genomic_sequence from pg

begin;

drop table warehouse.genomic_sequence;

commit;
