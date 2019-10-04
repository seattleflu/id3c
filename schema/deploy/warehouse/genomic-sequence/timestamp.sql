-- Deploy seattleflu/schema:warehouse/genomic-sequence/timestamp to pg
-- requires: warehouse/genomic-sequence

begin;

alter table warehouse.genomic_sequence
    add column created timestamp with time zone,
    add column modified timestamp with time zone;

alter table warehouse.genomic_sequence
    alter column created set default now(),
    alter column modified set default now();

comment on column warehouse.genomic_sequence.created is
    'When the record was first processed by ID3C';

comment on column warehouse.genomic_sequence.modified is
    'When the record was last updated';

commit;
