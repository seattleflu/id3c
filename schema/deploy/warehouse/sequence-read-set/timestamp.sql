-- Deploy seattleflu/schema:warehouse/sequence-read-set/timestamp to pg
-- requires: warehouse/sequence-read-set

begin;

alter table warehouse.sequence_read_set
    add column created timestamp with time zone,
    add column modified timestamp with time zone;

alter table warehouse.sequence_read_set
    alter column created set default now(),
    alter column modified set default now();

comment on column warehouse.sequence_read_set.created is
    'When the record was first processed by ID3C';

comment on column warehouse.sequence_read_set.modified is
    'When the record was last updated';

commit;
