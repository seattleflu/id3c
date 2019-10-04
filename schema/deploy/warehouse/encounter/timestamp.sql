-- Deploy seattleflu/schema:warehouse/encounter/timestamp to pg
-- requires: warehouse/encounter

begin;

alter table warehouse.encounter
    add column created timestamp with time zone,
    add column modified timestamp with time zone;

alter table warehouse.encounter
    alter column created set default now(),
    alter column modified set default now();

comment on column warehouse.encounter.created is
    'When the record was first processed by ID3C';

comment on column warehouse.encounter.modified is
    'When the record was last updated';

commit;
