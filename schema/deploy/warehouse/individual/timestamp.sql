-- Deploy seattleflu/schema:warehouse/individual/timestamp to pg
-- requires: warehouse/individual

begin;

alter table warehouse.individual
    add column created timestamp with time zone,
    add column modified timestamp with time zone;

alter table warehouse.individual
    alter column created set default now(),
    alter column modified set default now();

comment on column warehouse.individual.created is
    'When the record was first processed by ID3C';

comment on column warehouse.individual.modified is
    'When the record was last updated';

commit;
