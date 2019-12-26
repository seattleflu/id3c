-- Deploy seattleflu/schema:warehouse/location/timestamp to pg
-- requires: warehouse/location

begin;

alter table warehouse.location
    add column created timestamp with time zone,
    add column modified timestamp with time zone;

alter table warehouse.location
    alter column created set default now(),
    alter column modified set default now();

comment on column warehouse.location.created is
    'When the record was first processed by ID3C';

comment on column warehouse.location.modified is
    'When the record was last updated';

commit;
