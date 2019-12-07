-- Deploy seattleflu/schema:warehouse/encounter-location/timestamp to pg
-- requires: warehouse/encounter-location

begin;

alter table warehouse.encounter_location
    add column created timestamp with time zone,
    add column modified timestamp with time zone;

alter table warehouse.encounter_location
    alter column created set default now(),
    alter column modified set default now();

comment on column warehouse.encounter_location.created is
    'When the record was first processed by ID3C';

comment on column warehouse.encounter_location.modified is
    'When the record was last updated';

commit;
