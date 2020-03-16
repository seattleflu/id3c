-- Deploy seattleflu/schema:warehouse/encounter-location-relation to pg
-- requires: warehouse/schema

begin;

create table warehouse.encounter_location_relation (
    relation citext primary key,
    priority integer unique
);

comment on table warehouse.encounter_location_relation is
    'Controlled vocabulary for warehouse.encounter_location.relation';

comment on column warehouse.encounter_location_relation.relation is
    'A relation between an encounter and location, e.g. collection site, residence, workplace, etc.';

comment on column warehouse.encounter_location_relation.priority is
    'Arbitrary inter-relation ranking, where smaller numbers mean greater importance within this ID3C instance.  Used to determine the default "primary" location related to an encounter when there is more than one.';

commit;
