-- Deploy seattleflu/schema:warehouse/encounter-location to pg
-- requires: warehouse/location

begin;

set local search_path to warehouse, public;

create table encounter_location (
    encounter_id integer not null references encounter (encounter_id),
    relation citext not null,
    location_id integer not null references location (location_id),
    details jsonb,
    primary key (encounter_id, relation)
);

comment on table encounter_location is 'Associates an encounter with a location';
comment on column encounter_location.encounter_id is 'Internal id of the encounter';
comment on column encounter_location.location_id is 'Internal id of the location';
comment on column encounter_location.relation is 'The relation between the encounter and location, e.g. collection site, residence, workplace, etc.';
comment on column encounter_location.details is 'Additional information about this encounter-location which does not have a place in the relational schema';

-- We will often restrict by relation alone
create index encounter_location_relation_idx on encounter_location (relation);

-- Index details document by default so containment queries on it are quick
create index encounter_location_details_idx on encounter_location using gin (details jsonb_path_ops);

commit;
