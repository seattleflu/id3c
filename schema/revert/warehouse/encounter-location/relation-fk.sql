-- Revert seattleflu/schema:warehouse/encounter-location/relation-fk from pg

begin;

alter table warehouse.encounter_location
    drop constraint encounter_location_relation_fkey;

commit;
