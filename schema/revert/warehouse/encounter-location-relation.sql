-- Revert seattleflu/schema:warehouse/encounter-location-relation from pg

begin;

drop table warehouse.encounter_location_relation;

commit;
