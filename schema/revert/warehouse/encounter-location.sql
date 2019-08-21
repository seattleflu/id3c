-- Revert seattleflu/schema:warehouse/encounter-location from pg

begin;

drop table warehouse.encounter_location;

commit;
