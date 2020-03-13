-- Revert seattleflu/schema:warehouse/primary-encounter-location from pg

begin;

drop view warehouse.primary_encounter_location;

commit;
