-- Revert seattleflu/schema:warehouse/encounter-location/timestamp from pg

begin;

alter table warehouse.encounter_location
    drop column created,
    drop column modified;

commit;
