-- Revert seattleflu/schema:warehouse/encounter-location-relation/data from pg

begin;

delete from warehouse.encounter_location_relation where relation in (
    'residence',
    'lodging',
    'workplace',
    'school',
    'site'
);

commit;
