-- Verify seattleflu/schema:warehouse/encounter-location-relation on pg

begin;

select pg_catalog.has_table_privilege('warehouse.encounter_location_relation', 'select');

rollback;
