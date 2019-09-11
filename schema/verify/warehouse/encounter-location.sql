-- Verify seattleflu/schema:warehouse/encounter-location pg

begin;

select pg_catalog.has_table_privilege('warehouse.encounter_location', 'select');

rollback;
