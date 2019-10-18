-- Verify seattleflu/schema:receiving/fhir on pg

begin;

select pg_catalog.has_table_privilege('receiving.fhir', 'select');

rollback;
