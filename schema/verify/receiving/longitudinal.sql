-- Verify seattleflu/schema:receiving/longitudinal on pg

begin;

select pg_catalog.has_table_privilege('receiving.longitudinal', 'select');

rollback;
