-- Verify seattleflu/schema:shipping/age-bin-decade on pg

begin;

select pg_catalog.has_table_privilege('shipping.age_bin_decade_v1', 'select');

rollback;
