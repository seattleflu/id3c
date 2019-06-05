-- Verify seattleflu/schema:shipping/age-bin-v2 on pg

begin;

select pg_catalog.has_table_privilege('shipping.age_bin_fine_v2', 'select');
select pg_catalog.has_table_privilege('shipping.age_bin_coarse_v2', 'select');

rollback;
