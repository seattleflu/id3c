-- Verify seattleflu/schema:shipping/age-bin on pg

begin;

set local role id3c;

select pg_catalog.has_table_privilege('shipping.age_bin_fine', 'select');
select pg_catalog.has_table_privilege('shipping.age_bin_coarse', 'select');

rollback;
