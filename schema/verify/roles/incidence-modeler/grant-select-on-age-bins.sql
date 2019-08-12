-- Verify seattleflu/schema:roles/incidence-modeler/grant-select-on-age-bins on pg

begin;

set local role id3c;

do $$ begin
    assert pg_catalog.has_table_privilege('incidence-modeler', 'shipping.age_bin_fine', 'select');
    assert pg_catalog.has_table_privilege('incidence-modeler', 'shipping.age_bin_coarse', 'select');
end $$;

rollback;
