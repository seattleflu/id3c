-- Revert seattleflu/schema:roles/incidence-modeler/grant-select-on-age-bins from pg

begin;

revoke select
    on table shipping.age_bin_fine,
             shipping.age_bin_coarse
  from "incidence-modeler";

commit;
