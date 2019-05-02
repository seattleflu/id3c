-- Deploy seattleflu/schema:roles/incidence-modeler/grant-select-on-age-bins to pg
-- requires: roles/incidence-modeler
-- requires: shipping/age-bin

begin;

grant select
   on table shipping.age_bin_fine,
            shipping.age_bin_coarse
   to "incidence-modeler";

commit;
