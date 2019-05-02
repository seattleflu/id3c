-- Revert seattleflu/schema:shipping/age-bin from pg

begin;

drop table shipping.age_bin_fine;
drop table shipping.age_bin_coarse;

commit;
