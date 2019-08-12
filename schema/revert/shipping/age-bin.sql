-- Revert seattleflu/schema:shipping/age-bin from pg

begin;

set local role id3c;

drop table shipping.age_bin_fine;
drop table shipping.age_bin_coarse;

commit;
