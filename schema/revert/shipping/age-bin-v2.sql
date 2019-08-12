-- Revert seattleflu/schema:shipping/age-bin-v2 from pg

begin;

set local role id3c;

drop table shipping.age_bin_fine_v2;
drop table shipping.age_bin_coarse_v2;

commit;
