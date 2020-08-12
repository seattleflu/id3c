-- Revert seattleflu/schema:shipping/age-bin-decade from pg

begin;

drop table shipping.age_bin_decade_v1;

commit;
