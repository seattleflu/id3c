-- Revert seattleflu/schema:receiving/longitudinal from pg

begin;

drop table receiving.longitudinal;

commit;
