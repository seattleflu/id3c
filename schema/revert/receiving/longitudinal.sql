-- Revert seattleflu/schema:receiving/longitudinal from pg

begin;

set local role id3c;

drop table receiving.longitudinal;

commit;
