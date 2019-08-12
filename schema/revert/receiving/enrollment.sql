-- Revert seattleflu/schema:receiving/enrollment from pg

begin;

set local role id3c;

drop table receiving.enrollment;

commit;
