-- Revert seattleflu/schema:receiving/enrollment from pg

begin;

drop table receiving.enrollment;

commit;
