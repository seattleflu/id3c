-- Revert seattleflu/schema:staging/enrollment from pg

begin;

drop table staging.enrollment;

commit;
