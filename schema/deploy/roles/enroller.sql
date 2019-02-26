-- Deploy seattleflu/schema:roles/enroller to pg
-- requires: staging/enrollment

begin;

create role enroller;

grant connect on database :"DBNAME" to enroller;
grant usage on schema staging to enroller;
grant insert (document) on staging.enrollment to enroller;

commit;
