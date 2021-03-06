-- Deploy seattleflu/schema:roles/enroller to pg
-- requires: receiving/enrollment

begin;

create role enroller;

grant connect on database :"DBNAME" to enroller;
grant usage on schema receiving to enroller;
grant insert (document) on receiving.enrollment to enroller;

commit;
