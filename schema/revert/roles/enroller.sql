-- Revert seattleflu/schema:roles/enroller from pg

begin;

revoke insert (document) on staging.enrollment from enroller;
revoke usage on schema staging from enroller;
revoke connect on database :"DBNAME" from enroller;

drop role enroller;

commit;
