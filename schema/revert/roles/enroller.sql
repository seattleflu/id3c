-- Revert seattleflu/schema:roles/enroller from pg

begin;

revoke insert (document) on receiving.enrollment from enroller;
revoke usage on schema receiving from enroller;
revoke connect on database :"DBNAME" from enroller;

drop role enroller;

commit;
