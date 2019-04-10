-- Revert seattleflu/schema:roles/diagnostic-lab from pg

begin;

revoke insert (document) on receiving.presence_absence from "diagnostic-lab";
revoke usage on schema receiving from "diagnostic-lab";
revoke connect on database :"DBNAME" from "diagnostic-lab";

drop role "diagnostic-lab";

commit;
