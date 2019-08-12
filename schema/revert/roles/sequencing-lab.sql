-- Revert seattleflu/schema:roles/sequencing-lab from pg

begin;

set local role id3c;

revoke insert (document) on receiving.sequence_read_set from "sequencing-lab";
revoke usage on schema receiving from "sequencing-lab";
revoke connect on database :"DBNAME" from "sequencing-lab";

drop role "sequencing-lab";

commit;
