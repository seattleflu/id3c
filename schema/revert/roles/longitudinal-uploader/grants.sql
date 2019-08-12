-- Revert seattleflu/schema:roles/longitudinal-uploader/grants from pg

-- Revert seattleflu/schema:roles/longitudinal-uploader/grants from pg

begin;

set local role id3c;

revoke insert (document)
    on receiving.longitudinal
  from "longitudinal-uploader";

revoke select (longitudinal_id)
    on receiving.longitudinal
  from "longitudinal-uploader";

revoke usage
    on schema receiving
  from "longitudinal-uploader";

revoke connect on database :"DBNAME" from "longitudinal-uploader";

commit;
