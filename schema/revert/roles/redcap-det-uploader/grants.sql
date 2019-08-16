-- Revert seattleflu/schema:roles/redcap-det-uploader/grants from pg

begin;

revoke insert (document)
    on receiving.redcap_det
  from "redcap-det-uploader";

revoke select (redcap_det_id)
    on receiving.redcap_det
  from "redcap-det-uploader";

revoke usage
    on schema receiving
  from "redcap-det-uploader";

revoke connect on database :"DBNAME" from "redcap-det-uploader";

commit;
