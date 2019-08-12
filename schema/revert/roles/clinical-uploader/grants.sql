-- Revert seattleflu/schema:roles/clinical-uploader/grants from pg

-- Revert seattleflu/schema:roles/clinical-uploader/grants from pg

begin;

set local role id3c;

revoke insert (document)
    on receiving.clinical
  from "clinical-uploader";

revoke select (clinical_id)
    on receiving.clinical
  from "clinical-uploader";

revoke usage
    on schema receiving
  from "clinical-uploader";

revoke connect on database :"DBNAME" from "clinical-uploader";

commit;
