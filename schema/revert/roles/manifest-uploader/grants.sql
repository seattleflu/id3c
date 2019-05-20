-- Revert seattleflu/schema:roles/manifest-uploader/grants from pg

begin;

revoke insert (document)
    on receiving.manifest
  from "manifest-uploader";

revoke select (manifest_id)
    on receiving.manifest
  from "manifest-uploader";

revoke usage
    on schema receiving
  from "manifest-uploader";

revoke connect on database :"DBNAME" from "manifest-uploader";

commit;
