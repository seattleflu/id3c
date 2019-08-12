-- Deploy seattleflu/schema:roles/manifest-uploader/grants to pg
-- requires: roles/manifest-uploader/create
-- requires: receiving/manifest

begin;

set local role id3c;

grant connect on database :"DBNAME" to "manifest-uploader";

grant usage
   on schema receiving
   to "manifest-uploader";

grant select (manifest_id)
   on receiving.manifest
   to "manifest-uploader";

grant insert (document)
   on receiving.manifest
   to "manifest-uploader";

commit;
