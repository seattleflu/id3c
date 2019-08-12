-- Revert seattleflu/schema:roles/manifest-uploader/create from pg

begin;

set local role id3c;

drop role "manifest-uploader";

commit;
