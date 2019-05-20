-- Revert seattleflu/schema:roles/manifest-uploader/create from pg

begin;

drop role "manifest-uploader";

commit;
