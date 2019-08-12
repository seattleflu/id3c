-- Revert seattleflu/schema:roles/clinical-uploader/create from pg

begin;

set local role id3c;

drop role "clinical-uploader";

commit;
