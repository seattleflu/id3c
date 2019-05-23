-- Revert seattleflu/schema:roles/clinical-uploader/create from pg

begin;

drop role "clinical-uploader";

commit;
