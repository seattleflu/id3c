-- Revert seattleflu/schema:roles/redcap-det-uploader/create from pg

begin;

drop role "redcap-det-uploader";

commit;
