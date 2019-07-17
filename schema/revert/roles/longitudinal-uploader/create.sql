-- Revert seattleflu/schema:roles/longitudinal-uploader/create from pg

begin;

drop role "longitudinal-uploader";

commit;
