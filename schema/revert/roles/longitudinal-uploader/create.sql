-- Revert seattleflu/schema:roles/longitudinal-uploader/create from pg

begin;

set local role id3c;

drop role "longitudinal-uploader";

commit;
