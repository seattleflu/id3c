-- Deploy seattleflu/schema:roles/longitudinal-uploader/grants to pg
-- requires: receiving/longitudinal
-- requires: roles/longitudinal-uploader/create


begin;

grant connect on database :"DBNAME" to "longitudinal-uploader";

grant usage
   on schema receiving
   to "longitudinal-uploader";

grant select (longitudinal_id)
   on receiving.longitudinal
   to "longitudinal-uploader";

grant insert (document)
   on receiving.longitudinal
   to "longitudinal-uploader";

commit;
