-- Deploy seattleflu/schema:roles/redcap-det-uploader/grants to pg
-- requires: receiving/redcap-det
-- requires: roles/redcap-det-uploader/create

begin;

grant connect on database :"DBNAME" to "redcap-det-uploader";

grant usage
   on schema receiving
   to "redcap-det-uploader";

grant select (redcap_det_id)
   on receiving.redcap_det
   to "redcap-det-uploader";

grant insert (document)
   on receiving.redcap_det
   to "redcap-det-uploader";

commit;
