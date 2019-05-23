-- Deploy seattleflu/schema:roles/clinical-uploader/grants to pg
-- requires: receiving/clinical
-- requires: roles/clinical-uploader/create


begin;

grant connect on database :"DBNAME" to "clinical-uploader";

grant usage
   on schema receiving
   to "clinical-uploader";

grant select (clinical_id)
   on receiving.clinical
   to "clinical-uploader";

grant insert (document)
   on receiving.clinical
   to "clinical-uploader";

commit;
